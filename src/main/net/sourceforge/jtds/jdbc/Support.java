// jTDS JDBC Driver for Microsoft SQL Server
// Copyright (C) 2004 The jTDS Project
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either
// version 2.1 of the License, or (at your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
//
package net.sourceforge.jtds.jdbc;

import java.math.BigDecimal;
import java.text.MessageFormat;
import java.util.ResourceBundle;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;
import net.sourceforge.jtds.util.*;

/**
 * This class contains static utility methods designed to support the
 * main driver classes.
 * <p>
 * Implementation notes:
 * <ol>
 * <li>The methods in this class incorporate some code from previous versions
 *     of jTDS to handle dates, BLobs etc.
 * <li>This class contains routines to generate runtime messages from the resource file.
 * <li>The key data conversion logic used in Statements and result sets is implemented here.
 * <li>There is nothing here which is TDS specific.
 * </ol>
 *
 * @author Mike Hutchinson
 * @author jTDS project
 * @version $Id: Support.java,v 1.1 2004-06-27 17:00:53 bheineman Exp $
 */
public class Support {
    // Constants used in datatype conversions to avoid object allocations.
    private static final Byte BYTE_ZERO = new Byte((byte) 0);
    private static final Byte BYTE_ONE = new Byte((byte) 1);
    private static final Short SHORT_ZERO = new Short((short) 0);
    private static final Short SHORT_ONE = new Short((short) 1);
    private static final Integer INTEGER_ZERO = new Integer(0);
    private static final Integer INTEGER_ONE = new Integer(1);
    private static final Long LONG_ZERO = new Long(0L);
    private static final Long LONG_ONE = new Long(1L);
    private static final Float FLOAT_ZERO = new Float(0.0);
    private static final Float FLOAT_ONE = new Float(1.0);
    private static final Double DOUBLE_ZERO = new Double(0.0);
    private static final Double DOUBLE_ONE = new Double(1.0);
    private static final BigDecimal BIG_DECIMAL_ZERO = new BigDecimal(0.0);
    private static final BigDecimal BIG_DECIMAL_ONE = new BigDecimal(1.0);
    private static final java.sql.Date DATE_ZERO = new java.sql.Date(0);
    private static final java.sql.Time TIME_ZERO = new java.sql.Time(0);

    /**
     * Convert java clases to java.sql.Type constant.
     */
    private static final HashMap typeMap = new HashMap();

    static {
        typeMap.put(Byte.class,               new Integer(java.sql.Types.TINYINT));
        typeMap.put(Short.class,              new Integer(java.sql.Types.SMALLINT));
        typeMap.put(Integer.class,            new Integer(java.sql.Types.INTEGER));
        typeMap.put(Long.class,               new Integer(java.sql.Types.BIGINT));
        typeMap.put(Float.class,              new Integer(java.sql.Types.REAL));
        typeMap.put(Double.class,             new Integer(java.sql.Types.DOUBLE));
        typeMap.put(BigDecimal.class,         new Integer(java.sql.Types.DECIMAL));
        typeMap.put(Boolean.class,            new Integer(JtdsStatement.BOOLEAN));
        typeMap.put(byte[].class,             new Integer(java.sql.Types.VARBINARY));
        typeMap.put(java.sql.Date.class,      new Integer(java.sql.Types.DATE));
        typeMap.put(java.sql.Time.class,      new Integer(java.sql.Types.TIME));
        typeMap.put(java.sql.Timestamp.class, new Integer(java.sql.Types.TIMESTAMP));
        typeMap.put(BlobImpl.class,           new Integer(java.sql.Types.LONGVARBINARY));
        typeMap.put(ClobImpl.class,           new Integer(java.sql.Types.LONGVARCHAR));
        typeMap.put(String.class,             new Integer(java.sql.Types.VARCHAR));
    }

    /**
     *  Hex constants to use in conversion routines.
     */
    private static final char hex[] = {'0', '1', '2', '3', '4', '5', '6','7',
        '8', '9', 'A', 'B', 'C', 'D', 'E','F'
    };

    /**
     * Static utility Calendar object.
     */
    private static final GregorianCalendar cal = new GregorianCalendar();

    /**
     * Default name for resource bundle containing the messages
     */
    private static final String defaultResource = "net.sourceforge.jtds.jdbc.Messages";

    /**
     * Get runtime message using supplied key.
     *
     * @param key The key of the message in Messages.properties
     * @return The selected message as a <code>String</code>.
     */
    public static String getMessage(String key) {
        return getMessageText(key, null);
    }

    /**
     * Get runtime message using supplied key and substitute parameter
     * into message.
     *
     * @param key The key of the message in Messages.properties
     * @param param1 The object to insert into message.
     * @return The selected message as a <code>String</code>.
     */
    static String getMessage(String key, Object param1) {
        Object args[] = {param1};

        return getMessageText(key, args);
    }

    /**
     * Get runtime message using supplied key and substitute parameters
     * into message.
     *
     * @param key The key of the message in Messages.properties
     * @param param1 The object to insert into message.
     * @param param2 The object to insert into message.
     * @return The selected message as a <code>String</code>.
     */
    static String getMessage(String key, Object param1, Object param2) {
        Object args[] = {param1, param2};

        return getMessageText(key, args);
    }

    /**
     * Convert a byte[] object to a hex string.
     *
     * @param bytes The byte array to convert.
     * @return The hex equivalent as a <code>String</code>.
     */
    static String toHex(byte[] bytes) {
        int len = bytes.length;

        if (len > 0) {
            StringBuffer buf = new StringBuffer(len * 2);

            for (int i = 0; i < len; i++) {
                int b1 = bytes[i] & 0xFF;

                buf.append(hex[b1 >> 4]);
                buf.append(hex[b1 & 0x0F]);
            }

            return buf.toString();
        }

        return "";
    }

    /**
     * Convert an existing data object to the specified JDBC type.
     *
     * @param x The data object to convert.
     * @param jdbcType The required type constant from java.sql.Types.
     * @return The (possibly) converted data object.
     * @throws SQLException if the conversion is not supported or fails.
     */
    static Object convert(Object x, int jdbcType, String charSet)
    throws SQLException {
        try {
            switch (jdbcType) {
                case java.sql.Types.TINYINT:
                    if (x == null) {
                        return BYTE_ZERO;
                    } else if (x instanceof Byte) {
                        return x;
                    } else if (x instanceof Number) {
                        return new Byte(((Number) x).byteValue());
                    } else if (x instanceof String) {
                        return new Byte((String) x);
                    } else if (x instanceof Boolean) {
                        return ((Boolean) x).booleanValue() ? BYTE_ONE : BYTE_ZERO;
                    }

                    break;

                case java.sql.Types.SMALLINT:
                    if (x == null) {
                        return SHORT_ZERO;
                    } else if (x instanceof Short) {
                        return x;
                    } else if (x instanceof Number) {
                        return new Short(((Number) x).shortValue());
                    } else if (x instanceof String) {
                        return new Short((String) x);
                    } else if (x instanceof Boolean) {
                        return ((Boolean) x).booleanValue() ? SHORT_ONE : SHORT_ZERO;
                    }

                    break;

                case java.sql.Types.INTEGER:
                    if (x == null) {
                        return INTEGER_ZERO;
                    } else if (x instanceof Integer) {
                        return x;
                    } else if (x instanceof Number) {
                        return new Integer(((Number) x).intValue());
                    } else if (x instanceof String) {
                        return new Integer((String) x);
                    } else if (x instanceof Boolean) {
                        return ((Boolean) x).booleanValue() ? INTEGER_ONE : INTEGER_ZERO;
                    }

                    break;

                case java.sql.Types.BIGINT:
                    if (x == null) {
                        return LONG_ZERO;
                    } else if (x instanceof Long) {
                        return x;
                    } else if (x instanceof Number) {
                        return new Long(((Number) x).longValue());
                    } else if (x instanceof String) {
                        return new Long((String) x);
                    } else if (x instanceof Boolean) {
                        return ((Boolean) x).booleanValue() ? LONG_ONE : LONG_ZERO;
                    }

                    break;

                case java.sql.Types.REAL:
                case java.sql.Types.FLOAT:
                    if (x == null) {
                        return FLOAT_ZERO;
                    } else if (x instanceof Float) {
                        return x;
                    } else if (x instanceof Number) {
                        return new Float(((Number) x).floatValue());
                    } else if (x instanceof String) {
                        return new Float((String) x);
                    } else  if (x instanceof Boolean) {
                        return ((Boolean) x).booleanValue() ? FLOAT_ONE : FLOAT_ZERO;
                    }

                    break;

                case java.sql.Types.DOUBLE:
                    if (x == null) {
                        return DOUBLE_ZERO;
                    } else if (x instanceof Double) {
                        return x;
                    } else if (x instanceof Number) {
                        return new Double(((Number) x).doubleValue());
                    } else if (x instanceof String) {
                        return new Double((String) x);
                    } else if (x instanceof Boolean) {
                        return ((Boolean) x).booleanValue() ? DOUBLE_ONE : DOUBLE_ZERO;
                    }

                    break;

                case java.sql.Types.NUMERIC:
                case java.sql.Types.DECIMAL:
                    if (x == null) {
                        return null;
                    } else if (x instanceof BigDecimal) {
                        return x;
                    } else if (x instanceof Number) {
                        return new BigDecimal(((Number) x).doubleValue());
                    } else if (x instanceof String) {
                        return new BigDecimal((String) x);
                    } else if (x instanceof Boolean) {
                        return ((Boolean) x).booleanValue() ? BIG_DECIMAL_ONE : BIG_DECIMAL_ZERO;
                    }

                    break;

                case java.sql.Types.LONGVARCHAR:
                case java.sql.Types.VARCHAR:
                case java.sql.Types.CHAR:
                    if (x == null) {
                        return null;
                    } else if (x instanceof String) {
                        return x;
                    } else if (x instanceof BigDecimal) {
                        if (((BigDecimal) x).scale() > 0) {
                            // Eliminate trailing zeros
                            String tmp = x.toString();

                            for (int i = tmp.length() - 1; i > 0; i--) {
                                if (tmp.charAt(i) != '0') {
                                    if (tmp.charAt(i) == '.') {
                                        return tmp.substring(0, i);
                                    } else {
                                        return tmp.substring(0, i + 1);
                                    }
                                }
                            }

                            return tmp;
                        } else {
                            return x.toString();
                        }
                    } else if (x instanceof Number) {
                        return x.toString();
                    } else if (x instanceof Boolean) {
                        return((Boolean) x).booleanValue() ? "1" : "0";
                    } else if (x instanceof Clob) {
                        Clob clob = (Clob) x;

                        return clob.getSubString(1, (int) clob.length());
                    } else if (x instanceof Blob) {
                        Blob blob = (Blob) x;

                        x = blob.getBytes(1, (int) blob.length());
                    }

                    if (x instanceof byte[]) {
                        return Support.toHex((byte[]) x);
                    }

                    return x.toString(); // Last hope!

                case java.sql.Types.BIT:
                case JtdsStatement.BOOLEAN:
                    if (x == null) {
                        return Boolean.FALSE;
                    } else if (x instanceof Boolean) {
                        return x;
                    } else if (x instanceof Number) {
                        return(((Number) x).intValue() == 0) ? Boolean.FALSE : Boolean.TRUE;
                    } else if (x instanceof String) {
                        String tmp = ((String) x).trim();

                        return (tmp.equals("1") || tmp.equalsIgnoreCase("true")) ? Boolean.TRUE : Boolean.FALSE;
                    }

                case java.sql.Types.LONGVARBINARY:
                case java.sql.Types.VARBINARY:
                case java.sql.Types.BINARY:
                    if (x == null) {
                        return null;
                    } else if (x instanceof byte[]) {
                        return x;
                    } else if (x instanceof Blob) {
                        Blob blob = (Blob) x;

                        return blob.getBytes(1, (int) blob.length());
                    } else if (x instanceof Clob) {
                        Clob clob = (Clob) x;

                        x = clob.getSubString(1, (int) clob.length());
                    }

                    if (x instanceof String) {
                        if (charSet == null) {
                            charSet = "ISO-8859-1";
                        }

                        try {
                            return ((String) x).getBytes(charSet);
                        } catch (UnsupportedEncodingException e) {
                            return ((String) x).getBytes();
                        }
                    } else if (x instanceof UniqueIdentifier) {
                        return ((UniqueIdentifier) x).getBytes();
                    }

                    break;

                case java.sql.Types.TIMESTAMP:
                    if (x == null) {
                        return null;
                    } else if (x instanceof java.sql.Timestamp) {
                        return x;
                    } else if (x instanceof java.sql.Date) {
                        return new java.sql.Timestamp(((java.sql.Date) x).getTime());
                    } else if (x instanceof java.sql.Time) {
                        return new java.sql.Timestamp(((java.sql.Time) x).getTime());
                    }

                    break;

                case java.sql.Types.DATE:
                    if (x == null) {
                        return null;
                    } else if (x instanceof java.sql.Date) {
                        return x;
                    } else if (x instanceof java.sql.Time) {
                        return DATE_ZERO;
                    } else if (x instanceof java.sql.Timestamp) {
                        synchronized (cal) {
                            cal.setTime((java.util.Date) x);
                            cal.set(Calendar.HOUR_OF_DAY, 0);
                            cal.set(Calendar.MINUTE, 0);
                            cal.set(Calendar.SECOND, 0);
                            cal.set(Calendar.MILLISECOND, 0);
// VM1.4+ only              return new java.sql.Date(cal.getTimeInMillis());
                            return new java.sql.Date(cal.getTime().getTime());
                        }
                    }

                    break;

                case java.sql.Types.TIME:
                    if (x == null) {
                        return null;
                    } else if (x instanceof java.sql.Time) {
                        return x;
                    } else if (x instanceof java.sql.Date) {
                        return TIME_ZERO;
                    } else if (x instanceof java.sql.Timestamp) {
                        synchronized (cal) {
                            cal.setTime((java.util.Date) x);
                            cal.set(Calendar.YEAR, 0);
                            cal.set(Calendar.MONTH, 0);
                            cal.set(Calendar.DAY_OF_MONTH, 0);
// VM 1.4+ only             return new java.sql.Time(cal.getTimeInMillis());
                            return new java.sql.Time(cal.getTime().getTime());
                        }
                    }

                    break;

                case java.sql.Types.OTHER:
                case java.sql.Types.JAVA_OBJECT:
                    return x;

                case java.sql.Types.BLOB:
                    if (x == null) {
                        return null;
                    } else if (x instanceof Blob) {
                        return x;
                    } else if (x instanceof byte[]) {
                        return new BlobImpl((byte[]) x);
                    }

                    break;

                case java.sql.Types.CLOB:
                    if (x == null) {
                        return null;
                    } else if (x instanceof Clob) {
                        return x;
                    } else if (x instanceof String) {
                        return new ClobImpl((String) x);
                    }

                    break;

                default:
                    throw new SQLException(
                                          getMessage("error.convert.badtypeconst",
                                                     getJdbcTypeName(jdbcType)), "HY004");
            }

            throw new SQLException(
                                  getMessage("error.convert.badtypes",
                                             getJdbcTypeName(getJdbcType(x)),
                                             getJdbcTypeName(jdbcType)), "22005");
        } catch (NumberFormatException nfe) {
            throw new SQLException(
                                  getMessage("error.convert.badnumber",
                                             getJdbcTypeName(jdbcType)), "22000");
        }
    }

    /**
     * Get the JDBC type constant which matches the supplied Object type.
     *
     * @param value The object to analyse.
     * @return The JDBC type constant as an <code>int</code>.
     */
    static int getJdbcType(Object value) {
        if (value == null) {
            return java.sql.Types.NULL;
        }

        Object type = typeMap.get(value.getClass());

        if (type == null) {
            return java.sql.Types.OTHER;
        }

        return ((Integer) type).intValue();
    }

    /**
     * Get a String describing the supplied JDBC type constant.
     *
     * @param jdbcType The constant to be decoded.
     * @return The text decode of the type constant as a <code>String</code>.
     */
    static String getJdbcTypeName(int jdbcType) {
        switch (jdbcType) {
            case java.sql.Types.ARRAY:         return "ARRAY";
            case java.sql.Types.BIGINT:        return "BIGINT";
            case java.sql.Types.BINARY:        return "BINARY";
            case java.sql.Types.BIT:           return "BIT";
            case java.sql.Types.BLOB:          return "BLOB";
            case JtdsStatement.BOOLEAN:        return "BOOLEAN";
            case java.sql.Types.CHAR:          return "CHAR";
            case java.sql.Types.CLOB:          return "CLOB";
            case JtdsStatement.DATALINK:       return "DATALINK";
            case java.sql.Types.DATE:          return "DATE";
            case java.sql.Types.DECIMAL:       return "DECIMAL";
            case java.sql.Types.DISTINCT:      return "DISTINCT";
            case java.sql.Types.DOUBLE:        return "DOUBLE";
            case java.sql.Types.FLOAT:         return "FLOAT";
            case java.sql.Types.INTEGER:       return "INTEGER";
            case java.sql.Types.JAVA_OBJECT:   return "JAVA_OBJECT";
            case java.sql.Types.LONGVARBINARY: return "LONGVARBINARY";
            case java.sql.Types.LONGVARCHAR:   return "LONGVARCHAR";
            case java.sql.Types.NULL:          return "NULL";
            case java.sql.Types.NUMERIC:       return "NUMERIC";
            case java.sql.Types.OTHER:         return "OTHER";
            case java.sql.Types.REAL:          return "REAL";
            case java.sql.Types.REF:           return "REF";
            case java.sql.Types.SMALLINT:      return "SMALLINT";
            case java.sql.Types.STRUCT:        return "STRUCT";
            case java.sql.Types.TIME:          return "TIME";
            case java.sql.Types.TIMESTAMP:     return "TIMESTAMP";
            case java.sql.Types.TINYINT:       return "TINYINT";
            case java.sql.Types.VARBINARY:     return "VARBINARY";
            case java.sql.Types.VARCHAR:       return "VARCHAR";
            default:                           return "ERROR";
        }
    }

    /**
     * Retrieve the fully qualified java class name for the
     * supplied JDBC Types constant.
     *
     * @param jdbcType The JDBC Types constant.
     * @return The fully qualified java class name as a <code>String</code>.
     */
    static String getClassName(int jdbcType) {
        switch (jdbcType) {
            case JtdsStatement.BOOLEAN:
            case java.sql.Types.BIT:
                return "java.lang.Boolean";

            case java.sql.Types.TINYINT:
                return "java.lang.Byte";

            case java.sql.Types.SMALLINT:
                return "java.lang.Short";

            case java.sql.Types.INTEGER:
                return  "java.lang.Integer";

            case java.sql.Types.BIGINT:
                return "java.lang.Long";

            case java.sql.Types.NUMERIC:
            case java.sql.Types.DECIMAL:
                return "java.math.BigDecimal";

            case java.sql.Types.FLOAT:
            case java.sql.Types.DOUBLE:
                return "java.lang.Double";

            case java.sql.Types.REAL:
                return "java.lang.Float";

            case java.sql.Types.CHAR:
            case java.sql.Types.VARCHAR:
            case java.sql.Types.LONGVARCHAR:
                return "java.lang.String";

            case java.sql.Types.BINARY:
            case java.sql.Types.VARBINARY:
            case java.sql.Types.LONGVARBINARY:
                return "byte[]";

            case java.sql.Types.BLOB:
                return "java.sql.Blob";

            case java.sql.Types.CLOB:
                return "java.sql.Clob";

            case java.sql.Types.DATE:
                return "java.sql.Date";

            case java.sql.Types.TIME:
                return "java.sql.Time";

            case java.sql.Types.TIMESTAMP:
                return "java.sql.Timestamp";
        }

        return "java.lang.Object";
    }

    /**
     * Embed the data object as a string literal in the buffer supplied.
     *
     * @param buf The buffer in which the data will be embeded.
     * @param value The data object.
     */
    static void embedData(StringBuffer buf, Object value)
    throws SQLException {
        if (value == null) {
            buf.append("NULL");
            return;
        }

        if (value instanceof Blob) {
            Blob blob = (Blob) value;

            value = blob.getBytes(1, (int) blob.length());
        } else if (value instanceof Clob) {
            Clob clob = (Clob) value;

            value = clob.getSubString(1, (int) clob.length());
        }

        if (value instanceof byte[]) {
            byte[] bytes = (byte[]) value;

            int len = bytes.length;

            if (len > 0) {
                buf.append('0').append('x');

                for (int i = 0; i < len; i++) {
                    int b1 = bytes[i] & 0xFF;

                    buf.append(hex[b1 >> 4]);
                    buf.append(hex[b1 & 0x0F]);
                }
            } else {
                buf.append("NULL");
            }
        } else if (value instanceof String) {
            String tmp = (String) value;
            int len = tmp.length();

            buf.append('\'');

            for (int i = 0; i < len; i++) {
                char c = tmp.charAt(i);

                if (c == '\'') {
                    buf.append('\'');

                    if (i + 1 < len) {
                        if (tmp.charAt(i + 1) == '\'') {
                            i++;
                        }
                    }
                }

                buf.append(c);
            }

            buf.append('\'');
        } else if (value instanceof java.sql.Date) {
            synchronized (cal) {
                cal.setTime((java.sql.Date) value);
                buf.append('\'');
                long dt = cal.get(Calendar.YEAR) * 10000;
                dt += (cal.get(Calendar.MONTH) + 1) * 100;
                dt += cal.get(Calendar.DAY_OF_MONTH);
                buf.append(dt);
                buf.append(' ');
                int t = cal.get(Calendar.HOUR_OF_DAY);
                buf.append((t < 10) ? "0" + t + ":" : t + ":");
                t = cal.get(Calendar.MINUTE);
                buf.append((t < 10) ? "0" + t + ":" : t + ":");
                t = cal.get(Calendar.SECOND);
                buf.append((t < 10) ? "0" + t + "'" : t + "'");
            }
        } else
            if (value instanceof java.sql.Time) {
            synchronized (cal) {
                cal.setTime((java.sql.Time) value);
                buf.append('\'');
                int t = cal.get(Calendar.HOUR_OF_DAY);
                buf.append((t < 10) ? "0" + t + ":" : t + ":");
                t = cal.get(Calendar.MINUTE);
                buf.append((t < 10) ? "0" + t + ":" : t + ":");
                t = cal.get(Calendar.SECOND);
                buf.append((t < 10) ? "0" + t + "'" : t + "'");
            }
        } else
            if (value instanceof java.sql.Timestamp) {
            synchronized (cal) {
                cal.setTime((java.sql.Timestamp) value);
                buf.append('\'');
                long dt = cal.get(Calendar.YEAR) * 10000;
                dt += (cal.get(Calendar.MONTH) + 1) * 100;
                dt += cal.get(Calendar.DAY_OF_MONTH);
                buf.append(dt);
                buf.append(' ');
                int t = cal.get(Calendar.HOUR_OF_DAY);
                buf.append((t < 10) ? "0" + t + ":" : t + ":");
                t = cal.get(Calendar.MINUTE);
                buf.append((t < 10) ? "0" + t + ":" : t + ":");
                t = cal.get(Calendar.SECOND);
                buf.append((t < 10) ? "0" + t + "." : t + ".");
                t = (int)(cal.getTime().getTime() % 1000L);

                if (t < 100) {
                    buf.append('0');
                }

                if (t < 10) {
                    buf.append('0');
                }

                buf.append(t);
                buf.append('\'');
            }
        } else if (value instanceof Boolean) {
            buf.append(((Boolean) value).booleanValue() ? '1' : '0');
        } else if (value instanceof BigDecimal) {
            BigDecimal bd = (BigDecimal) value;

            if (bd.scale() > 10) { // Must be a better answer than this
                bd = bd.setScale(10, BigDecimal.ROUND_HALF_UP);
            }

            buf.append(bd.toString());
        } else {
            buf.append(value.toString());
        }
    }

    /**
     * Update the SQL string and replace the &#63; markers with parameter names
     * eg @P0, @P1 etc.
     *
     * @param sql The SQL containing markers to substitute.
     * @param list The Parameter list.
     * @return The modified SQL as a <code>String</code>.
     */
    static String substituteParamMarkers(String sql, ParamInfo[] list){
        StringBuffer buf = new StringBuffer(sql.length() + list.length * 4);
        int start = 0;

        for (int i = 0; i < list.length; i++) {
            int pos = list[i].markerPos;

            if (pos > 0) {
                buf.append(sql.substring(start, list[i].markerPos));
                start = pos + 1;
                buf.append("@P");
                buf.append(i);
            }
        }

        if (start < sql.length()) {
            buf.append(sql.substring(start));
        }

        return buf.toString();
    }

    /**
     * Substitute actual data for the parameter markers to simulate
     * parameter substitution in a PreparedStatement.
     *
     * @param sql The SQL containing parameter markers to substitute.
     * @param list The parameter descriptors.
     * @return The modified SQL statement.
     */
    static String substituteParameters(String sql, ParamInfo[] list)
    throws SQLException {
        int len = sql.length();

        for (int i = 0; i < list.length; i++) {
            if (!list[i].isRetVal && !list[i].isSet && !list[i].isOutput) {
                throw new SQLException(Support.getMessage("error.prepare.paramnotset",
                                                          Integer.toString(i+1)),
                                       "07000");
            }

            Object value = list[i].value;
            if (value instanceof java.io.InputStream ||
                value instanceof java.io.Reader) {
                try {
                    if (list[i].jdbcType == java.sql.Types.LONGVARCHAR) {
                        value = list[i].getString("US-ASCII");
                    } else {
                        value = list[i].getBytes("US-ASCII");
                    }
                } catch (java.io.IOException e) {
                    throw new SQLException(Support.getMessage("error.generic.ioerror",
                                                              e.getMessage()),
                                           "HY000");
                }
            }

            if (value instanceof String) {
                len += ((String) value).length() + 2;
            } else if (value instanceof byte[]) {
                len += ((byte[]) value).length * 2 + 2;
            } else {
                len += 10; // Default size
            }
        }

        StringBuffer buf = new StringBuffer(len);
        int start = 0;

        for (int i = 0; i < list.length; i++) {
            int pos = list[i].markerPos;

            if (pos > 0) {
                buf.append(sql.substring(start, list[i].markerPos));
                start = pos + 1;
                Support.embedData(buf, list[i].value);
            }
        }

        if (start < sql.length()) {
            buf.append(sql.substring(start));
        }

        return buf.toString();
    }

    /**
     * Encode a string into a byte array using the specified character set.
     *
     * @param cs The Charset name.
     * @param value The value to encode.
     * @return The value of the String as a <code>byte[]</code>.
     */
    static byte[] encodeString(String cs, String value) {
        try {
            return value.getBytes(cs);
        } catch (UnsupportedEncodingException e) {
            return value.getBytes();
        }
    }

    /**
     * Link an the original cause exception to a SQL Exception.
     * <p>If running under VM 1.4+ the Exception.initCause() method
     * will be used to chain the exception.
     * Modeled after the code written by Brian Heineman.
     *
     * @param sqle The SQLException to enhance.
     * @param cause The child exception to link.
     * @return The enhanced <code>SQLException</code>.
     */
    public static SQLException linkException(SQLException sqle, Throwable cause) {
        Class sqlExceptionClass = sqle.getClass();
        Class[] parameterTypes = new Class[] {Throwable.class};
        Object[] arguments = new Object[] {cause};

        try {
            Method initCauseMethod = sqlExceptionClass.getMethod("initCause",
                                                                 parameterTypes);
            initCauseMethod.invoke(sqle, arguments);
        } catch (NoSuchMethodException e) {
            // Ignore; this method does not exist in older JVM's.
            if (Logger.isActive()) {
                Logger.logException((Exception) cause); // Best we can do
            }
        } catch (Exception e) {
            // Ignore all other exceptions, do not prevent the main exception
            // from being returned if reflection fails for any reason...
        }

        return sqle;
    }

    // ------------- Private methods  ---------

    private Support() {
        // Prevent an instance of this class being created.
    }

    /**
     * Get runtime error using supplied key and substitute parameters
     * into message.
     *
     * @param key The key of the error message in Messages.properties
     * @param arguments The objects to insert into the message.
     * @return The selected error message as a <code>String</code>.
     */
    private static String getMessageText(String key, Object[] arguments) {
        //
        // ResourceBundle does caching so performance should be OK.
        //
        ResourceBundle rb = ResourceBundle.getBundle(defaultResource);

        String formatString;

        try {
            formatString = rb.getString(key);
        } catch (java.util.MissingResourceException mre) {
            throw new RuntimeException("no message resource found for message property "
                                       + key);
        }

        MessageFormat formatter = new MessageFormat(formatString);

        return formatter.format(arguments);
    }
}
