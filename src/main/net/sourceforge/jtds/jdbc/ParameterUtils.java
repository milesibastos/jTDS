//
// Copyright 1998 CDS Networks, Inc., Medford Oregon
//
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
// 1. Redistributions of source code must retain the above copyright
//    notice, this list of conditions and the following disclaimer.
// 2. Redistributions in binary form must reproduce the above copyright
//    notice, this list of conditions and the following disclaimer in the
//    documentation and/or other materials provided with the distribution.
// 3. All advertising materials mentioning features or use of this software
//    must display the following acknowledgement:
//      This product includes software developed by CDS Networks, Inc.
// 4. The name of CDS Networks, Inc.  may not be used to endorse or promote
//    products derived from this software without specific prior
//    written permission.
//
// THIS SOFTWARE IS PROVIDED BY CDS NETWORKS, INC. ``AS IS'' AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED.  IN NO EVENT SHALL CDS NETWORKS, INC. BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
// OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
// HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
// LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
// OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
// SUCH DAMAGE.
//

package net.sourceforge.jtds.jdbc;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Calendar;
import java.util.GregorianCalendar;

public class ParameterUtils {
    public static final String cvsVersion = "$Id: ParameterUtils.java,v 1.18 2004-05-07 04:38:08 bheineman Exp $";

    public static final int BOOLEAN = 16;
    public static final int DATALINK = 70;

    /**
     * Used to normalize date and time values.
     */
    private static Calendar staticCalendar = new GregorianCalendar();

    /**
     * Check that all items in parameterList have been given a value
     *
     * @exception SQLException thrown if one or more parameters aren't set
     */
    private static void verifyThatParametersAreSet(ParameterListItem[] parameterList)
            throws SQLException {
        for (int i = 0; i < parameterList.length; i++) {
            if (parameterList[i].isOutput) {
                // Allow output parameters to be "not set"
                parameterList[i].isSet = true;
            }

            if (!parameterList[i].isSet) {
                throw new SQLException("parameter #" + (i + 1) + " has not been set", "07001");
            }
        }
    }

    /**
     * Create the formal parameters for a parameter list.
     *
     * This method takes a sql string and a parameter list containing
     * the actual parameters and creates the formal parameters for
     * a stored procedure.  The formal parameters will later be used
     * when the stored procedure is submitted for creation on the server.
     *
     * @param rawQueryString   (in-only) the original SQL with '?' placehoders
     * @param parameterList    (update) the parameter list to update
     * @param tds              <code>Tds</code> the procedure will be executed
     *                         on
     * @param assignNames      if <code>true</code>, names are assigned to
     *                         parameters (for <code>PreparedStatement</code>s)
     */
    public static void createParameterMapping(String rawQueryString,
                                              ParameterListItem[] parameterList,
                                              Tds tds,
                                              boolean assignNames)
            throws SQLException {
        // First make sure the caller has filled in all the parameters.
        verifyThatParametersAreSet(parameterList);

//      int nextParameterNumber = 0;  MJH 14/03/04 Not needed
        int tdsVer = tds.getTdsVer();
        EncodingHelper encoder = tds.getEncoder();

        for (int i = 0; i < parameterList.length; i++) {
            if (assignNames) {
//
// MJH 14/03/04  Not sure what the point of this code was.
// What would happen if SQL string contained embedded strings
// such as P1 for example.
//              String nextFormal;
//              do {
//                  nextParameterNumber++;
//                  nextFormal = "P" + nextParameterNumber;
//              } while (-1 != rawQueryString.indexOf(nextFormal));
//
//              parameterList[i].formalName = '@' + nextFormal;
                parameterList[i].formalName = "@P" + (i+1);
            }

            switch (parameterList[i].type) {
                case Types.VARCHAR:
                case Types.CHAR:
                {
                    String value = (String)parameterList[i].value;

                    if (tdsVer == Tds.TDS70) {
                        // SQL Server 7 can handle Unicode so use it wherever
                        // possible AND required
                        if ((value == null || value.length() < 4001)
                            && tds.useUnicode()) {
                            parameterList[i].formalType = "nvarchar(4000)";
                            parameterList[i].maxLength = 4000;
                        } else if (value == null ||
                                    (value.length() < 8001
                                     && !encoder.isDBCS()
                                     && encoder.canBeConverted(value))) {
                            parameterList[i].formalType = "varchar(8000)";
                            parameterList[i].maxLength = 8000;
                        } else {
                            // SAfe We'll always use NTEXT (not TEXT) because CLOB
                            //      columns can't be index columns, so there's no
                            //      performance hit
                            parameterList[i].formalType = "ntext";
                            parameterList[i].maxLength = Integer.MAX_VALUE;
                        }
                    } else {
                        int len = 0; // This is for NULL values

                        if (value != null) {
                            len = value.length();

                            if (encoder.isDBCS() && len > 127 && len < 256) {
                                len = encoder.getBytes(value).length;
                            }
                        }

                        if (len < 256) {
                            parameterList[i].formalType = "varchar(255)";
                            parameterList[i].maxLength = 255;
                        } else {
                            parameterList[i].formalType = "text";
                            parameterList[i].maxLength = Integer.MAX_VALUE;
                        }
                    }
                    break;
                }
                case Types.CLOB:
                case Types.LONGVARCHAR:
                {
                    if (tdsVer == Tds.TDS70) {
                        parameterList[i].formalType = "ntext";
                    } else {
                        parameterList[i].formalType = "text";
                    }

                    parameterList[i].maxLength = Integer.MAX_VALUE;
                    break;
                }
                case Types.INTEGER:
                {
                    parameterList[i].formalType = "integer";
                    break;
                }
                case Types.FLOAT:
                case Types.REAL:
                {
                    parameterList[i].formalType = "real";
                    break;
                }
                case Types.DOUBLE:
                {
                    parameterList[i].formalType = "float";
                    break;
                }
                case Types.TIMESTAMP:
                case Types.DATE:
                case Types.TIME:
                {
                    parameterList[i].formalType = "datetime";
                    break;
                }
                case Types.BLOB:
                case Types.LONGVARBINARY:
                {
                    parameterList[i].formalType = "image";
                    break;
                }
                case Types.BINARY:
                case Types.VARBINARY:
                {
                    if (tdsVer == Tds.TDS70) {
                        parameterList[i].formalType = "varbinary(8000)";
                        parameterList[i].maxLength = 8000;
                    } else {
                        parameterList[i].formalType = "varbinary(255)";
                        parameterList[i].maxLength = 255;
                    }
                    break;
                }
                case Types.BIT:
                case ParameterUtils.BOOLEAN:
                {
                    parameterList[i].formalType = "bit";
                    break;
                }
                case Types.BIGINT: {
                    parameterList[i].formalType = "decimal("
                            + tds.getConnection().getMaxPrecision() + ",0)";
                    break;
                }
                case Types.SMALLINT:
                {
                    parameterList[i].formalType = "smallint";
                    break;
                }
                case Types.TINYINT:
                {
                    parameterList[i].formalType = "tinyint";
                    break;
                }
                case Types.DECIMAL:
                case Types.NUMERIC:
                {
                    int scale = parameterList[i].scale;
                    if (scale == -1) {
                        // @todo Find a way to do this properly (i.e. what
                        //       happens if the scale is larger than the
                        //       precision, e.g. if the BigDecimal was created
                        //       from a float/double - try AsTest)
                        // if (parameterList[i].value instanceof BigDecimal) {
                        //     scale = ((BigDecimal) parameterList[i].value).scale();
                        //     System.out.println("scale = " + scale);
                        //     new Exception().printStackTrace();
                        // } else {
                            scale = 10;
                        // }
                    }
                    parameterList[i].formalType = "decimal("
                            + tds.getConnection().getMaxPrecision() + ","
                            + scale + ")";
                    break;
                }
                case Types.NULL:
                case Types.OTHER:
                {
                    throw new SQLException("Not implemented (type is Types."
                                           + TdsUtil.javaSqlTypeToString(parameterList[i].type)
                                           + ")", "HY004");
                }
                default:
                {
                    throw new SQLException("Internal error.  Unrecognized type "
                                           + parameterList[i].type, "HY000");
                }
            }
        }
    }

    public static Object getObject(int sqlType,
                                   Object value,
                                   EncodingHelper encodingHelper)
    throws SQLException {
        if (value == null || sqlType == Types.NULL) {
            return null;
        } else {
            switch (sqlType) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                if (value instanceof String) {
                    return value;
                } else if (value instanceof byte[]) {
                    // Binary value, generate hex string
                    byte[] b = (byte[]) value;
                    StringBuffer buf = new StringBuffer(2 * b.length);

                    for (int i = 0; i < b.length; i++) {
                        int n = ((int) b[i]) & 0xFF;
                        int v = n >> 4;

                        buf.append((char) (v < 10 ? '0' + v : 'A' + v - 10));
                        v = n & 0x0F;
                        buf.append((char) (v < 10 ? '0' + v : 'A' + v - 10));
                    }

                    return buf.toString();
                } else if (value instanceof Boolean) {
                    return ((Boolean) value).booleanValue() ? "1" : "0";
                }

                return value.toString();
            case Types.CLOB:
                if (value instanceof Clob) {
                    return value;
                }

                return new ClobImpl(value.toString());
            case Types.TINYINT:
                if (value instanceof Byte) {
                    return value;
                } else if (value instanceof Number) {
                    return new Byte(((Number) value).byteValue());
                } else if (value instanceof Boolean) {
                    return ((Boolean) value).booleanValue() ? new Byte((byte) 1) : new Byte((byte) 0);
                }

                try {
                    return new Byte(value.toString().trim());
                } catch (NumberFormatException e) {
                    throw TdsUtil.getSQLException("Cannot convert " + value.getClass().getName()
                                                  + " to Byte", null, e);
                }
            case Types.SMALLINT:
                if (value instanceof Short) {
                    return value;
                } else if (value instanceof Number) {
                    return new Short(((Number) value).shortValue());
                } else if (value instanceof Boolean) {
                    return ((Boolean) value).booleanValue() ? new Short((short) 1) : new Short((short) 0);
                }

                try {
                    return new Short(value.toString().trim());
                } catch (NumberFormatException e) {
                    throw TdsUtil.getSQLException("Cannot convert " + value.getClass().getName()
                                                  + " to Short", null, e);
                }
            case Types.INTEGER:
                if (value instanceof Integer) {
                    return value;
                } else if (value instanceof Number) {
                    return new Integer(((Number) value).intValue());
                } else if (value instanceof Boolean) {
                    return ((Boolean) value).booleanValue() ? new Integer(1) : new Integer(0);
                }

                try {
                    return new Integer(value.toString().trim());
                } catch (NumberFormatException e) {
                    throw TdsUtil.getSQLException("Cannot convert " + value.getClass().getName()
                                                  + " to Integer", null, e);
                }
            case Types.BIGINT:
                if (value instanceof Long) {
                    return value;
                } else if (value instanceof Number) {
                    return new Long(((Number) value).longValue());
                } else if (value instanceof Boolean) {
                    return ((Boolean) value).booleanValue() ? new Long(1) : new Long(0);
                }

                try {
                    return new Long(value.toString().trim());
                } catch (NumberFormatException e) {
                    throw TdsUtil.getSQLException("Cannot convert " + value.getClass().getName()
                                                  + " to Long", null, e);
                }
            case Types.REAL:
                if (value instanceof Float) {
                    return value;
                } else if (value instanceof Number) {
                    return new Float(((Number) value).doubleValue());
                } else if (value instanceof Boolean) {
                    return ((Boolean) value).booleanValue() ? new Float(1) : new Float(0);
                }


                try {
                    return new Float(value.toString().trim());
                } catch (NumberFormatException e) {
                    throw TdsUtil.getSQLException("Cannot convert " + value.getClass().getName()
                                                  + " to Float", null, e);
                }
            case Types.FLOAT:
            case Types.DOUBLE:
                if (value instanceof Double) {
                    return value;
                } else if (value instanceof Number) {
                    return new Double(((Number) value).doubleValue());
                } else if (value instanceof Boolean) {
                    return ((Boolean) value).booleanValue() ? new Double(1) : new Double(0);
                }

                try {
                    return new Double(value.toString().trim());
                } catch (NumberFormatException e) {
                    throw TdsUtil.getSQLException("Cannot convert " + value.getClass().getName()
                                                  + " to Double", null, e);
                }
            case Types.DECIMAL:
            case Types.NUMERIC:
                if (value instanceof BigDecimal) {
                    return value;
                } else if (value instanceof java.lang.Double) {
                    return new BigDecimal(((Double) value).doubleValue());
                } else if (value instanceof java.lang.Float) {
                    return new BigDecimal(((Float) value).doubleValue());
                } else if (value instanceof Number) {
                    // This handles Byte, Short, Integer, and Long
                    return BigDecimal.valueOf(((Number) value).longValue());
                } else if (value instanceof Boolean) {
                    return ((Boolean) value).booleanValue() ? new BigDecimal(1) : new BigDecimal(0);
                }

                try {
                    return new BigDecimal(value.toString().trim());
                } catch (NumberFormatException e) {
                    throw TdsUtil.getSQLException("Cannot convert " + value.getClass().getName()
                                                  + " to BigDecimal", null, e);
                }
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                if (value instanceof byte[]) {
                    return (byte[]) value;
                }

                // This was previously only possible with String instances...
                return encodingHelper.getBytes(value.toString());
            case Types.BLOB:
                if (value instanceof Blob) {
                    return value;
                }

                return new BlobImpl((byte[]) getObject(Types.BINARY, value, encodingHelper));
            case Types.DATE:
                synchronized (staticCalendar) {
                    if (value instanceof Date) {
                        staticCalendar.setTime((Date) value);
                    } else if (value instanceof java.util.Date) {
                        staticCalendar.setTime(new Date(((java.util.Date) value).getTime()));
                    } else {
                        throw new SQLException("Cannot convert " + value.getClass().getName()
                                               + " to Date.");
                    }

                    staticCalendar.set(Calendar.HOUR_OF_DAY, 0);
                    staticCalendar.set(Calendar.MINUTE, 0);
                    staticCalendar.set(Calendar.SECOND, 0);
                    staticCalendar.set(Calendar.MILLISECOND, 0);

                    return new Date(staticCalendar.getTime().getTime());
                }
            case Types.TIME:
                synchronized(staticCalendar) {
                    if (value instanceof Time) {
                        staticCalendar.setTime((Time) value);
                    } else if (value instanceof java.util.Date) {
                        staticCalendar.setTime(new Time(((java.util.Date) value).getTime()));
                    } else {
                        throw new SQLException("Cannot convert " + value.getClass().getName()
                                               + " to Time.");
                    }

                    staticCalendar.set(Calendar.ERA, GregorianCalendar.AD);
                    staticCalendar.set(Calendar.YEAR, 1970);
                    staticCalendar.set(Calendar.MONTH, 0);
                    staticCalendar.set(Calendar.DAY_OF_MONTH, 1);
                    return new Time(staticCalendar.getTime().getTime());
                }
            case Types.TIMESTAMP:
                if (value instanceof Timestamp) {
                    return value;
                } else if (value instanceof java.util.Date) {
                    return new Timestamp(((java.util.Date) value).getTime());
                }

                throw new SQLException("Cannot convert " + value.getClass().getName()
                                       + " to Timestamp.");
            case Types.BIT:
            case ParameterUtils.BOOLEAN:
                if (value instanceof Boolean) {
                    return value;
                } else if (value instanceof Number) {
                    // Would somebody like to tell what a true/false has
                    // to do with a double?
                    // SAfe It looks like it has to work just like with BIT columns ('0' or 'false' for false and '1' or
                    //      'true' for true).
                    return ((Number) value).intValue() == 0 ? Boolean.FALSE : Boolean.TRUE;
                }
                String tmpValue = value.toString().trim();

                if (tmpValue.equals("0") || tmpValue.equalsIgnoreCase("false")) {
                    return Boolean.FALSE;
                }
//                    } else if (tmpValue.equals("1") || tmpValue.equalsIgnoreCase("true")) {
                    // Okay, I'm really confused as to what you mean
                    // by a character string being true or false.  What
                    // is the boolean value for "Let the wookie win"?
                    // But since the spec says I have to convert from
                    // character to boolean data...

                    // SAfe It looks like it has to work just like with BIT columns ('0' or 'false' for false and '1' or
                    //      'true' for true).

                    // SAfe Otherwise fall-through and throw an exception.

                    // 05/01/2004
                    // Why not just have an else statment that returns true instead of checking for specific values?
                    // (true being defined as !false); the logic has been updated to reflect this...
                return Boolean.TRUE;
            case Types.ARRAY:
            case ParameterUtils.DATALINK:
            case Types.DISTINCT:
            case Types.JAVA_OBJECT:
            case Types.OTHER:
            case Types.REF:
            case Types.STRUCT:
                throw new SQLException("Not implemented");
            default:
                throw new SQLException("Unsupported datatype " + sqlType);
//                throw new SQLException("Unknown datatype "
//                                       + getColumnType(columnIndex));
            }
        }
    }
}
