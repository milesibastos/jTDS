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

import java.io.*;
import java.math.BigInteger;
import java.math.BigDecimal;
import java.util.Calendar;
import java.sql.Timestamp;
import java.sql.SQLException;
import java.util.GregorianCalendar;

/**
 * Implement TDS data types and related I/O logic.
 * <p>
 * Implementation notes:
 * <bl>
 * <li>This class encapsulates all the knowledge about reading and writing
 *     TDS data descriptors and related application data.
 * <li>There are four key methods supplied here:
 * <ol>
 * <li>readType() - Reads the column and parameter meta data.
 * <li>readData() - Reads actual data values.
 * <li>writeParam() - Write parameter descriptors and data.
 * <li>getNativeType() - knows how to map JDBC data types to the equivalent TDS type.
 * </ol>
 * <li>The code needs to be extended to cope with varchar/varbinary columns > 255 bytes
 *     as supported by the latest Sybase versions.
 * </bl>
 *
 * @author Mike Hutchinson
 * @author Alin Sinpalean
 * @author freeTDS project
 * @version $Id: TdsData.java,v 1.12 2004-07-21 21:57:21 bheineman Exp $
 */
public class TdsData {
    /**
     * This class implements a descriptor for TDS data types;
     *
     * @author Mike Hutchinson.
     */
    private static class TypeInfo {
        /** The SQL type name. */
        public String sqlType;
        /**
         * The size of this type or &lt; 0 for variable sizes.
         * <p> Special values as follows:
         * <ol>
         * <li> -5 sql_variant type.
         * <li> -4 text, image or ntext types.
         * <li> -2 SQL Server 7+ long char and var binary types.
         * <li> -1 varchar, varbinary, null types.
         * </ol>
         */
        public int size;
        /**
         * The precision of the type.
         * <p>If this is -1 precision must be calculated from buffer size
         * eg for varchar fields.
         */
        public int precision;
        /**
         * The display size of the type.
         * <p>Special values are used if the precision field is -1.
         * <ol>
         * <li>Value = -2 - The display size is half the buffersize.
         * <li>Value = +2 - The display size is twice the buffersize.
         * <li>Value =  1 - The display size is equal to the buffersize.
         * </ol>
         */
        public int displaySize;
        /** true if type is a signed numeric. */
        public boolean isSigned;
        /** true if type requires TDS80 collation. */
        public boolean isCollation;
        /** The java.sql.Types constant for this data type. */
        public int jdbcType;

        /**
         * Construct a new TDS data type descriptor.
         *
         * @param sqlType   SQL type name.
         * @param size Byte size for this type or &lt; 0 for variable length types.
         * @param precision Decimal precision or -1
         * @param displaySize Printout size for this type or special values -1,-2.
         * @param isSigned True if signed numeric type.
         * @param isCollation True if type has TDS 8 collation information.
         * @param jdbcType The java.sql.Type constant for this type.
         */
        TypeInfo(String sqlType, int size, int precision, int displaySize,
                 boolean isSigned, boolean isCollation, int jdbcType) {
            this.sqlType = sqlType;
            this.size = size;
            this.precision = precision;
            this.displaySize = displaySize;
            this.isSigned = isSigned;
            this.isCollation = isCollation;
            this.jdbcType = jdbcType;
        }

    }

    /*
     * Constants for TDS data types
     */
    private static final int SYBCHAR               = 47; // 0x2F
    private static final int SYBVARCHAR            = 39; // 0x27
    private static final int SYBINTN               = 38; // 0x26
    private static final int SYBINT1               = 48; // 0x30
    private static final int SYBINT2               = 52; // 0x34
    private static final int SYBINT4               = 56; // 0x38
    private static final int SYBINT8               = 127;// 0x7F
    private static final int SYBFLT8               = 62; // 0x3E
    private static final int SYBDATETIME           = 61; // 0x3D
    private static final int SYBBIT                = 50; // 0x32
    private static final int SYBTEXT               = 35; // 0x23
    private static final int SYBNTEXT              = 99; // 0x63
    private static final int SYBIMAGE              = 34; // 0x22
    private static final int SYBMONEY4             = 122;// 0x7A
    private static final int SYBMONEY              = 60; // 0x3C
    private static final int SYBDATETIME4          = 58; // 0x3A
    private static final int SYBREAL               = 59; // 0x3B
    private static final int SYBBINARY             = 45; // 0x2D
    private static final int SYBVOID               = 31; // 0x1F
    private static final int SYBVARBINARY          = 37; // 0x25
    private static final int SYBNVARCHAR           = 103;// 0x67
    private static final int SYBBITN               = 104;// 0x68
    private static final int SYBNUMERIC            = 108;// 0x6C
    private static final int SYBDECIMAL            = 106;// 0x6A
    private static final int SYBFLTN               = 109;// 0x6D
    private static final int SYBMONEYN             = 110;// 0x6E
    private static final int SYBDATETIMN           = 111;// 0x6F
    private static final int XSYBCHAR              = 175;// 0xAF
    private static final int XSYBVARCHAR           = 167;// 0xA7
    private static final int XSYBNVARCHAR          = 231;// 0xE7
    private static final int XSYBNCHAR             = 239;// 0xEF
    private static final int XSYBVARBINARY         = 165;// 0xA5
    private static final int XSYBBINARY            = 173;// 0xAD
    private static final int SYBLONGBINARY         = 225;// 0xE1
    private static final int SYBSINT1              = 64; // 0x40
    private static final int SYBUINT2              = 65; // 0x41
    private static final int SYBUINT4              = 66; // 0x42
    private static final int SYBUINT8              = 67; // 0x43
    private static final int SYBUNIQUE             = 36; // 0x24
    private static final int SYBVARIANT            = 98; // 0x62

    /**
     * Array of TDS data type descriptors.
     */
    private final static TypeInfo types[] = new TypeInfo[256];

    /**
     * Static block to initialise TDS data type descriptors.
     */
    static {//                             SQL Type       Size Prec  DS signed TDS8 Col java Type
        types[SYBCHAR]      = new TypeInfo("char",          -1, -1,  1, false, false, java.sql.Types.CHAR);
        types[SYBVARCHAR]   = new TypeInfo("varchar",       -1, -1,  1, false, false, java.sql.Types.VARCHAR);
        types[SYBINTN]      = new TypeInfo("int",           -1, 10, 11, true,  false, java.sql.Types.INTEGER);
        types[SYBINT1]      = new TypeInfo("tinyint",        1,  2,  3, false, false, java.sql.Types.TINYINT);
        types[SYBINT2]      = new TypeInfo("smallint",       2,  5,  6, true,  false, java.sql.Types.SMALLINT);
        types[SYBINT4]      = new TypeInfo("int",            4, 10, 11, true,  false, java.sql.Types.INTEGER);
        types[SYBINT8]      = new TypeInfo("bigint",         8, 20, 20, true,  false, java.sql.Types.BIGINT);
        types[SYBFLT8]      = new TypeInfo("float",          8, 15, 24, true,  false, java.sql.Types.DOUBLE);
        types[SYBDATETIME]  = new TypeInfo("datetime",       8, 23, 23, false, false, java.sql.Types.TIMESTAMP);
        types[SYBBIT]       = new TypeInfo("bit",            1,  1,  1, false, false, java.sql.Types.BIT);
        types[SYBTEXT]      = new TypeInfo("text",          -4, -1,  1, false, true,  java.sql.Types.LONGVARCHAR);
        types[SYBNTEXT]     = new TypeInfo("ntext",         -4, -1, -2, false, true,  java.sql.Types.LONGVARCHAR);
        types[SYBIMAGE]     = new TypeInfo("image",         -4, -1,  2, false, false, java.sql.Types.LONGVARBINARY);
        types[SYBMONEY4]    = new TypeInfo("smallmoney",     4, 10, 12, true,  false, java.sql.Types.DECIMAL);
        types[SYBMONEY]     = new TypeInfo("money",          8, 19, 21, true,  false, java.sql.Types.DECIMAL);
        types[SYBDATETIME4] = new TypeInfo("smalldatetime",  4, 16, 19, false, false, java.sql.Types.TIMESTAMP);
        types[SYBREAL]      = new TypeInfo("real",           4,  7, 14, true,  false, java.sql.Types.REAL);
        types[SYBBINARY]    = new TypeInfo("binary",        -1,  1,  2, false, false, java.sql.Types.BINARY);
        types[SYBVOID]      = new TypeInfo("void",          -1,  1,  1, false, false, 0);
        types[SYBVARBINARY] = new TypeInfo("varbinary",     -1, -1,  2, false, false, java.sql.Types.VARBINARY);
        types[SYBNVARCHAR]  = new TypeInfo("nvarchar",      -1, -1, -2, false, false, java.sql.Types.VARCHAR);
        types[SYBBITN]      = new TypeInfo("bit",           -1,  1,  1, false, false, java.sql.Types.BIT);
        types[SYBNUMERIC]   = new TypeInfo("numeric",       -1, -1, -1, true,  false, java.sql.Types.NUMERIC);
        types[SYBDECIMAL]   = new TypeInfo("decimal",       -1, -1, -1, true,  false, java.sql.Types.DECIMAL);
        types[SYBFLTN]      = new TypeInfo("float",         -1, 15, 24, true,  false, java.sql.Types.DOUBLE);
        types[SYBMONEYN]    = new TypeInfo("money",         -1, 19, 21, true,  false, java.sql.Types.DECIMAL);
        types[SYBDATETIMN]  = new TypeInfo("datetime",      -1, 23, 23, false, false, java.sql.Types.TIMESTAMP);
        types[XSYBCHAR]     = new TypeInfo("char",          -2, -1,  1, false, true,  java.sql.Types.CHAR);
        types[XSYBVARCHAR]  = new TypeInfo("varchar",       -2, -1,  1, false, true,  java.sql.Types.VARCHAR);
        types[XSYBNVARCHAR] = new TypeInfo("nvarchar",      -2, -1, -2, false, true,  java.sql.Types.VARCHAR);
        types[XSYBNCHAR]    = new TypeInfo("nchar",         -2, -1, -2, false, true,  java.sql.Types.CHAR);
        types[XSYBVARBINARY]= new TypeInfo("varbinary",     -2, -1,  2, false, false, java.sql.Types.VARBINARY);
        types[XSYBBINARY]   = new TypeInfo("binary",        -2, -1,  2, false, false, java.sql.Types.BINARY);
        types[SYBLONGBINARY]= new TypeInfo("long binary",   -2, -1,  2, false, false, java.sql.Types.BINARY);
        types[SYBSINT1]     = new TypeInfo("tinyint",        1,  2,  3, false, false, java.sql.Types.TINYINT);
        types[SYBUINT2]     = new TypeInfo("smallint",       2,  5,  6, false, false, java.sql.Types.SMALLINT);
        types[SYBUINT4]     = new TypeInfo("int",            4, 10, 11, false, false, java.sql.Types.INTEGER);
        types[SYBUINT8]     = new TypeInfo("bigint",         8, 20, 20, false, false, java.sql.Types.BIGINT);
        types[SYBUNIQUE]    = new TypeInfo("uniqueidentifier",-1,36, 36, false, false, java.sql.Types.VARCHAR);
        types[SYBVARIANT]   = new TypeInfo("sql_variant",   -5,  1,  1, false, false, java.sql.Types.OTHER);
    }

    /** Default Decimal Scale. */
    static final int DEFAULT_SCALE = 10;

    /**
     * TDS 8 supplies collation information for character data types.
     *
     * @param in The Server response stream.
     * @param ci The column descriptor.
     * @return The number of bytes read from the stream as a <code>int</code>.
     * @throws IOException
     */
    static int getCollation(ResponseStream in, ColInfo ci) throws IOException {
        if (TdsData.isCollation(ci)) {
            // Read TDS8 collation info
            ci.collation = new byte[5];
            in.read(ci.collation);

            return 5;
        }

        return 0;
    }

    /**
     * Read the TDS datastream and populate the ColInfo parameter with
     * data type and related information.
     * <p>The type infomation conforms to one of the following formats:
     * <ol>
     * <li> [int1 type]  - eg SYBINT4.
     * <li> [int1 type] [int1 buffersize]  - eg VARCHAR &lt; 256
     * <li> [int1 type] [int2 buffersize]  - eg VARCHAR &gt; 255.
     * <li> [int1 type] [int4 buffersize] [int1 tabnamelen] [int1*n tabname] - eg text.
     * <li> [int1 type] [int4 buffersize] - eg sql_variant.
     * <li> [int1 type] [int1 buffersize] [int1 precision] [int1 scale] - eg decimal.
     * </ol>
     * For TDS 8 large character types include a 5 byte collation field after the buffer size.
     *
     * @param in The server response stream.
     * @param ci The ColInfo column descriptor object.
     * @return The number of bytes read from the input stream.
     * @throws IOException
     * @throws ProtocolException
     */
    static int readType(ResponseStream in, ColInfo ci)
    throws IOException, ProtocolException {
        boolean isTds8 = in.getTdsVersion() >= TdsCore.TDS80;
        int bytesRead = 1;
        // Get the TDS data type code
        int type = in.read();

        if (types[type] == null) {
            throw new ProtocolException("Invalid TDS data type 0x" + Integer.toHexString(type));
        }

        ci.tdsType = type;
        ci.jdbcType = types[type].jdbcType;
        ci.displaySize = -1;
        ci.tableName = "";
        ci.bufferSize = types[type].size;
        ci.nullable = java.sql.ResultSetMetaData.columnNoNulls;

        // Now get the buffersize if required
        if (ci.bufferSize == -5) {
            // sql_variant
            ci.bufferSize = in.readInt();
            bytesRead += 4;
        } else if (ci.bufferSize == -4) {
            // text or image
            ci.bufferSize = in.readInt();

            if (isTds8) {
                bytesRead += getCollation(in, ci);
            }

            int lenName = in.readShort();

            ci.tableName = in.readString(lenName);
            bytesRead += 6 + ((in.getTdsVersion() >= TdsCore.TDS70) ? lenName * 2 : lenName);
        } else if (ci.bufferSize == -2) {
            // longvarchar longvarbinary
            ci.bufferSize = in.readShort();

            if (isTds8) {
                bytesRead += getCollation(in, ci);
            }

            bytesRead += 2;
        } else if (ci.bufferSize == -1) {
            // varchar varbinary decimal etc
            bytesRead += 1;
            ci.bufferSize = in.read();
        }

        // Set scale for money and date types
        if (isCurrency(ci)) {
            ci.scale = 4;
        } else if (type == SYBDATETIME ||(type == SYBDATETIMN && ci.bufferSize == 8)) {
            ci.scale = 3;
        }

        // Set default displaySize and precision
        ci.displaySize = types[type].displaySize;
        ci.precision = types[type].precision;
        ci.sqlType = types[type].sqlType;

        // Now fine tune sizes for specific types
        if (type == SYBDATETIMN) {
            ci.nullable = java.sql.ResultSetMetaData.columnNullable;

            if (ci.bufferSize == 8) {
                ci.displaySize = types[SYBDATETIME].displaySize;
                ci.precision = types[SYBDATETIME].precision;
            } else {
                ci.displaySize = types[SYBDATETIME4].displaySize;
                ci.precision = types[SYBDATETIME4].precision;
                ci.sqlType = types[SYBDATETIME4].sqlType;
            }
        } else if (type == SYBFLTN) {
            ci.nullable = java.sql.ResultSetMetaData.columnNullable;

            if (ci.bufferSize == 8) {
                ci.displaySize = types[SYBFLT8].displaySize;
                ci.precision = types[SYBFLT8].precision;
            } else {
                ci.displaySize = types[SYBREAL].displaySize;
                ci.precision = types[SYBREAL].precision;
                ci.jdbcType = java.sql.Types.FLOAT;
                ci.sqlType = types[SYBREAL].sqlType;
            }
        } else if (type == SYBINTN) {
            ci.nullable = java.sql.ResultSetMetaData.columnNullable;

            if (ci.bufferSize == 8) {
                ci.displaySize = types[SYBINT8].displaySize;
                ci.precision = types[SYBINT8].precision;
                ci.jdbcType = java.sql.Types.BIGINT;
                ci.sqlType = types[SYBINT8].sqlType;
            } else if (ci.bufferSize == 4) {
                ci.displaySize = types[SYBINT4].displaySize;
                ci.precision = types[SYBINT4].precision;
            } else if (ci.bufferSize == 2) {
                ci.displaySize = types[SYBINT2].displaySize;
                ci.precision = types[SYBINT2].precision;
                ci.jdbcType = java.sql.Types.SMALLINT;
                ci.sqlType = types[SYBINT2].sqlType;
            } else {
                ci.displaySize = types[SYBINT1].displaySize;
                ci.precision = types[SYBINT1].precision;
                ci.jdbcType = java.sql.Types.TINYINT;
                ci.sqlType = types[SYBINT1].sqlType;
            }
        } else if (type == SYBMONEYN) {
            ci.nullable = java.sql.ResultSetMetaData.columnNullable;

            if (ci.bufferSize == 8) {
                ci.displaySize = types[SYBMONEY].displaySize;
                ci.precision = types[SYBMONEY].precision;
            } else {
                ci.displaySize = types[SYBMONEY4].displaySize;
                ci.precision = types[SYBMONEY4].precision;
                ci.sqlType = types[SYBMONEY4].sqlType;
            }
        }

        // Set sizes for character types
        if (ci.precision == -1) {
            ci.precision  = ci.bufferSize;

            if (ci.displaySize == -2) {
                ci.displaySize = ci.bufferSize / 2;
            } else if (ci.displaySize == 2) {
                ci.displaySize = ci.precision * 2;
            } else {
                ci.displaySize = ci.precision;
            }
        }

        // Read in scale and precision for decimal types
        if (type == SYBDECIMAL || type == SYBNUMERIC) {
            ci.precision = in.read();
            ci.scale = in.read();
            ci.displaySize = ((ci.scale > 0) ? 2 : 1) + ci.precision;
            bytesRead += 2;
            StringBuffer tmp = new StringBuffer(16);
            tmp.append(types[type].sqlType).append('(').append(ci.precision);

            if (ci.scale > 0) {
                tmp.append(',').append(ci.scale);
            }

            tmp.append(')');
            ci.sqlType = tmp.toString();
        }

        return bytesRead;
    }

    /**
     * Read the TDS data item from the Response Stream.
     * <p> The data size is either implicit in the type for example
     * fixed size integers, or a count field precedes the actual data.
     * The size of the count field varies with the data type.
     *
     * @param in The server ResponseStream.
     * @param ci The ColInfo column descriptor object.
     * @param readTextMode True if reading results from ReadText;
     * @return The data item Object or null.
     * @throws IOException
     * @throws ProtocolException
     */
    static Object readData(ResponseStream in, ColInfo ci, boolean readTextMode)
    throws IOException, ProtocolException {
        int len;

        switch (ci.tdsType) {
            case SYBINTN:
                switch (in.read()) {
                    case 1:
                        return new Byte((byte) in.read());
                    case 2:
                        return new Short((short) in.readShort());
                    case 4:
                        return new Integer(in.readInt());
                    case 8:
                        return new Long(in.readLong());
                }

                break;

            case SYBINT1:
                return new Byte((byte) in.read());

            case SYBINT2:
                return new Short((short) in.readShort());

            case SYBINT4:
                return new Integer(in.readInt());

            case SYBINT8:
                return new Long(in.readLong());

            case SYBIMAGE:
                len = in.read();

                if (len > 0) {
                	return new BlobImpl(in);
                }

                break;

            case SYBTEXT:
                len = in.read();

                if (len > 0) {
                    return new ClobImpl(in, false, readTextMode);
                }

                break;

            case SYBNTEXT:
                len = in.read();

                if (len > 0) {
                	return new ClobImpl(in, true, readTextMode);
                }

                break;

            case SYBCHAR:
            case SYBVARCHAR:
                len = in.read();

                if (len == 1 && in.getTdsVersion() < TdsCore.TDS70) {
                    // In TDS 4/5 zero length strings are stored as a single space
                    // to distinguish them from nulls.
                    String value = in.readAsciiString(len);

                    return (value.equals(" ")) ? "" : value;
                }

                if (len > 0) {
                    return in.readAsciiString(len);
                }

                break;

            case SYBNVARCHAR:
                len = in.read();

                if (len > 0) {
                    return in.readString(len / 2);
                }

                break;

            case XSYBCHAR:
            case XSYBVARCHAR:
                len = in.readShort();

                if (len != -1) {
                    return in.readAsciiString(len);
                }

                break;

            case XSYBNCHAR:
            case XSYBNVARCHAR:
                len = in.readShort();

                if (len != -1) {
                    return in.readString(len / 2);
                }

                break;

            case SYBVARBINARY:
            case SYBBINARY:
                len = in.read();

                if (len > 0) {
                    byte[] bytes = new byte[len];

                    in.read(bytes);

                    return bytes;
                }

                break;

            case XSYBVARBINARY:
            case XSYBBINARY:
                len = in.readShort();

                if (len != -1) {
                    byte[] bytes = new byte[len];

                    in.read(bytes);

                    return bytes;
                }

                break;

            case SYBMONEY4:
            case SYBMONEY:
            case SYBMONEYN:
                return getMoneyValue(in, ci.tdsType);

            case SYBDATETIME4:
            case SYBDATETIMN:
            case SYBDATETIME:
                return getDatetimeValue(in, ci.tdsType);

            case SYBBIT:
                return (in.read() != 0) ? Boolean.TRUE : Boolean.FALSE;

            case SYBBITN:
                len = in.read();

                if (len > 0) {
                    return (in.read() != 0) ? Boolean.TRUE : Boolean.FALSE;
                }

                break;

            case SYBREAL:
                return new Float(Float.intBitsToFloat(in.readInt()));

            case SYBFLT8:
                return new Double(Double.longBitsToDouble(in.readLong()));

            case SYBFLTN:
                len = in.read();

                if (len == 4) {
                    return new Float(Float.intBitsToFloat(in.readInt()));
                } else if (len == 8) {
                    return new Double(Double.longBitsToDouble(in.readLong()));
                }

                break;

            case SYBUNIQUE:
                len = in.read();

                if (len > 0) {
                    byte[] bytes = new byte[len];

                    in.read(bytes);

                    return new UniqueIdentifier(bytes);
                }

                break;

            case SYBNUMERIC:
            case SYBDECIMAL:
                len = in.read();

                if (len > 0) {
                    int sign = in.read();

                    len--;
                    byte[] bytes = new byte[len];
                    BigInteger bi;

                    if (in.getServerType() == TdsCore.SYBASE) {
                        // Sybase order is MSB first!
                        for (int i = 0; i < len; i++) {
                            bytes[i] = (byte) in.read();
                        }

                        bi = new BigInteger((sign == 0) ? 1 : -1, bytes);
                    } else {
                        while (len-- > 0) {
                            bytes[len] = (byte)in.read();
                        }

                        bi = new BigInteger((sign == 0) ? -1 : 1, bytes);
                    }

                    return new BigDecimal(bi, ci.scale);
                }

                break;

            case SYBVARIANT:
                return getVariant(in);

            default:
                throw new ProtocolException("Unsupported TDS data type 0x"
                                            + Integer.toHexString(ci.tdsType));
        }

        return null;
    }

    /**
     * Retrieve the signed status of the column.
     *
     * @param ci The column meta data.
     * @return <code>boolean</code> true if the column is a signed numeric.
     */
    static boolean isSigned(ColInfo ci) {
        int type = ci.tdsType;

        if (type < 0 || type > 255 || types[type] == null) {
            throw new IllegalArgumentException("TDS data type " + type
                    + " invalid");
        }

        return types[type].isSigned;
    }

    /**
     * Retrieve the collation status of the column.
     *
     * <p>TDS8 character columns include collation information.
     * @param ci The column meta data.
     * @return <code>boolean</code> true if the column requires collation data.
     */
    static boolean isCollation(ColInfo ci) {
        int type = ci.tdsType;

        if (type < 0 || type > 255 || types[type] == null) {
            throw new IllegalArgumentException("TDS data type " + type
                    + " invalid");
        }

        return types[type].isCollation;
    }
    /**
     * Retrieve the currency status of the column.
     *
     * @param ci The column meta data.
     * @return <code>boolean</code> true if the column is a currency type.
     */
    static boolean isCurrency(ColInfo ci) {
        int type = ci.tdsType;

        if (type < 0 || type > 255 || types[type] == null) {
            throw new IllegalArgumentException("TDS data type " + type
                    + " invalid");
        }

        return type == SYBMONEY || type == SYBMONEY4 || type == SYBMONEYN;
    }

    /**
     * Retrieve the searchable status of the column.
     *
     * @param ci The column meta data.
     * @return <code>boolean</code> true if the column is not a text or image type.
     */
    static boolean isSearchable(ColInfo ci) {
        int type = ci.tdsType;

        if (type < 0 || type > 255 || types[type] == null) {
            throw new IllegalArgumentException("TDS data type " + type
                    + " invalid");
        }

        return types[type].size != -4;
    }

    /**
     * Retrieve the TDS native type code for the parameter.
     *
     * @param connection The connectionJDBC object.
     * @param pi The parameter descriptor.
     * @throws SQLException
     */
    static void getNativeType(ConnectionJDBC2 connection, ParamInfo pi)
    throws SQLException {
        int len;
        int jdbcType = pi.jdbcType;

        if (jdbcType == java.sql.Types.OTHER) {
            jdbcType = Support.getJdbcType(pi.value);
        }

        switch (jdbcType) {
            case java.sql.Types.CHAR:
            case java.sql.Types.VARCHAR:
            case java.sql.Types.LONGVARCHAR:
            case java.sql.Types.CLOB:
                if (pi.value == null) {
                    len = 0;
                } else {
                    len = pi.length;
                }
                if (connection.getTdsVersion() < TdsCore.TDS70) {
                    if (len < 256) {
                        pi.tdsType = SYBVARCHAR;
                        pi.sqlType = "varchar(255)";
                    } else {
                        pi.tdsType = SYBTEXT;
                        pi.sqlType = "text";
                    }
                } else {
                    if (pi.isUnicode && len < 4001) {
                        pi.tdsType = XSYBNVARCHAR;
                        pi.sqlType = "nvarchar(4000)";
                    } else if (!pi.isUnicode && len < 8001) {
                        pi.tdsType = XSYBVARCHAR;
                        pi.sqlType = "varchar(8000)";
                    } else {
                        if (pi.isOutput) {
                            throw new SQLException(
                                                  Support.getMessage("error.textoutparam"), "HY000");
                        }

                        if (pi.isUnicode) {
                            pi.tdsType = SYBNTEXT;
                            pi.sqlType = "ntext";
                        } else {
                            pi.tdsType = SYBTEXT;
                            pi.sqlType = "text";
                        }
                    }
                }
                break;

            case java.sql.Types.TINYINT:
            case java.sql.Types.SMALLINT:
            case java.sql.Types.INTEGER:
                pi.tdsType = SYBINTN;
                pi.sqlType = "int";
                break;

            case JtdsStatement.BOOLEAN:
            case java.sql.Types.BIT:
                if (connection.getTdsVersion() < TdsCore.TDS70) {
                    pi.tdsType = SYBBIT;
                } else {
                    pi.tdsType = SYBBITN;
                }

                pi.sqlType = "bit";
                break;

            case java.sql.Types.FLOAT:
            case java.sql.Types.REAL:
            case java.sql.Types.DOUBLE:
                pi.tdsType = SYBFLTN;
                pi.sqlType = "float";
                break;

            case java.sql.Types.DATE:
            case java.sql.Types.TIME:
            case java.sql.Types.TIMESTAMP:
                pi.tdsType = SYBDATETIMN;
                pi.sqlType = "datetime";
                break;

            case java.sql.Types.BINARY:
            case java.sql.Types.VARBINARY:
            case java.sql.Types.BLOB:
            case java.sql.Types.LONGVARBINARY:
                if (pi.value == null) {
                    len = 0;
                } else {
                    len = pi.length;
                }

                if (connection.getTdsVersion() < TdsCore.TDS70) {
                    if (len < 256) {
                        pi.tdsType = SYBVARBINARY;
                        pi.sqlType = "varbinary(255)";
                    } else {
                        pi.tdsType = SYBIMAGE;
                        pi.sqlType = "image";
                    }
                } else {
                    if (len < 8001) {
                        pi.tdsType = XSYBVARBINARY;
                        pi.sqlType = "varbinary(8000)";
                    } else {
                        if (pi.isOutput) {
                            throw new SQLException(
                                                  Support.getMessage("error.textoutparam"), "HY000");
                        }

                        pi.tdsType = SYBIMAGE;
                        pi.sqlType = "image";
                    }
                }

                break;

            case java.sql.Types.BIGINT:
                if (connection.getTdsVersion() >= TdsCore.TDS80) {
                    pi.tdsType = SYBINTN;
                    pi.sqlType = "bigint";
                } else {
                    // int8 not supported send as a decimal field
                    pi.tdsType  = SYBDECIMAL;

                    if (connection.getMaxPrecision() > 28) {
                        pi.sqlType = "decimal(38)";
                    } else {
                        pi.sqlType = "decimal(28)";
                    }
                }

                break;

            case java.sql.Types.DECIMAL:
            case java.sql.Types.NUMERIC:
                pi.tdsType  = SYBDECIMAL;
                if (connection.getMaxPrecision() > 28) {
                    pi.sqlType = "decimal(38,10)";
                } else {
                    pi.sqlType = "decimal(28,10)";
                }

                if (pi.value instanceof BigDecimal) {
                    BigDecimal value = (BigDecimal)pi.value;
                    if (connection.getMaxPrecision() > 28) {
                        if (value.scale() > 10 || value.compareTo(limit38) > 0) {
                            pi.sqlType = "decimal(38," + value.scale() + ")";
                        }
                    } else {
                        if (value.scale() > 10 || value.compareTo(limit28) > 0) {
                            pi.sqlType = "decimal(28," + value.scale() + ")";
                        }
                    }
                }

                break;

            case java.sql.Types.OTHER:
            case java.sql.Types.NULL:
                // Send a null String in the absence of anything better
                pi.tdsType = SYBVARCHAR;
                pi.sqlType = "varchar(255)";
                break;

            default:
                throw new SQLException(Support.getMessage(
                                                         "error.baddatatype",
                                                         Integer.toString(pi.jdbcType)), "HY000");
        }
    }

    /**
     * Calculate the size of the parameter descriptor array for TDS 5 packets.
     *
     * @param charset The encoding character set.
     * @param isWideChar True if multi byte encoding.
     * @param pi The parameter to describe.
     * @param useParamNames True if named parameters should be used.
     * @return The size of the parameter descriptor as an <code>int</code>.
     */
    static int getTds5ParamSize(String charset,
                                boolean isWideChar,
                                ParamInfo pi,
                                boolean useParamNames) {
        int size = 8;
        if (pi.name != null && useParamNames) {
            // Size of parameter name
            if (isWideChar) {
                byte[] buf = Support.encodeString(charset, pi.name);

                size += buf.length;
            } else {
                size += pi.name.length();
            }
        }

        switch (pi.tdsType) {
            case SYBVARCHAR:
            case SYBVARBINARY:
            case SYBINTN:
            case SYBFLTN:
            case SYBDATETIMN:
                size += 1;
                break;
            case SYBDECIMAL:
                size += 3;
                break;
            case SYBBIT:
                break;
            default:
                throw new IllegalStateException("Unsupported output TDS type 0x"
                        + Integer.toHexString(pi.tdsType));
        }

        return size;
    }

    /**
     * Write a TDS 5 parameter format descriptor.
     *
     * @param out The server RequestStream.
     * @param charset The encoding character set.
     * @param isWideChar True if multi byte encoding.
     * @param pi The parameter to describe.
     * @param useParamNames True if named parameters should be used.
     * @throws IOException
     */
    static void writeTds5ParamFmt(RequestStream out,
                                  String charset,
                                  boolean isWideChar,
                                  ParamInfo pi,
                                  boolean useParamNames)
    throws IOException {
        if (pi.name != null && useParamNames) {
            // Output parameter name.
            if (isWideChar) {
                byte[] buf = Support.encodeString(charset, pi.name);

                out.write((byte) buf.length);
                out.write(buf);
            } else {
                out.write((byte) pi.name.length());
                out.write(pi.name);
            }
        } else {
            out.write((byte)0);
        }

        out.write((byte) (pi.isOutput ? 1 : 0)); // Output param
        out.write((int) 0); // user type
        out.write((byte) pi.tdsType); // TDS data type token

        // Output length fields
        switch (pi.tdsType) {
            case SYBVARCHAR:
            case SYBVARBINARY:
                out.write((byte) 255);
                break;
            case SYBBIT:
                break;
            case SYBINTN:
                out.write((byte) 4);
                break;
            case SYBFLTN:
            case SYBDATETIMN:
                out.write((byte) 8);
                break;
            case SYBDECIMAL:
                out.write((byte) 17);
                out.write((byte) 38);

                if (pi.jdbcType == java.sql.Types.BIGINT) {
                    out.write((byte) 0);
                } else {
                    if (pi.value instanceof BigDecimal) {
                        out.write((byte) ((BigDecimal) pi.value).scale());
                    } else {
                        out.write((byte) 10);
                    }
                }

                break;
            default:
                throw new IllegalStateException(
                                               "Unsupported output TDS type " + Integer.toHexString(pi.tdsType));
        }

        out.write((byte) 0); // Locale information
    }

    /**
     * Write the actual TDS 5 parameter data.
     *
     * @param out The server RequestStream.
     * @param charset The encoding character set.
     * @param isWideChar True if multi byte encoding.
     * @param pi The parameter to output.
     * @throws IOException
     * @throws SQLException
     */
    static void writeTds5Param(RequestStream out,
                               String charset,
                               boolean isWideChar,
                               ParamInfo pi)
    throws IOException, SQLException {
        int len;

        switch (pi.tdsType) {

            case SYBVARCHAR:
                if (pi.value == null) {
                    out.write((byte) 0);
                } else {
                    byte buf[] = pi.getBytes(charset);

                    if (buf.length == 0) {
                        buf = new byte[1];
                        buf[0] = ' ';
                    }

                    if (buf.length > 255) {
                        throw new SQLException(
                                              Support.getMessage("error.generic.truncmbcs"), "HY000");
                    }

                    out.write((byte) buf.length);
                    out.write(buf);
                }

                break;

            case SYBVARBINARY:
                if (pi.value == null) {
                    out.write((byte) 0);
                } else {
                    byte buf[] = pi.getBytes(charset);

                    out.write((byte) buf.length);
                    out.write(buf);
                }

                break;

            case SYBINTN:
                if (pi.value == null) {
                    out.write((byte) 0);
                } else {
                    out.write((byte) 4);
                    out.write(((Number) pi.value).intValue());
                }

                break;

            case SYBFLTN:
                if (pi.value == null) {
                    out.write((byte) 0);
                } else {
                    out.write((byte) 8);
                    out.write(((Number) pi.value).doubleValue());
                }

                break;

            case SYBDATETIMN:
                putDateTimeValue(out, pi.value);
                break;

            case SYBBIT:
                if (pi.value == null) {
                    out.write((byte) 0);
                } else {
                    out.write((byte) (((Boolean) pi.value).booleanValue() ? 1 : 0));
                }

                break;

            case SYBNUMERIC:
            case SYBDECIMAL:
                BigDecimal value = null;

                if (pi.value != null) {
                    if (pi.value instanceof Long) {
                        value = new BigDecimal(((Long) pi.value).longValue());
                    } else {
                        value = (BigDecimal) pi.value;
                    }
                }

                out.write(value);
                break;

            default:
                throw new IllegalStateException(
                                               "Unsupported output TDS type " + Integer.toHexString(pi.tdsType));
        }
    }

    /**
     * TDS 8 requires collation information for char data descriptors.
     *
     * @param out The Server request stream.
     * @param pi The parameter descriptor.
     * @throws IOException
     */
    static void putCollation(RequestStream out, ParamInfo pi)
    throws IOException {
        //
        // For TDS 8 write a collation string
        // I am assuming this can be all zero for now if none is known
        // TODO Set collation info?
        //
        if (types[pi.tdsType].isCollation) {
            if (pi.collation != null) {
                out.write(pi.collation);
            } else {
                byte collation[] = {0x00, 0x00, 0x00, 0x00, 0x00};

                out.write(collation);
            }
        }
    }

    /**
     * Write a parameter to the server request stream.
     *
     * @param out The Server request stream.
     * @param charset The encoding character set name.
     * @param isWideChar True if multi byte encoding.
     * @param collation The SQL Server 2000 collation
     * @param pi The parameter descriptor.
     * @throws IOException
     * @throws SQLException
     */
    static void writeParam(RequestStream out,
                           String charset,
                           boolean isWideChar,
                           byte[] collation,
                           ParamInfo pi)
    throws IOException, SQLException {
        int len;
        String tmp;
        byte[] buf;
        boolean isTds8 = out.getTdsVersion() >= TdsCore.TDS80;

        if (isTds8 && pi.collation == null) {
            pi.collation = collation;
        }

        switch (pi.tdsType) {

            case XSYBVARCHAR:
                if (pi.value == null) {
                    out.write((byte) pi.tdsType);
                    out.write((short) 8000);

                    if (isTds8) {
                        putCollation(out, pi);
                    }

                    out.write((short) 0xFFFF);
                } else {
                    buf = pi.getBytes(charset);

                    if (buf.length > 8000) {
                        out.write((byte) SYBTEXT);
                        out.write((int) buf.length);

                        if (isTds8) {
                            putCollation(out, pi);
                        }

                        out.write((int) buf.length);
                        out.write(buf);
                    } else {
                        out.write((byte) pi.tdsType);
                        out.write((short) 8000);

                        if (isTds8) {
                            putCollation(out, pi);
                        }

                        out.write((short) buf.length);
                        out.write(buf);
                    }
                }

                break;

            case SYBVARCHAR:
                if (pi.value == null) {
                    out.write((byte) pi.tdsType);
                    out.write((byte) 255);
                    out.write((byte) 0);
                } else {
                    buf = pi.getBytes(charset);

                    if (buf.length > 255) {
                        if (buf.length < 8001 && out.getTdsVersion() >= TdsCore.TDS70) {
                            out.write((byte) XSYBVARCHAR);
                            out.write((short) 8000);

                            if (isTds8) {
                                putCollation(out, pi);
                            }

                            out.write((short) buf.length);
                            out.write(buf);
                        } else {
                            out.write((byte) SYBTEXT);
                            out.write((int) buf.length);

                            if (isTds8) {
                                putCollation(out, pi);
                            }

                            out.write((int) buf.length);
                            out.write(buf);
                        }
                    } else {
                        if (buf.length == 0) {
                            buf = new byte[1];
                            buf[0] = ' ';
                        }

                        out.write((byte) pi.tdsType);
                        out.write((byte) 255);
                        out.write((byte) buf.length);
                        out.write(buf);
                    }
                }

                break;

            case XSYBNVARCHAR:
                out.write((byte) pi.tdsType);
                out.write((short) 8000);

                if (isTds8) {
                    putCollation(out, pi);
                }

                if (pi.value == null) {
                    out.write((short) 0xFFFF);
                } else {
                    tmp = pi.getString(charset);
                    out.write((short) (tmp.length() * 2));
                    out.write(tmp);
                }

                break;

            case SYBTEXT:
                if (pi.value == null) {
                    len = 0;
                } else {
                    len = pi.length;

                    if (len == 0 && out.getTdsVersion() < TdsCore.TDS70) {
                        pi.value = " ";
                        len = 1;
                    }
                }

                out.write((byte) pi.tdsType);

                if (len > 0) {
                    if (pi.value instanceof InputStream) {
                        // Write output directly from stream
                        out.write((int) len);

                        if (isTds8) {
                            putCollation(out, pi);
                        }

                        out.write((int) len);
                        out.writeStreamBytes((InputStream) pi.value, len);
                    } else if (pi.value instanceof Reader && !isWideChar) {
                        // Write output directly from stream with character translation
                        out.write((int) len);

                        if (isTds8) {
                            putCollation(out, pi);
                        }

                        out.write((int) len);
                        out.writeReaderBytes((Reader) pi.value, len);
                    } else {
                        buf = pi.getBytes(charset);
                        out.write((int) buf.length);

                        if (isTds8) {
                            putCollation(out, pi);
                        }

                        out.write((int) buf.length);
                        out.write(buf);
                    }
                } else {
                    out.write((int) len); // Zero length

                    if (isTds8) {
                        putCollation(out, pi);
                    }

                    out.write((int)len);
                }

                break;

            case SYBNTEXT:
                if (pi.value == null) {
                    len = 0;
                } else {
                    len = pi.length;
                }

                out.write((byte)pi.tdsType);

                if (len > 0) {
                    if (pi.value instanceof Reader) {
                        out.write((int) len);

                        if (isTds8) {
                            putCollation(out, pi);
                        }

                        out.write((int) len * 2);
                        out.writeReaderChars((Reader) pi.value, len);
                    } else if (pi.value instanceof InputStream && !isWideChar) {
                        out.write((int) len);

                        if (isTds8) {
                            putCollation(out, pi);
                        }

                        out.write((int) len * 2);
                        out.writeReaderChars(new InputStreamReader((InputStream) pi.value, charset), len);
                    } else {
                        tmp = pi.getString(charset);
                        len = tmp.length();
                        out.write((int) len);

                        if (isTds8) {
                            putCollation(out, pi);
                        }

                        out.write((int) len * 2);
                        out.write(tmp);
                    }
                } else {
                    out.write((int) len);

                    if (isTds8) {
                        putCollation(out, pi);
                    }

                    out.write((int) len);
                }

                break;

            case XSYBVARBINARY:
                out.write((byte) pi.tdsType);
                out.write((short) 8000);

                if (pi.value == null) {
                    out.write((short)0xFFFF);
                } else {
                    buf = pi.getBytes(charset);
                    out.write((short) buf.length);
                    out.write(buf);
                }

                break;

            case SYBVARBINARY:
                out.write((byte) pi.tdsType);
                out.write((byte) 255);

                if (pi.value == null) {
                    out.write((byte) 0);
                } else {
                    buf = pi.getBytes(charset);
                    out.write((byte) buf.length);
                    out.write(buf);
                }

                break;

            case SYBIMAGE:
                if (pi.value == null) {
                    len = 0;
                } else {
                    len = pi.length;
                }

                out.write((byte) pi.tdsType);

                if (len > 0) {
                    if (pi.value instanceof InputStream) {
                        out.write((int) len);
                        out.write((int) len);
                        out.writeStreamBytes((InputStream) pi.value, len);
                    } else {
                        buf = pi.getBytes(charset);
                        out.write((int) buf.length);
                        out.write((int) buf.length);
                        out.write(pi.getBytes(charset));
                    }
                } else {
                    out.write((int) len);
                    out.write((int) len);
                }

                break;

            case SYBINTN:
                out.write((byte) pi.tdsType);

                if (pi.value == null) {
                    out.write((byte) 4);
                    out.write((byte) 0);
                } else {
                    if (pi.sqlType.equals("bigint")) {
                        out.write((byte) 8);
                        out.write((byte) 8);
                        out.write((long) ((Number) pi.value).longValue());
                    } else {
                        out.write((byte) 4);
                        out.write((byte) 4);
                        out.write((int) ((Number) pi.value).intValue());
                    }
                }

                break;

            case SYBFLTN:
                out.write((byte) pi.tdsType);
                out.write((byte) 8);

                if (pi.value == null) {
                    out.write((byte) 0);
                } else {
                    out.write((byte) 8);
                    out.write(((Number) pi.value).doubleValue());
                }

                break;

            case SYBDATETIMN:
                out.write((byte) SYBDATETIMN);
                out.write((byte) 8);
                putDateTimeValue(out, pi.value);
                break;

            case SYBBIT:
                out.write((byte) pi.tdsType);

                if (pi.value == null) {
                    out.write((byte) 0);
                } else {
                    out.write((byte) (((Boolean) pi.value).booleanValue() ? 1 : 0));
                }

                break;

            case SYBBITN:
                out.write((byte) SYBBITN);
                out.write((byte) 1);

                if (pi.value == null) {
                    out.write((byte) 0);
                } else {
                    out.write((byte) 1);
                    out.write((byte) (((Boolean) pi.value).booleanValue() ? 1 : 0));
                }

                break;

            case SYBNUMERIC:
            case SYBDECIMAL:
                out.write((byte) pi.tdsType);
                BigDecimal value = null;
                int scale = (pi.jdbcType == java.sql.Types.BIGINT) ? 0 : DEFAULT_SCALE;

                if (pi.value != null) {
                    if (pi.value instanceof Long) {
                        value = new BigDecimal(((Long) pi.value).longValue());
                    } else {
                        value = (BigDecimal) pi.value;
                        scale = value.scale();
                    }
                }

                int prec = out.getMaxPrecision();
                int maxLen = (prec <= 28) ? 13 : 17;
                out.write((byte) maxLen);
                out.write((byte) prec);
                out.write((byte) scale);
                out.write(value);
                break;

            default:
                throw new IllegalStateException("Unsupported output TDS type "
                        + Integer.toHexString(pi.tdsType));
        }
    }
//
// ---------------------- Private methods from here -----------------------
//

    /**
     * Private constructor to prevent users creating an
     * actual instance of this class.
     */
    private TdsData() {
    }

    /**
     * Convert a Julian date from the Sybase epoch of 1900-01-01
     * to a Calendar object.
     * @param julianDate The Sybase days from 1900 value.
     * @param cal The Calendar object to populate.
     *
     * Algorithm  from Fliegel, H F and van Flandern, T C (1968).
     * Communications of the ACM, Vol 11, No 10 (October, 1968).
     * <pre>
     *           SUBROUTINE GDATE (JD, YEAR,MONTH,DAY)
     *     C
     *     C---COMPUTES THE GREGORIAN CALENDAR DATE (YEAR,MONTH,DAY)
     *     C   GIVEN THE JULIAN DATE (JD).
     *     C
     *           INTEGER JD,YEAR,MONTH,DAY,I,J,K
     *     C
     *           L= JD+68569
     *           N= 4*L/146097
     *           L= L-(146097*N+3)/4
     *           I= 4000*(L+1)/1461001
     *           L= L-1461*I/4+31
     *           J= 80*L/2447
     *           K= L-2447*J/80
     *           L= J/11
     *           J= J+2-12*L
     *           I= 100*(N-49)+I+L
     *     C
     *           YEAR= I
     *           MONTH= J
     *           DAY= K
     *     C
     *           RETURN
     *           END
     * </pre>
     */
    private static void sybaseToCalendar(int julianDate, GregorianCalendar cal) {
        int l = julianDate + 68569 + 2415021;
        int n = 4 * l / 146097;
        l = l - (146097 * n + 3) / 4;
        int i = 4000 * (l + 1) / 1461001;
        l = l - 1461 * i / 4 + 31;
        int j = 80 * l / 2447;
        int k = l - 2447 * j / 80;
        l = j / 11;
        j = j + 2 - 12 * l;
        i = 100 * (n - 49) + i + l;
        cal.set(Calendar.YEAR, i);
        cal.set(Calendar.MONTH, j-1);
        cal.set(Calendar.DAY_OF_MONTH, k);
    }

    private static GregorianCalendar cal = new GregorianCalendar();
    private static BigDecimal limit28 = new BigDecimal("999999999999999999");
    private static BigDecimal limit38 = new BigDecimal("9999999999999999999999999999");

    /**
     * Get a DATETIME value from the server response stream.
     *
     * @param in The server response stream.
     * @param type The TDS data type.
     * @return The java.sql.Timestamp value or null.
     * @throws java.io.IOException
     */
    private static Object getDatetimeValue(ResponseStream in, final int type)
    throws IOException, ProtocolException {
        int len;
        int daysSince1900;
        int time;
        int hours;
        int minutes;
        int seconds;

        synchronized (cal) {
            if (type == SYBDATETIMN) {
                len = in.read(); // No need to & with 0xff
            } else if (type == SYBDATETIME4) {
                len = 4;
            } else {
                len = 8;
            }

            switch (len) {
                case 0:
                    return null;

                case 8:
                    // A datetime is made of of two 32 bit integers
                    // The first one is the number of days since 1900
                    // The second integer is the number of seconds*300
                    // Negative days indicate dates earlier than 1900.
                    // The full range is 1753-01-01 to 9999-12-31.
                    daysSince1900 = in.readInt();
                    sybaseToCalendar(daysSince1900, cal);
                    time = in.readInt();
                    hours = time / 1080000;
                    cal.set(Calendar.HOUR_OF_DAY, hours);
                    time = time - hours * 1080000;
                    minutes = time / 18000;
                    cal.set(Calendar.MINUTE, minutes);
                    time = time - (minutes * 18000);
                    seconds = time / 300;
                    cal.set(Calendar.SECOND, seconds);
                    time = time - seconds * 300;                    
                    time = (int) Math.round(time * 1000 / 300f);
                    cal.set(Calendar.MILLISECOND, time);
//
//                  getTimeInMillis() is protected in java vm 1.3 :-(
//                  return new Timestamp(cal.getTimeInMillis());
//
                    return new Timestamp(cal.getTime().getTime());
                case 4:
                    // A smalldatetime is two 16 bit integers.
                    // The first is the number of days past January 1, 1900,
                    // the second smallint is the number of minutes past
                    // midnight.
                    // The full range is 1900-01-01 to 2079-06-06.
                    daysSince1900 = ((int) in.readShort()) & 0xFFFF;
                    sybaseToCalendar(daysSince1900, cal);
                    minutes = in.readShort();
                    hours = minutes / 60;
                    cal.set(Calendar.HOUR_OF_DAY, hours);
                    minutes = minutes - hours * 60;
                    cal.set(Calendar.MINUTE, minutes);
                    cal.set(Calendar.SECOND, 0);
                    cal.set(Calendar.MILLISECOND, 0);
//                    return new Timestamp(cal.getTimeInMillis());
                    return new Timestamp(cal.getTime().getTime());
                default:
                    throw new ProtocolException("Invalid DATETIME value with size of "
                                                + len + " bytes.");
            }
        }
    }

    /**
     * Convert a calendar date into days since 1900 (Sybase epoch).
     * <p>
     * Algorithm  from Fliegel, H F and van Flandern, T C (1968).
     * Communications of the ACM, Vol 11, No 10 (October, 1968).
     *
     * <pre>
     *           INTEGER FUNCTION JD (YEAR,MONTH,DAY)
     *     C
     *     C---COMPUTES THE JULIAN DATE (JD) GIVEN A GREGORIAN CALENDAR
     *     C   DATE (YEAR,MONTH,DAY).
     *     C
     *           INTEGER YEAR,MONTH,DAY,I,J,K
     *     C
     *           I= YEAR
     *           J= MONTH
     *           K= DAY
     *     C
     *           JD= K-32075+1461*(I+4800+(J-14)/12)/4+367*(J-2-(J-14)/12*12)
     *          2    /12-3*((I+4900+(J-14)/12)/100)/4
     *     C
     *           RETURN
     *           END
     * </pre>
     *
     * @param year The year eg 2003.
     * @param month The month 1-12.
     * @param day The day in month 1-31.
     * @return The julian date adjusted for Sybase epoch of 1900.
     */
    private static int calendarToSybase(int year, int month, int day) {
        int i = year;
        int j = month;
        int k = day;

        return day - 32075 + 1461 * (year + 4800 + (month - 14) / 12) /
        4 + 367 * (month - 2 - (month - 14) / 12 * 12) /
        12 - 3 * ((year + 4900 + (month -14) / 12) / 100) /
        4 - 2415021;
    }

    /**
     * Output a java.sql.Date/Time/Timestamp value to the server
     * as a Sybase datetime value.
     *
     * @param out The server request stream.
     * @param value The date value to write.
     * @throws IOException
     */
    private static void putDateTimeValue(RequestStream out, Object value)
    throws IOException {
        int daysSince1900;
        int time;

        if (value == null) {
            out.write((byte) 0);
            return;
        }

        synchronized (cal) {
            out.write((byte) 8);
            cal.setTime((java.util.Date) value);

            if (!Driver.JDBC3) {
                // Not Running under 1.4 so need to add milliseconds
                cal.set(Calendar.MILLISECOND,
                        ((java.sql.Timestamp)value).getNanos() / 1000000);
            }

            if (value instanceof java.sql.Time) {
                daysSince1900 = 0;
            } else {
                daysSince1900 = calendarToSybase(cal.get(Calendar.YEAR),
                                                 cal.get(Calendar.MONTH) + 1,
                                                 cal.get(Calendar.DAY_OF_MONTH));
            }

            time = cal.get(Calendar.HOUR_OF_DAY) * 1080000;
            time += cal.get(Calendar.MINUTE) * 18000;
            time += cal.get(Calendar.SECOND) * 300;

            if (value instanceof java.sql.Timestamp) {
                time += Math.round(cal.get(Calendar.MILLISECOND) * 300f / 1000);
            }

            out.write((int) daysSince1900);
            out.write((int) time);
        }
    }

    /**
     * Read a MONEY value from the server response stream.
     *
     * @param in The server response stream.
     * @param type The TDS data type.
     * @return The java.math.BigDecimal value or null.
     * @throws IOException
     * @throws ProtocolException
     */
    private static Object getMoneyValue(ResponseStream in, final int type)
    throws IOException, ProtocolException {
        final int len;

        if (type == SYBMONEY) {
            len = 8;
        } else if (type == SYBMONEYN) {
            len = in.read();
        } else {
            len = 4;
        }

        BigInteger x = null;

        if (len == 4) {
            x = BigInteger.valueOf(in.readInt());
        } else if (len == 8) {
            final byte b4 = (byte) in.read();
            final byte b5 = (byte) in.read();
            final byte b6 = (byte) in.read();
            final byte b7 = (byte) in.read();
            final byte b0 = (byte) in.read();
            final byte b1 = (byte) in.read();
            final byte b2 = (byte) in.read();
            final byte b3 = (byte) in.read();
            final long l = (long) (b0 & 0xff) + ((long) (b1 & 0xff) << 8)
                           + ((long) (b2 & 0xff) << 16) + ((long) (b3 & 0xff) << 24)
                           + ((long) (b4 & 0xff) << 32) + ((long) (b5 & 0xff) << 40)
                           + ((long) (b6 & 0xff) << 48) + ((long) (b7 & 0xff) << 56);

            x = BigInteger.valueOf(l);
        } else if (len != 0) {
            throw new ProtocolException("Invalid money value.");
        }

        return (x == null) ? null : new BigDecimal(x, 4);
    }

    /**
     * Read a MSQL 2000 sql_variant data value from the input stream.
     * <p>sql_variant has the following structure:
     * <ol>
     * <li>int4 total size of data.
     * <li>int1 TDS data type (text/image/ntext/sql_variant not allowed).
     * <li>int1 Length of extra type descriptor information.
     * <li>Optional additional type info required by some types.
     * <li>byte[0...n] the actual data.
     * </ol>
     *
     * @param in The server response stream.
     * @return The sql_variant data.
     * @throws IOException
     * @throws ProtocolException
     */
    private static Object getVariant(ResponseStream in)
    throws IOException, ProtocolException {
        byte[] bytes;
        int len = in.readInt();

        if (len == 0) {
            // Length of zero means item is null
            return null;
        }

        ColInfo ci = new ColInfo();
        len -= 2;
        ci.tdsType = in.read(); // TDS Type
        len = len - in.read(); // Size of descriptor

        switch (ci.tdsType) {
            case SYBINT1:
                return new Byte((byte) in.read());

            case SYBINT2:
                return new Short((short) in.readShort());

            case SYBINT4:
                return new Integer(in.readInt());

            case SYBINT8:
                return new Long(in.readLong());

            case XSYBCHAR:
            case XSYBVARCHAR:
                in.skip(7); // Skip collation and buffer size

                return in.readAsciiString(len);

            case XSYBNCHAR:
            case XSYBNVARCHAR:
                in.skip(7); // Skip collation and buffer size

                return in.readString(len / 2);

            case XSYBVARBINARY:
            case XSYBBINARY:
                in.skip(2); // Skip buffer size
                bytes = new byte[len];
                in.read(bytes);

                return bytes;

            case SYBMONEY4:
            case SYBMONEY:
                return getMoneyValue(in, ci.tdsType);

            case SYBDATETIME4:
            case SYBDATETIME:
                return getDatetimeValue(in, ci.tdsType);

            case SYBBIT:
                return (in.read() != 0) ? Boolean.TRUE : Boolean.FALSE;

            case SYBREAL:
                return new Float(Float.intBitsToFloat(in.readInt()));

            case SYBFLT8:
                return new Double(Double.longBitsToDouble(in.readLong()));

            case SYBUNIQUE:
                bytes = new byte[len];
                in.read(bytes);
                StringBuffer buf = new StringBuffer(36);

                for (int i = 0; i < bytes.length; i++) {
                    buf.append("0123456789ABCDEF".substring((bytes[i] >> 4) & 0x0F, 1));
                    buf.append("0123456789ABCDEF".substring(bytes[i] & 0x0F, 1));

                    if (i == 4 || i == 7 || i == 10 || i == 13) {
                        buf.append('-');
                    }
                }

                return buf.toString();

            case SYBNUMERIC:
            case SYBDECIMAL:
                ci.precision = in.read();
                ci.scale = in.read();
                int sign = in.read();
                len--;
                bytes = new byte[len];
                BigInteger bi;

                while (len-- > 0) {
                    bytes[len] = (byte)in.read();
                }

                bi = new BigInteger((sign == 0) ? -1 : 1, bytes);

                return new BigDecimal(bi, ci.scale);

            default:
                throw new ProtocolException("Unsupported TDS data type 0x"
                		                    + Integer.toHexString(ci.tdsType)
											+ " in sql_variant");
        }
        //
        // For compatibility with the MS driver convert to String.
        // Change the data type for sql_variant from OTHER to VARCHAR
        // Without this code the actual Object type can be retrieved
        // by using getObject(n).
        //
//        try {
//            value = Support.convert(value, java.sql.Types.VARCHAR, in.getCharset());
//        } catch (SQLException e) {
//            // Conversion failed just try toString();
//            value = value.toString();
//        }
    }
}
