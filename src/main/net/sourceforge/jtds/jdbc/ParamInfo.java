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

import java.sql.SQLException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * This class is a descriptor for procedure and prepared statement parameters.
 *
 * @author Mike Hutchinson
 * @version $Id: ParamInfo.java,v 1.1 2004-06-27 17:00:52 bheineman Exp $
 */
class ParamInfo {
    /** Internal TDS data type */
    int tdsType = 0;
    /** JDBC type constant from java.sql.Types */
    int jdbcType = 0;
    /** Formal parameter name eg @P1 */
    public String name = null;
    /** SQL type name eg varchar(10) */
    String sqlType = null;
    /** Parameter offset in target SQL statement */
    public int markerPos = -1;
    /** Current parameter value */
    Object value = null;
    /** Parameter buffer size (max length) */
    int bufferSize= 0;
    /** Parameter decimal precision */
    int precision = -1;
    /** Parameter decimal scale */
    int scale = -1;
    /** Length of InputStream */
    int length = -1;
    /** Parameter is an output parameter */
    boolean isOutput = false;
    /** Parameter is used as  SP return value */
    boolean isRetVal = false;
    /** Parameter has been set */
    boolean isSet = false;
    /** Parameter should be sent as unicode */
    boolean isUnicode = true;
    /** TDS 8 Collation string. */
    byte collation[] = null;

    /**
     * Default constructor
     */
    ParamInfo() {
    }

    /**
     * Copy constructor used to create a ParamInfo item initialised
     * to the same values as the supplied parameter.
     *
     * @param pi The ParamInfo object to copy.
     */
    ParamInfo(ParamInfo pi) {
        this.bufferSize = pi.bufferSize;
        this.collation = pi.collation;
        this.isOutput = pi.isOutput;
        this.isRetVal = pi.isRetVal;
        this.isSet = pi.isSet;
        this.isUnicode = pi.isUnicode;
        this.jdbcType = pi.jdbcType;
        this.length = pi.length;
        this.markerPos = pi.markerPos;
        this.name = pi.name;
        this.precision = pi.precision;
        this.scale = pi.scale;
        this.sqlType = pi.sqlType;
        this.tdsType = pi.tdsType;
        this.value = pi.value;
    }

    /**
     * Construct a parameter with parameter marker offset.
     *
     * @param pos The offset of the ? symbol in the target SQL string.
     */
    ParamInfo(int pos) {
        markerPos = pos;
    }

    /**
     * Get the string value of the parameter.
     *
     * @return The data value as a <code>String</code> or null.
     * @throws SQLException
     */
    String getString(String charset) throws SQLException, IOException {
        if (value instanceof String) {
            return (String) value;
        }

        if (value instanceof InputStream) {
            try {
                value = loadFromReader(new InputStreamReader((InputStream) value, charset), length);
                length = ((String) value).length();

                return (String) value;
            } catch (UnsupportedEncodingException e) {
                throw new IOException("I/O Error: UnsupportedEncodingException: "+ e.getMessage());
            }
        }

        if (value instanceof Reader) {
            value = loadFromReader((Reader)value, length);
            return (String)value;
        }

        return value.toString();
    }

    /**
     * Get the byte array value of the parameter.
     *
     * @return The data value as a <code>byte[]</code> or null.
     * @throws SQLException
     */
    byte[] getBytes(String charset) throws SQLException, IOException {
        if (value instanceof byte[]) {
            return (byte[])value;
        }

        if (value instanceof InputStream) {
            value = loadFromStream((InputStream) value, length);

            return (byte[]) value;
        }

        if (value instanceof Reader) {
            value = loadFromReader((Reader) value, length);
        }

        if (value instanceof String) {
            return Support.encodeString(charset, (String) value);
        }

        return new byte[0];
    }

    /**
     * Load a byte array from an InputStream
     *
     * @param in The InputStream to read from.
     * @param length The length of the stream.
     * @return The data as a <code>byte[]</code>.
     * @throws IOException
     */
    private static byte[] loadFromStream(InputStream in, int length)
        throws IOException {
        byte[] buf = new byte[length];

        if (in.read(buf) != length) {
            throw new java.io.IOException(
                "Data in stream less than specified by length");
        }

        if (in.read() >= 0) {
            throw new java.io.IOException(
                    "More data in stream than specified by length");
        }

        return buf;
    }

    /**
     * Create a String from a Reader stream.
     *
     * @param in The Reader object with the data.
     * @param length Number of characters to read.
     * @return The data as a <code>String</code>.
     * @throws IOException
     */
    private static String loadFromReader(Reader in, int length)
        throws IOException {
        char[] buf = new char[length];

        if (in.read(buf) != length) {
            throw new java.io.IOException(
                "Data in stream less than specified by length");
        }

        if (in.read() >= 0) {
            throw new java.io.IOException(
                    "More data in stream than specified by length");
        }

        return new String(buf);
    }
}
