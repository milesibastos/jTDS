// jTDS JDBC Driver for Microsoft SQL Server and Sybase
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

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;

/**
 * This class implements an InputStream populated with data returned by the
 * READTEXT command for image, text and ntext columns.
 *
 * @author Mike Hutchinson
 * @version $Id: JtdsInputStream.java,v 1.7 2004-08-31 17:25:17 alin_sinpalean Exp $
 */
public class JtdsInputStream extends InputStream {
    private TdsCore tds;
    private int length;
    private int offset = 0;
    private String colName;
    private String tabName;
    private TextPtr textPtr;
    private static final int BUFSIZE = 4096;
    private byte[] buffer = null;
    private int pos = 0;
    private String charset;

   /**
    * Construct a new InputStream.
    *
    * @param con The parent connection object.
    * @param ci The descriptor for the text or image column.
    * @param textPtr The textpointer.
    * @param charset The character set for converting strings to bytes.
    * @throws SQLException
    */
    JtdsInputStream(ConnectionJDBC2 con, ColInfo ci, TextPtr textPtr, String charset)
        throws SQLException {
        this.tds = new TdsCore(con, new SQLDiagnostic(con.getServerType()));
        this.colName = ci.realName;
        this.tabName = ci.tableName;
        this.textPtr = textPtr;
        this.charset = charset;
        this.length = tds.dataLength(tabName, colName);

        if (ci.sqlType.equalsIgnoreCase("ntext")) {
            this.length = this.length / 2;
        }

        fillBuffer();
    }

    public int available() throws IOException {
        return length - offset;
    }

    public int getLength() {
        return length;
    }

    /**
     * Resets the stream so that the data may be read from the specified offset.
     *
     * @throws IOException if <code>offset</code> is less <code>0</code>;
     *                     if <code>offset</code> is greater than <code>getLength()</code>;
     */
    public void reset() throws IOException {
        offset = 0;
        pos = 0;
        buffer = null;

        try {
            fillBuffer();
        } catch (SQLException e) {
            throw new IOException("SQL Error: " + e.getMessage());
        }
    }

    /**
     * Invoke READTEXT to obtain the next block of data from the server.
     *
     * @throws SQLException
     */
    void fillBuffer() throws SQLException {
        int bc = BUFSIZE;
        pos = 0;

        if (offset + bc > length) {
            bc = length - offset;

            if (bc == 0) {
                buffer = new byte[0];

                return; // At end of file.
            }
        }

        Object result = tds.readText(tabName,
                                     colName,
                                     textPtr,
                                     offset,
                                     bc);

        if (result instanceof byte[]) {
            buffer = (byte[]) result;
            offset += buffer.length;
        } else if (result instanceof char[]) {
           char[] tmp = (char[]) result;

           if (charset.equals("UTF-16BE")) {
               // Unicode stream
               buffer = new byte[tmp.length * 2];
               int ptr = 0;

               for (int i = 0; i < tmp.length; i++) {
                   buffer[ptr++] = (byte) (tmp[i] >> 8);
                   buffer[ptr++] = (byte) tmp[i];
               }
           } else {
               buffer = Support.encodeString(charset, new String(tmp));
           }

           offset += ((char[]) result).length;
        }
    }

    public int read() throws IOException {
        if (pos == buffer.length) {
            if (tds == null) {
                throw new IOException("InputStream is closed");
            }

            try {
                fillBuffer();
            } catch (SQLException e) {
                throw new IOException("SQL Error: " + e.getMessage());
            }

            if (buffer.length == 0) {
                return -1; // EOF
            }
        }

        return buffer[pos++] & 0xFF;
    }

    public void close() throws IOException {
        try {
            tds.close();
        } catch (SQLException e) {
            // Ignore
        } finally {
            tds = null;
            buffer = new byte[0];
        }
    }
}
