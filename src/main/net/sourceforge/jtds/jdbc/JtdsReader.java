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
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;

/**
 * This class implements a Reader populated with data returned by the
 * READTEXT command for text and ntext columns.
 *
 * @author Mike Hutchinson.
 * @version $Id: JtdsReader.java,v 1.6 2004-08-24 21:47:39 bheineman Exp $
 */
public class JtdsReader extends Reader {
    private TdsCore tds;
    private int length;
    private int offset = 0;
    private String colName;
    private String tabName;
    private TextPtr textPtr;
    private static final int BUFSIZE = 4096;
    private char[] buffer = null;
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
    JtdsReader(ConnectionJDBC2 con, ColInfo ci, TextPtr textPtr, String charset)
        throws SQLException {
        this.tds = new TdsCore(con, new SQLDiagnostic(con.getServerType()));
        this.colName = ci.realName;
        this.tabName = ci.tableName;
        this.textPtr = textPtr;
        this.length = tds.dataLength(tabName, colName);

        if (ci.sqlType.equalsIgnoreCase("ntext")) {
            this.length = this.length / 2; // Get length in chars not bytes
        }

        this.charset = charset;
        fillBuffer();
    }

    int available() {
        return length - offset;
    }
    
    int getLength() {
        return length;
    }

    /**
     * Resets the stream so that the data may be read from the begining.
     */
    public void reset() throws IOException {
        offset = 0;
        pos = 0;
        buffer = null;
        
        try {
            fillBuffer();
        } catch (SQLException e) {
            throw new IOException("SQL Error: "+ e.getMessage());
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
                buffer = new char[0];

                return; // At end of file.
            }
        }

        Object result = tds.readText(tabName,
                                     colName,
                                     textPtr,
                                     offset,
                                     bc);

        if (result instanceof byte[]) {
           try {
                buffer = new String((byte[]) result, charset).toCharArray();
           } catch (UnsupportedEncodingException e) {
                buffer = new String((byte[]) result).toCharArray();
           }

           offset += ((byte[]) result).length;
        } else if (result instanceof char[]) {
            buffer = (char[]) result;
            offset += buffer.length;
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

        return buffer[pos++];
    }

    public void close() throws IOException {
        try {
            tds.close();
        } catch (SQLException e) {
            // Ignore
        } finally {
            tds = null;
            buffer = new char[0];
        }
    }

    public int read(char[] buf, int off, int len) throws IOException {
        if (buf == null) {
            throw new NullPointerException();
        }

        if (off < 0 || len < 0 || off + len > buf.length) {
            throw new IndexOutOfBoundsException();
        }

        if (len == 0) {
            return 0;
        }

        int b = read();

        if (b < 0) {
            return -1;
        }

        buf[off] = (char) b;

        int i = 1;

        for ( ; i < len; i++) {
            b = read();

            if (b >= 0) {
                buf[off + i] = (char) b;
            } else {
                return i;
            }
        }

        return i;
    }
}
