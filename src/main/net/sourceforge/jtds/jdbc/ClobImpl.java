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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.sql.Clob;
import java.sql.SQLException;

/**
 * An in-memory representation of character data.
 * <p>
 * Implementation note:
 * <ol>
 * <li> Mostly Brian's original code but modified to include the
 *      ability to convert a stream into a String when required.
 * <li> SQLException messages loaded from properties file.
 * </ol>
 *
 * @author Brian Heineman
 * @author Mike Hutchinson
 * @version $Id: ClobImpl.java,v 1.9 2004-06-27 17:00:50 bheineman Exp $
 */
public class ClobImpl implements Clob {
    private String _clob;
    private Reader stream;
    private int length;

    /**
     * Constructs a new Clob instance.
     */
    ClobImpl(String clob)  {
        if (clob == null) {
            throw new IllegalArgumentException("clob String cannot be null.");
        }

        _clob = clob;
    }

    /**
     * Constructs a new Clob instance from a stream.
     *
     * @param stream The input stream to initialize the Clob.
     */
    ClobImpl(Reader stream) {
        this.stream = stream;
        this.length = ((JtdsReader) stream).available();
    }

    /**
     * Returns a new ascii stream for the CLOB data.
     */
    public synchronized InputStream getAsciiStream() throws SQLException {
        try {
            loadClob();
            return new ByteArrayInputStream(_clob.getBytes("ASCII"));
        } catch (UnsupportedEncodingException e) {
            // This should never happen...
            throw new SQLException(
                Support.getMessage("error.generic.encoding", e.getMessage()), "HY000");
        }
    }

    /**
     * Returns a new reader for the CLOB data.
     */
    public synchronized Reader getCharacterStream() throws SQLException {
        if (stream != null) {
            return stream;
        }

        return new StringReader(_clob);
    }

    public synchronized String getSubString(long pos, int length) throws SQLException {
        if (pos < 1) {
            throw new SQLException(Support.getMessage("error.blobclob.badpos"), "HY090");
        } else if (pos > Integer.MAX_VALUE) {
            throw new SQLException(Support.getMessage("error.blobclob.postoolong"), "HY090");
        }

        int off = (int) pos - 1;

        loadClob();

        if (off + length > _clob.length()) {
            throw new SQLException(Support.getMessage("error.blobclob.lentoolong"), "HY090");
        }

        try {
            return _clob.substring((int) off, off + length);
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }
    }

    /**
     * Returns the length of the value.
     */
    public synchronized long length() throws SQLException {
        if (stream != null) {
            return length;
        }

        return _clob.length();
    }

    public synchronized long position(Clob searchStr, long start) throws SQLException {
        if (searchStr == null) {
            throw new SQLException(Support.getMessage("error.clob.searchnull"), "HY024");
        } else if (start > Integer.MAX_VALUE) {
            throw new SQLException(Support.getMessage("error.blobclob.postoolong"), "HY090");
        }

        loadClob();

        long length = searchStr.length();

        if (length > Integer.MAX_VALUE) {
            throw new SQLException(Support.getMessage("error.clob.searchtoolong"), "HY090");
        }

        return _clob.indexOf(searchStr.getSubString(0, (int) length),
                             (int) --start);
    }

    public synchronized long position(String searchStr, long start) throws SQLException {
        if (searchStr == null) {
            throw new SQLException(Support.getMessage("error.clob.searchnull"), "HY024");
        } else if (start < 1) {
            throw new SQLException(Support.getMessage("error.blobclob.badpos"), "HY024");
        } else if (start > Integer.MAX_VALUE) {
            throw new SQLException(Support.getMessage("error.blobclob.postoolong"), "HY024");
        }

        return _clob.indexOf(searchStr, (int) --start);
    }

    public synchronized OutputStream setAsciiStream(final long pos) throws SQLException {
        if (pos < 1) {
            throw new SQLException(Support.getMessage("error.blobclob.badpos"), "HY024");
        } else if (pos > _clob.length()) {
            throw new SQLException(Support.getMessage("error.blobclob.badposlen"), "HY024");
        } else if (pos >= Integer.MAX_VALUE) {
            throw new SQLException(Support.getMessage("error.blobclob.postoolong"), "HY024");
        }

        final byte[] clob;

        try {
            clob = _clob.getBytes("ASCII");
        } catch (UnsupportedEncodingException e) {
            // This should never happen...
            throw new SQLException(
                Support.getMessage("error.generic.encoding", e.getMessage()), "HY000");
        }

        stream = null;

        return new ByteArrayOutputStream() {
            {
                write(clob, 0, (int) pos - 1);
            }

            public void flush() throws IOException {
                synchronized (ClobImpl.this) {
                    String clob = new String(toByteArray(), "ASCII");

                    if (clob.length() < _clob.length()) {
                        _clob = clob + _clob.substring(clob.length());
                    } else {
                        _clob = clob;
                    }
                }
            }

            public void close() throws IOException {
                flush();
            }
        };
    }

    public synchronized Writer setCharacterStream(final long pos) throws SQLException {
        if (pos < 1) {
            throw new SQLException(Support.getMessage("error.blobclob.badpos"), "HY024");
        } else if (pos > _clob.length()) {
            throw new SQLException(Support.getMessage("error.blobclob.badposlen"), "HY024");
        } else if (pos >= Integer.MAX_VALUE) {
            throw new SQLException(Support.getMessage("error.blobclob.postoolong"), "HY024");
        }

        stream = null;

        return new StringWriter() {
            {write(_clob, 0, (int) pos - 1);}

            public void flush() {
                synchronized (ClobImpl.this) {
                    String clob = this.toString();

                    if (clob.length() < _clob.length()) {
                        _clob = clob + _clob.substring(clob.length());
                    } else {
                        _clob = clob;
                    }
                }
            }

            public void close() throws IOException {
                flush();
            }
        };
    }

    public synchronized int setString(long pos, String str) throws SQLException {
        if (str == null) {
            throw new SQLException(Support.getMessage("error.clob.strnull"), "HY090");
        }

        loadClob();

        return setString(pos, str, 0, str.length());
    }

    public synchronized int setString(long pos, String str, int offset, int len)
    throws SQLException {
        Writer writer = setCharacterStream(pos);

        try {
            writer.write(str, offset, len);
            writer.close();
        } catch (IOException e) {
            throw new SQLException(
                Support.getMessage("error.generic.iowrite", "String", e.getMessage()),
                            "HY000");
        }

        return len;
    }

    /**
     * Truncates the value to the length specified.
     *
     * @param len the length to truncate the value to
     */
    public synchronized void truncate(long len) throws SQLException {
        if (len < 0) {
            throw new SQLException(Support.getMessage("error.blobclob.badlen"), "HY090");
        } else if (len > _clob.length()) {
            throw new SQLException(Support.getMessage("error.blobclob.lentoolong"), "HY090");
        }

        loadClob();

        _clob = _clob.substring(0, (int) len);
    }

    private void loadClob() throws SQLException {
        if (stream == null || length < 0) {
            return;
        }

        try {
            char[] buf = new char[length];
            if (stream.read(buf) != length)
                throw new SQLException(Support.getMessage("error.clob.readlen"), "HY000");
            _clob = new String(buf);
        } catch (IOException ioe) {
           throw new SQLException(
                Support.getMessage("error.generic.ioread", "String", ioe.getMessage()),
                                        "HY000");
        }
    }

    /**
     * Returns the string representation of this object.
     */
    public synchronized String toString() {
        try {
            loadClob();
        } catch (SQLException e) {
            _clob = "";
        }

        return _clob;
    }
}
