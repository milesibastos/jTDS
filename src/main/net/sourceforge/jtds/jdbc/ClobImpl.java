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
 */
public class ClobImpl implements Clob {
    public static final String cvsVersion = "$Id: ClobImpl.java,v 1.8 2004-05-02 22:45:20 bheineman Exp $";

    private String _clob;

    /**
     * Constructs a new Clob instance.
     */
    ClobImpl(String clob) throws SQLException {
        if (clob == null) {
            throw new SQLException("clob cannot be null.");
        }

        _clob = clob;
    }

    /**
     * Returns a new ascii stream for the CLOB data.
     */
    public synchronized InputStream getAsciiStream() throws SQLException {
        try {
            return new ByteArrayInputStream(_clob.getBytes("ASCII"));
        } catch (UnsupportedEncodingException e) {
            // This should never happen...
            throw TdsUtil.getSQLException("Unexpected encoding exception", null, e);
        }
    }

    /**
     * Returns a new reader for the CLOB data.
     */
    public synchronized Reader getCharacterStream() throws SQLException {
        return new StringReader(_clob);
    }

    public synchronized String getSubString(long pos, int length) throws SQLException {
        if (pos < 1) {
            throw new SQLException("pos must be >= 1.");
        } else if (pos > Integer.MAX_VALUE) {
            throw new SQLException("pos must be <= " + Integer.MAX_VALUE);
        }

        try {
            return _clob.substring((int) --pos, length);
        } catch (Exception e) {
            throw TdsUtil.getSQLException(null, null, e);
        }
    }

    /**
     * Returns the length of the value.
     */
    public synchronized long length() throws SQLException {
        return _clob.length();
    }

    public synchronized long position(Clob searchStr, long start) throws SQLException {
        if (searchStr == null) {
            throw new SQLException("searchStr cannot be null.");
        } else if (start > Integer.MAX_VALUE) {
            throw new SQLException("start must be <= " + Integer.MAX_VALUE);
        }

        long length = searchStr.length();

        if (length > Integer.MAX_VALUE) {
            throw new SQLException("searchStr.length() must be <= " + Integer.MAX_VALUE);
        }

        return _clob.indexOf(searchStr.getSubString(0, (int) length),
                             (int) --start);
    }

    public synchronized long position(String searchStr, long start) throws SQLException {
        if (searchStr == null) {
            throw new SQLException("searchStr cannot be null.");
        } else if (start < 1) {
            throw new SQLException("start must be >= 1.");
        } else if (start > Integer.MAX_VALUE) {
            throw new SQLException("start must be <= " + Integer.MAX_VALUE);
        }

        return _clob.indexOf(searchStr, (int) --start);
    }

    public synchronized OutputStream setAsciiStream(final long pos) throws SQLException {
        if (pos < 1) {
            throw new SQLException("pos must be >= 1.");
        } else if (pos > _clob.length()) {
            throw new SQLException("pos specified is past length of value.");
        } else if (pos >= Integer.MAX_VALUE) {
            throw new SQLException("pos must be < " + Integer.MAX_VALUE);
        }

        final byte[] clob;

        try {
            clob = _clob.getBytes("ASCII");
        } catch (UnsupportedEncodingException e) {
            // This should never happen...
            throw TdsUtil.getSQLException("Unexpected encoding exception", null, e);
        }

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
            throw new SQLException("pos must be >= 1.");
        } else if (pos > _clob.length()) {
            throw new SQLException("pos specified is past length of value.");
        } else if (pos >= Integer.MAX_VALUE) {
            throw new SQLException("pos must be < " + Integer.MAX_VALUE);
        }

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
            throw new SQLException("str cannot be null.");
        }

        return setString(pos, str, 0, str.length());
    }

    public synchronized int setString(long pos, String str, int offset, int len)
    throws SQLException {
        Writer writer = setCharacterStream(pos);

        try {
            writer.write(str, offset, len);
            writer.close();
        } catch (IOException e) {
            throw TdsUtil.getSQLException("Unable to write value", null, e);
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
            throw new SQLException("len must be >= 0.");
        } else if (len > _clob.length()) {
            throw new SQLException("length specified is more than length of value.");
        }

        _clob = _clob.substring(0, (int) len);
    }

    /**
     * Returns the string representation of this object.
     */
    public synchronized String toString() {
        return _clob;
    }
}
