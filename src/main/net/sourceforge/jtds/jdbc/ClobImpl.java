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
    public static final String cvsVersion = "$Id: ClobImpl.java,v 1.2 2004-01-28 22:01:25 bheineman Exp $";

    private String _clob;

    /**
     * Constructs a new Clob instance.
     */
    ClobImpl(String clob) {
        if (clob == null) {
            throw new IllegalArgumentException("clob cannot be null.");
        }

        _clob = clob;
    }

    /**
     * Returns a new ascii stream for the CLOB data.
     */
    public InputStream getAsciiStream() throws SQLException {
        try {
            return new ByteArrayInputStream(_clob.getBytes("ASCII"));
        } catch (UnsupportedEncodingException e) {
            // This should never happen...
            throw new SQLException("Unexpected encoding exception: "
                                   + e.getMessage());
        }
    }

    /**
     * Returns a new reader for the CLOB data.
     */
    public Reader getCharacterStream() throws SQLException {
        return new StringReader(_clob);
    }

    public String getSubString(long pos, int length) throws SQLException {
        if (pos > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("pos must be <= " + Integer.MAX_VALUE);
        }

        return _clob.substring((int) pos, length);
    }

    /**
     * Returns the length of the value.
     */
    public long length() throws SQLException {
        return _clob.length();
    }

    public long position(Clob searchStr, long start) throws SQLException {
        if (searchStr == null) {
            throw new IllegalArgumentException("searchStr cannot be null.");
        } else if (start > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("start must be <= " + Integer.MAX_VALUE);
        }

        long length = searchStr.length();

        if (length > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("searchStr.length() must be <= "
                                               + Integer.MAX_VALUE);
        }

        return _clob.indexOf(searchStr.getSubString(0, (int) length),
                             (int) start);
    }

    public long position(String searchStr, long start) throws SQLException {
        if (searchStr == null) {
            throw new IllegalArgumentException("searchStr cannot be null.");
        } else if (start < 0) {
            throw new IllegalArgumentException("start must be >= 0.");
        } else if (start > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("start must be <= " + Integer.MAX_VALUE);
        }

        return _clob.indexOf(searchStr, (int) start);
    }

    public OutputStream setAsciiStream(final long pos) throws SQLException {
        if (pos < 0) {
            throw new IllegalArgumentException("pos must be >= 0.");
        } else if (pos > _clob.length()) {
            throw new IllegalArgumentException("pos specified is past length of value.");
        } else if (pos >= Integer.MAX_VALUE) {
            throw new IllegalArgumentException("pos must be < " + Integer.MAX_VALUE);
        }

        return new ByteArrayOutputStream() {
                    {
                        try {
                            byte[] clob = _clob.getBytes("ASCII");
                            write(clob, 0, (int) pos);
                        } catch (UnsupportedEncodingException e) {
                            // This should never happen...
                            throw new SQLException("Unexpected encoding exception: "
                                                   + e.getMessage());
                        }
                    }

                    public void flush() throws IOException {
                        String clob = new String(toByteArray(), "ASCII");

                        if (clob.length() < _clob.length()) {
                            _clob = clob + _clob.substring(clob.length());
                        } else {
                            _clob = clob;
                        }
                    }

                    public void close() throws IOException {
                        flush();
                    }
                };
    }

    public Writer setCharacterStream(final long pos) throws SQLException {
        if (pos < 0) {
            throw new IllegalArgumentException("pos must be >= 0.");
        } else if (pos > _clob.length()) {
            throw new IllegalArgumentException("pos specified is past length of value.");
        } else if (pos >= Integer.MAX_VALUE) {
            throw new IllegalArgumentException("pos must be < " + Integer.MAX_VALUE);
        }

        return new StringWriter() {
                    {write(_clob, 0, (int) pos);}

                    public void flush() {
                        String clob = toString();

                        if (clob.length() < _clob.length()) {
                            _clob = clob + _clob.substring(clob.length());
                        } else {
                            _clob = clob;
                        }
                    }

                    public void close() throws IOException {
                        flush();
                    }
                };
    }

    public int setString(long pos, String str) throws SQLException {
        if (str == null) {
            throw new IllegalArgumentException("str cannot be null.");
        }

        return setString(pos, str, 0, str.length());
    }

    public int setString(long pos, String str, int offset, int len) throws SQLException {
        Writer writer = setCharacterStream(pos);

        try {
            writer.write(str, offset, len);
            writer.close();
        } catch (IOException e) {
            throw new SQLException("Unable to write value: " + e.getMessage());
        }

        return len;
    }

    /**
     * Truncates the value to the length specified.
     * 
     * @param len the length to truncate the value to
     */
    public void truncate(long len) throws SQLException {
        if (len < 0) {
            throw new IllegalArgumentException("len must be >= 0.");
        } else if (len > _clob.length()) {
            throw new IllegalArgumentException("length specified is more than length of "
                                               + "value.");
        }

        _clob = _clob.substring(0, (int) len);
    }

    /**
     * Returns the string representation of this object.
     */
    public String toString() {
        return _clob;
    }
}
