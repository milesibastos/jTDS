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
import java.sql.Blob;
import java.sql.SQLException;

/**
 * An in-memory representation of binary data.
 */
public class BlobImpl implements Blob {
    public static final String cvsVersion = "$Id: BlobImpl.java,v 1.8 2004-05-02 22:45:20 bheineman Exp $";

    private byte[] _blob;

    /**
     * Constructs a new Blob instance.
     */
    BlobImpl(byte[] blob) throws SQLException {
        if (blob == null) {
            throw new SQLException("blob cannot be null.");
        }

        _blob = blob;
    }

    /**
     * Returns a stream for the BLOB data.
     */
    public synchronized InputStream getBinaryStream() throws SQLException {
        return new ByteArrayInputStream(_blob);
    }

    public synchronized byte[] getBytes(long pos, int length) throws SQLException {
        if (pos < 1) {
            throw new SQLException("pos must be >= 1.");
        } else if (pos >= _blob.length - 1) {
            throw new SQLException("pos must be < length of blob.");
        } else if (pos > Integer.MAX_VALUE) {
            throw new SQLException("pos must be <= " + Integer.MAX_VALUE);
        } else if (length < 0) {
            throw new SQLException("length must be >= 0.");
        } else if (--pos + length > _blob.length) {
            throw new SQLException("more bytes requested than exist in blob.");
        }

        byte[] value = new byte[length];

        System.arraycopy(_blob, (int) pos, value, 0, length);

        return value;
    }

    /**
     * Returns the length of the value.
     */
    public synchronized long length() throws SQLException {
        return _blob.length;
    }

    public synchronized long position(Blob pattern, long start) throws SQLException {
        if (pattern == null) {
            throw new SQLException("pattern cannot be null.");
        }

        long length = pattern.length();

        if (length > Integer.MAX_VALUE) {
            throw new SQLException("pattern.length() must be <= "
                                               + Integer.MAX_VALUE);
        }

        return position(pattern.getBytes(0, (int) length), start);
    }

    public synchronized long position(byte[] pattern, long start) throws SQLException {
        int length = _blob.length - pattern.length;

        if (pattern == null) {
            throw new SQLException("pattern cannot be null.");
        } else if (start < 1) {
            throw new SQLException("start must be >= 1.");
        } else if (start >= Integer.MAX_VALUE) {
            throw new SQLException("start must be < " + Integer.MAX_VALUE);
        }

        for (int i = (int) --start; i < length; i++) {
            boolean found = true;

            for (int x = 0; x < pattern.length; x++) {
                if (pattern[x] != _blob[i + x]) {
                    found = false;
                    break;
                }
            }

            if (found) {
                return i;
            }
        }

        return -1;
    }

    public synchronized OutputStream setBinaryStream(final long pos) throws SQLException {
        if (pos < 1) {
            throw new SQLException("pos must be >= 1.");
        } else if (pos > _blob.length) {
            throw new SQLException("pos specified is past length of value.");
        } else if (pos >= Integer.MAX_VALUE) {
            throw new SQLException("pos must be < " + Integer.MAX_VALUE);
        }

        return new ByteArrayOutputStream() {
                    {write(_blob, 0, (int) pos - 1);}

                    public void flush() throws IOException {
                        synchronized (BlobImpl.this) {
                            byte[] blob = toByteArray();

                            if (blob.length < _blob.length) {
                                // Possible synchronization problem...
                                System.arraycopy(blob, 0, _blob, 0, blob.length);
                            } else {
                                _blob = blob;
                            }
                        }
                    }

                    public void close() throws IOException {
                        flush();
                    }
                };
    }

    public synchronized int setBytes(long pos, byte[] bytes) throws SQLException {
        if (bytes == null) {
            throw new SQLException("bytes cannot be null.");
        }

        return setBytes(pos, bytes, 0, bytes.length);
    }

    public synchronized int setBytes(long pos, byte[] bytes, int offset, int len)
    throws SQLException {
        OutputStream outputStream = setBinaryStream(pos);

        try {
            outputStream.write(bytes, offset, len);
            outputStream.close();
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
        } else if (len > _blob.length) {
            throw new SQLException("length specified is more than length of "
                                               + "value.");
        }

        int length = (int) len;
        byte[] blob = new byte[length];

        System.arraycopy(_blob, 0, blob, 0, length);

        _blob = blob;
    }

    /**
     * Returns the string representation of this object.
     */
    public synchronized String toString() {
        return _blob.toString();
    }
}

