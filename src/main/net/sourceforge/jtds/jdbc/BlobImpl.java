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
import java.sql.Blob;
import java.sql.SQLException;

/**
 * An in-memory representation of binary data.
 * <p>
 * Implementation note:
 * <ol>
 * <li> Mostly Brian's original code but modified to include the
 *      ability to convert a stream into a byte[] when required.
 * <li> SQLException messages loaded from properties file.
 * </ol>
 *
 * @author Brian Heineman
 * @author Mike Hutchinson
 * @version $Id: BlobImpl.java,v 1.9 2004-06-27 17:00:50 bheineman Exp $
 */
public class BlobImpl implements Blob {
    private byte[] _blob;
    private InputStream stream;
    private int length;

    /**
     * Constructs a new Blob instance.
     *
     * @param blob The byte[] object to encapsulate
     */
    BlobImpl(byte[] blob) {
        if (blob == null) {
            throw new IllegalArgumentException("blob cannot be null.");
        }

        _blob = blob;
    }

    /**
     * Constructs a new Blob instance from a stream.
     *
     * @param stream The input stream to initialise the Blob.
     */
    BlobImpl(InputStream stream) {
        _blob = new byte[0];
        this.stream = stream;

        try {
            this.length = stream.available();
        } catch (IOException e) {
            // Will never occur
        }
    }

    /**
     * Returns a stream for the BLOB data.
     */
    public synchronized InputStream getBinaryStream() throws SQLException {
        if (stream != null) {
            return stream;
        }

        return new ByteArrayInputStream(_blob);
    }

    public synchronized byte[] getBytes(long pos, int length) throws SQLException {
        loadBlob();

        if (pos < 1) {
            throw new SQLException(Support.getMessage("error.blobclob.badpos"), "HY090");
        } else if (_blob.length > 0 && pos > _blob.length) {
            throw new SQLException(Support.getMessage("error.blobclob.badposlen"), "HY090");
        } else if (pos > Integer.MAX_VALUE) {
            throw new SQLException(Support.getMessage("error.blobclob.postoolong"), "HY090");
        } else if (length < 0) {
            throw new SQLException(Support.getMessage("error.blobclob.badlen"), "HY090");
        } else if (pos + length > _blob.length+1) {
            throw new SQLException(Support.getMessage("error.blobclob.lentoolong"), "HY090");
        }

        pos--;

        if (pos == 0 && length == _blob.length) {
            return _blob;
        } else {
            byte[] value = new byte[length];

            System.arraycopy(_blob, (int) pos, value, 0, length);
            return value;
        }
    }

    /**
     * Returns the length of the value.
     */
    public synchronized long length() throws SQLException {
        if (stream != null) {
            return length;
        }

        return _blob.length;
    }

    public synchronized long position(Blob pattern, long start) throws SQLException {
        if (pattern == null) {
            throw new SQLException(Support.getMessage("error.blob.badpattern"), "HY024");
        }

        long length = pattern.length();

        if (length > Integer.MAX_VALUE) {
            throw new SQLException(Support.getMessage("error.blob.pattoolong"), "HY090");
        }

        return position(pattern.getBytes(0, (int) length), start);
    }

    public synchronized long position(byte[] pattern, long start) throws SQLException {
        int length = _blob.length - pattern.length;

        if (pattern == null) {
            throw new SQLException(Support.getMessage("error.blob.badpattern"), "HY024");
        } else if (start < 1) {
            throw new SQLException(Support.getMessage("error.blobclob.badstart"), "HY090");
        } else if (start >= Integer.MAX_VALUE) {
            throw new SQLException(Support.getMessage("error.blobclob.postoolong"), "HY090");
        }

        loadBlob();

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
            throw new SQLException(Support.getMessage("error.blobclob.badpos"), "HY090");
        } else if (pos > _blob.length) {
            throw new SQLException(Support.getMessage("error.blobclob.badposlen"), "HY090");
        } else if (pos >= Integer.MAX_VALUE) {
            throw new SQLException(Support.getMessage("error.blobclob.postoolong"), "HY090");
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
            throw new SQLException(Support.getMessage("error.blob.bytesnull"), "HY024");
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
            throw new SQLException(
                                  Support.getMessage("error.generic.iowrite", "bytes", e.getMessage()),
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
        } else if (len > _blob.length) {
            throw new SQLException(Support.getMessage("error.blobclob.lentoolong"), "HY090");
        }

        loadBlob();

        int length = (int) len;
        byte[] blob = new byte[length];

        System.arraycopy(_blob, 0, blob, 0, length);

        _blob = blob;
    }

    /**
     * Returns the string representation of this object.
     */
    public synchronized String toString() {
        try {
            loadBlob();
        } catch (SQLException e) {
            _blob = new byte[0];
        }

        return _blob.toString();
    }

    /**
     * Initialize the in memory object from the InputStream.
     *
     * @throws SQLException
     */
    private void loadBlob() throws SQLException {
        if (stream == null ||length < 0) {
            return;
        }

        _blob = new byte[length];

        try {
            stream.read(_blob);
            stream = null;
        } catch (IOException ioe) {
            throw new SQLException(
                                  Support.getMessage("error.generic.ioread",
                                                     "InputStream",
                                                     ioe.getMessage()),
                                  "HY000");
        }
    }
}

