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

import java.io.*;
import java.sql.*;

import net.sourceforge.jtds.util.Logger;

/**
 * An in-memory, disk or database representation of binary data.
 *
 * @author Brian Heineman
 * @author Mike Hutchinson
 * @version $Id: BlobImpl.java,v 1.25 2004-12-06 12:10:13 alin_sinpalean Exp $
 */
public class BlobImpl implements Blob {
	private static final byte[] EMPTY_BLOB = new byte[0];

    private ConnectionJDBC2 _connection;
	private byte[] _blob;
    private File _blobFile;
    private JtdsInputStream _jtdsInputStream;

    /**
     * Constructs a new Blob instance.
     *
     * @param callerReference an object reference to the caller of this method;
     *        must be a <code>Connection</code>, <code>Statement</code> or
     *        <code>ResultSet</code>
     */
    BlobImpl(Object callerReference) {
        this(callerReference, EMPTY_BLOB);
    }

    /**
     * Constructs a new Blob instance.
     *
     * @param callerReference an object reference to the caller of this method;
     *        must be a <code>Connection</code>, <code>Statement</code> or
     *        <code>ResultSet</code>
     * @param blob the blob object to encapsulate
     */
    BlobImpl(Object callerReference, byte[] blob) {
        if (blob == null) {
            throw new IllegalArgumentException("blob cannot be null.");
        }

        _connection = Support.getConnection(callerReference);
        _blob = blob;
    }

    /**
     * Constructs a new Blob instance.
     *
     * @param callerReference an object reference to the caller of this method;
     *        must be a <code>Connection</code>, <code>Statement</code> or
     *        <code>ResultSet</code>
     * @param in the blob object to encapsulate
     */
    BlobImpl(Object callerReference, ResponseStream in) throws IOException {
        if (in == null) {
            throw new IllegalArgumentException("in cannot be null.");
        }

        _connection = Support.getConnection(callerReference);

        TextPtr tp = new TextPtr();

        in.read(tp.ptr);
        in.read(tp.ts);
        tp.len = in.readInt();

        if (tp.len < _connection.getLobBuffer()) {
        	_blob = new byte[tp.len];
        	in.read(_blob);
        } else {
			_blob = EMPTY_BLOB;

        	try {
	        	OutputStream outputStream = setBinaryStream(1);
	        	byte[] buffer = new byte[1024];
	        	int length = tp.len;
	        	int result;

	        	while ((result = in.read(buffer, 0, Math.min(length, buffer.length))) != -1 && length != 0) {
	        		outputStream.write(buffer, 0, result);
	        		length -= result;
	        	}

	        	outputStream.close();

                if (length != 0) {
                    throw new IOException(Messages.get("error.generic.ioread", "bytes", "unexpected EOF"));
                }
        	} catch (SQLException e) {
        		// Should never happen...
        	}
        }
/*
        if (statement != null && statement.getMaxFieldSize() == 1) {
            // Try to return BLOB built over InputStream
            ColData data = getColumn(columnIndex);
            ColInfo ci = columns[columnIndex - 1];

            if (data.getTextPtr() != null && ci.jdbcType == java.sql.Types.LONGVARBINARY) {
                return new BlobImpl(new JtdsInputStream((ConnectionJDBC2) statement.getConnection(),
                                            ci,
                                            data,
                                            "US-ASCII"));
            }
        }
*/
    }

    /**
     * Returns an InputStream for the BLOB data.
     */
    public synchronized InputStream getBinaryStream() throws SQLException {
        try {
        	if (_blob != null) {
                return new ByteArrayInputStream(_blob);
        	} else if (_blobFile != null) {
    			return new BufferedInputStream(new FileInputStream(_blobFile));
            }

            _jtdsInputStream.reset();

            return _jtdsInputStream;
        } catch (IOException e) {
            throw new SQLException(Messages.get("error.generic.ioerror", e.getMessage()),
                                   "HY000");
        }
    }

    public byte[] getBytes(long pos, int length) throws SQLException {
        if (pos < 1) {
            throw new SQLException(Messages.get("error.blobclob.badpos"), "HY090");
        } else if (length < 0) {
            throw new SQLException(Messages.get("error.blobclob.badlen"), "HY090");
        } else if (pos - 1 + length > length()) {
            // Don't throw an exception, just return as much data as available
            length = (int) (length() - pos + 1);
        }

        if (length == 0) {
            return EMPTY_BLOB;
        }

        InputStream inputStream = getBinaryStream();

        skip(inputStream, pos - 1);

        try {
            byte[] buffer = new byte[length];
            int bytesRead = 0, res;

            while ((res = inputStream.read(buffer, bytesRead, length - bytesRead)) != -1) {
                bytesRead += res;

                if (bytesRead == length) {
                    return buffer;
                }
            }

            throw new SQLException(Messages.get("error.blobclob.readlen"), "HY000");
        } catch (IOException e) {
            throw new SQLException(
                 Messages.get("error.generic.ioread", "byte", e.getMessage()),
                                    "HY000");
        }
    }

    /**
     * Returns the length of the value.
     */
    public synchronized long length() throws SQLException {
    	if (_blob != null) {
            return _blob.length;
    	} else if (_blobFile != null) {
    		return _blobFile.length();
    	}

        return _jtdsInputStream.getLength();
    }

    public long position(byte[] pattern, long start) throws SQLException {
        return position(new BlobImpl(_connection, pattern), start);
    }

    public long position(Blob pattern, long start) throws SQLException {
        if (pattern == null) {
            throw new SQLException(Messages.get("error.blob.badpattern"), "HY024");
        }

        try {
            InputStream inputStream = getBinaryStream();
            long length = length() - pattern.length();
            boolean reset = true;

            // TODO Implement a better pattern matching algorithm
            for (long i = start; i < length; i++) {
                boolean found = true;
                int value;

                if (reset) {
                    inputStream = getBinaryStream();
                    skip(inputStream, i);
                    reset = false;
                }

                value = inputStream.read();

                InputStream patternInputStream = pattern.getBinaryStream();
                int searchValue;

                while ((searchValue = patternInputStream.read()) != -1) {
                    if (value != searchValue) {
                        found = false;
                        break;
                    }

                    reset = true;
                }

                if (found) {
                    return i;
                }
            }
        } catch (IOException e) {
            throw new SQLException(
                Messages.get("error.generic.ioread", "String", e.getMessage()),
                                   "HY000");
        }

        return -1;
    }

    public synchronized OutputStream setBinaryStream(final long pos) throws SQLException {
        long length = length();

        if (pos < 1) {
            throw new SQLException(Messages.get("error.blobclob.badpos"), "HY090");
        } else if (pos > length && pos != 1) {
            throw new SQLException(Messages.get("error.blobclob.badposlen"), "HY090");
        }

        return new BlobOutputStream(pos, length);
    }

    public int setBytes(long pos, byte[] bytes) throws SQLException {
        if (bytes == null) {
            throw new SQLException(Messages.get("error.blob.bytesnull"), "HY024");
        }

        return setBytes(pos, bytes, 0, bytes.length);
    }

    public int setBytes(long pos, byte[] bytes, int offset, int len)
            throws SQLException {
        OutputStream outputStream = setBinaryStream(pos);

        try {
            outputStream.write(bytes, offset, len);
            outputStream.close();
        } catch (IOException e) {
            throw new SQLException(Messages.get("error.generic.iowrite",
            		                                  "bytes",
													  e.getMessage()),
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
        long currentLength = length();

        if (len < 0) {
            throw new SQLException(Messages.get("error.blobclob.badlen"), "HY090");
        } else if (len > currentLength) {
            throw new SQLException(Messages.get("error.blobclob.lentoolong"), "HY090");
        }

        if (len == currentLength) {
            return;
        } else if (len <= _connection.getLobBuffer()) {
            _blob = getBytes(1, (int) len);

            if (_blobFile != null) {
            	_blobFile.delete();
            	_blobFile = null;
            }

            _jtdsInputStream = null;
        } else {
            try {
                InputStream inputStream = getBinaryStream();
                File tmpFile = _blobFile;

                _blob = new byte[0];
                _blobFile = null;
                _jtdsInputStream = null;

                OutputStream outputStream = setBinaryStream(1);
                byte[] buffer = new byte[1024];
                int result;

                while ((result = inputStream.read(buffer, 0, (int) Math.min(buffer.length, len))) > 0) {
                    len -= result;
                    outputStream.write(buffer, 0, result);
                }

		        outputStream.close();

                // If the data came from a file; delete the original file to
                // free disk space
                if (tmpFile != null) {
                    tmpFile.delete();
                }
	        } catch (IOException e) {
	            throw new SQLException(Messages.get("error.generic.iowrite",
	            		                                  "bytes",
														  e.getMessage()),
									   "HY000");
	        }
        }
    }

    private void skip(InputStream inputStream, long skip) throws SQLException {
        try {
            long skipped = inputStream.skip(skip);

            if (skipped != skip) {
                throw new SQLException(Messages.get("error.blobclob.badposlen"), "HY090");
            }
        } catch (IOException e) {
            throw new SQLException(Messages.get("error.generic.ioerror", e.getMessage()),
                                   "HY000");
        }
    }

    protected void finalize() {
    	if (_blobFile != null) {
    		_blobFile.delete();
    	}
    }

    /**
     * Class to manage any Blob write.
     */
    class BlobOutputStream extends OutputStream {
        private OutputStream outputStream;
        private long curPos;
        private boolean securityFailure = false;

        BlobOutputStream(long pos, long length) throws SQLException {
            curPos = pos - 1;

            try {
                if (length > _connection.getLobBuffer()) {
                    if (_blobFile == null) {
                        writeToDisk(getBinaryStream());
                    }
                } else if (_jtdsInputStream != null) {
                    ByteArrayOutputStream baos = new ByteArrayOutputStream((int) length);

                    byte[] buffer = new byte[1024];
                    int result;

                    while ((result = _jtdsInputStream.read(buffer)) != -1) {
                        baos.write(buffer, 0, result);
                    }

                    _blob = baos.toByteArray();
                    _jtdsInputStream = null;
                }

                updateOuputStream();
            } catch (IOException e) {
                throw new SQLException(Messages.get("error.generic.ioerror", e.getMessage()),
                                       "HY000");
            }
        }

        public void write(int b) throws IOException {
            synchronized (BlobImpl.this) {
                checkSize(1);
                outputStream.write(b);
                curPos++;
            }
        }

        public void write(byte[] b, int off, int len) throws IOException {
            synchronized (BlobImpl.this) {
                checkSize(len);
                outputStream.write(b, off, len);
                curPos += len;
            }
        }

        /**
         * Checks the size of the in-memory buffer; if a write will
         * cause the size to exceed <code>MAXIMUM_SIZE</code> then
         * the data will be removed from memory and written to disk.
         *
         * @param length the length of data to be written
         */
        private void checkSize(long length) throws IOException {
            // Return if the data has already exceeded the maximum size
            if (curPos > _connection.getLobBuffer()) {
                return;
            }

            // Return if a file is already being used to store the data
            if (_blobFile != null) {
                return;
            }

            // Return if there was a security failure attempting to
            // create a buffer file
            if (securityFailure) {
                return;
            }

            // Return if the length will not exceed the maximum in-memory
            // value
            if (curPos + length <= _connection.getLobBuffer()) {
                return;
            }

            if (_blob != null) {
                writeToDisk(new ByteArrayInputStream(_blob));
                updateOuputStream();
            }
        }

        void writeToDisk(InputStream inputStream) throws IOException {
            OutputStream os;

            try {
                _blobFile = File.createTempFile("jtds", ".tmp");
                _blobFile.deleteOnExit();

                os = new BufferedOutputStream(new FileOutputStream(_blobFile));
            } catch (SecurityException e) {
                // Unable to write to disk
                securityFailure = true;

                if (_blobFile != null) {
                    try {
                        _blobFile.delete();
                    } catch (SecurityException ex) {
                        // Ignore exception
                    }
                    _blobFile = null;
                }

                os = new ByteArrayOutputStream();

                if (Logger.isActive()) {
                    Logger.println("Blob: Unable to buffer data to disk: " + e.getMessage());
                }
            }

            try {
                byte[] buffer = new byte[1024];
                int result;

                while ((result = inputStream.read(buffer)) != -1) {
                    os.write(buffer, 0, result);
                }
            } finally {
                os.flush();

                if (os instanceof ByteArrayOutputStream) {
                    _blob = ((ByteArrayOutputStream) os).toByteArray();
                } else {
                    _blob = null;
                }

                os.close();
            }
        }

        /**
         * Updates the <code>outputStream</code> member by creating the
         * appropriate type of output stream based upon the current
         * storage mechanism.
         *
         * @throws IOException if any failure occured while creating the
         *         output stream
         */
        void updateOuputStream() throws IOException {
            if (_blob != null) {
                final long startPos = curPos;

                outputStream = new OutputStream() {
                    int curPos = (int) startPos;
                    boolean closed = false;

                    private void checkOpen() throws IOException {
                        if (closed) {
                            throw new IOException("stream closed");
                        } else if (_blob == null) {
                            throw new IOException(
                                    Messages.get("error.generic.iowrite", "byte", "_blob = NULL"));
                        }
                    }

                    public void write(int b) throws IOException {
                        checkOpen();

                        if (curPos + 1 > _blob.length) {
                            byte[] buffer = new byte[curPos + 1];

                            System.arraycopy(_blob, 0, buffer, 0, _blob.length);
                            _blob = buffer;
                        }

                        _blob[curPos++] = (byte) b;
                    }

                    public void write(byte[] b, int off, int len) throws IOException {
                        checkOpen();

                        if (b == null) {
                            throw new NullPointerException();
                        } else if (off < 0 || off > b.length || len < 0 ||
                                off + len > b.length || off + len < 0) {
                            throw new IndexOutOfBoundsException();
                        } else if (len == 0) {
                            return;
                        }

                        // Reallocate the buffer
                        if (curPos + len > _blob.length) {
                            byte[] buffer = new byte[curPos + len];

                            System.arraycopy(_blob, 0, buffer, 0, _blob.length);
                            _blob = buffer;
                        }

                        // Append the contents of b to the blob
                        System.arraycopy(b, off, _blob, curPos, len);
                        curPos += len;
                    }

                    public void close() {
                        closed = true;
                    }
                };
            } else {
                outputStream = new BlobFileOutputStream(curPos);
            }
        }

        public void flush() throws IOException {
            outputStream.flush();
        }

        public void close() throws IOException {
            outputStream.close();
        }
    };


    /**
     * Class to manage Blob file writes.
     */
    class BlobFileOutputStream extends OutputStream {
        RandomAccessFile raf;

        BlobFileOutputStream(long curPos) throws IOException {
            raf = new RandomAccessFile(_blobFile, "rw");
            raf.seek(curPos);
        }

        public void write(int b) throws IOException {
            if (raf == null) {
                throw new IOException("stream closed");
            }

            raf.write(b);
        }

        public void write(byte b[], int off, int len) throws IOException {
            if (raf == null) {
                throw new IOException("stream closed");
            }

            raf.write(b, off, len);
        }

        public void close() throws IOException {
            if (raf != null) {
                raf.close();
                raf = null;
            }
        }
    };
}

