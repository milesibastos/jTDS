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
package net.sourceforge.jtds.util;

import java.io.*;

/**
 * Provides the opposite functionality of InputStreamReader.
 *
 * @author Brian Heineman
 * @version $Id: ReaderInputStream.java,v 1.3 2004-11-17 14:01:04 alin_sinpalean Exp $
 */
public class ReaderInputStream extends InputStream {
    protected Reader _reader;
    protected String _encoding;
    private byte[] _singleByte = new byte[1];
    private byte[] _buffer;
    private int _pointer;

    /**
     * Constructs a new ReaderInputStream for the specified reader.
     *
     * @param reader
     */
    public ReaderInputStream(Reader reader) {
        if (reader == null) {
            throw new NullPointerException();
        }

        _reader = reader;
    }

    /**
     * Constructs a new ReaderInputStream for the specified reader.
     *
     * @param reader
     * @param encoding
     */
    public ReaderInputStream(Reader reader, String encoding) {
        if (reader == null) {
            throw new NullPointerException();
        } else if (encoding == null) {
            throw new NullPointerException();
        }

        _reader = reader;
        _encoding = encoding;
    }

    public synchronized int read() throws IOException {
        if (read(_singleByte, 0, 1) == -1) {
            return -1;
        }

        return _singleByte[0];
    }

    public synchronized int read(byte[] b, int off, int len)
    throws IOException {
        if (_reader == null) {
            throw new IOException("stream closed");
        }

        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0
                   || len < 0
                   || off > b.length
                   || off + len > b.length) {
            throw new ArrayIndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }

        int bytesRead = 0;
        // Consume all data from the buffer first
        if (_buffer != null) {
            int bufferLength = readBuffer(b, off, len);

            if (bufferLength == len) {
                return len;
            }

            len -= bufferLength;
            off += bufferLength;
            bytesRead += bufferLength;
        }

        if (_buffer == null) {
            char[] buffer = new char[len];
            int result = _reader.read(buffer, 0, len);

            if (result == -1) {
                return bytesRead > 0 ? bytesRead : -1;
            }

            String data = new String(buffer, 0, result);

            if (_encoding == null) {
                _buffer = data.getBytes();
            } else {
                _buffer = data.getBytes(_encoding);
            }

            return readBuffer(b, off, len);
        }

        return -1;
    }

    private int readBuffer(byte[] b, int off, int len) {
        int bufferLength = _buffer.length - _pointer;

        if (bufferLength >= len) {
            System.arraycopy(_buffer, _pointer, b, off, len);

            _pointer += len;

            return len;
        }

        System.arraycopy(_buffer, _pointer, b, off, bufferLength);

        _buffer = null;
        _pointer = 0;

        return bufferLength;
    }

    public synchronized void reset() throws IOException {
        if (_reader == null) {
            throw new IOException("stream closed");
        }

        _reader.reset();
    }

    public synchronized void close() throws IOException {
        if (_reader != null) {
            _reader.close();
            _reader = null;
        }

        _encoding = null;
    }
}
