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
* Provides the opposite functionality of OutputStreamWriter.
*
* @author Brian Heineman
* @version $Id: WriterOutputStream.java,v 1.4 2004-08-24 19:17:07 bheineman Exp $
*/
public class WriterOutputStream extends OutputStream {
    protected Writer _writer;
    protected String _encoding;
    private byte[] _singleByte = new byte[1];

    /**
     * Constructs a new WriterOutputStream for the specified writer.
     * 
     * @param writer
     */
    public WriterOutputStream(Writer writer) {
        if (writer == null) {
            throw new NullPointerException();
        }
        
        _writer = writer;
    }
    
    /**
     * Constructs a new WriterOutputStream for the specified writer.
     * 
     * @param writer
     * @param encoding
     */
    public WriterOutputStream(Writer writer, String encoding) {
        if (writer == null) {
            throw new NullPointerException();
        } else if (encoding == null) {
            throw new NullPointerException();
        }
        
        _writer = writer;
        _encoding = encoding;
    }

    public synchronized void write(int b) throws IOException {
        _singleByte[0] = (byte) b;
        write(_singleByte, 0, 1);
    }

    public synchronized void write(byte[] b, int off, int len) throws IOException {
        if (_writer == null) {
            throw new IOException("stream closed");
        }
        
        String data;
        
        if (_encoding == null) {
            data = new String(b, off, len);
        } else {
            data = new String(b, off, len, _encoding);
        }

        // TODO Optimize with buffer and String.getChars()
        _writer.write(data.toCharArray());
    }

    public synchronized void flush() throws IOException {
        if (_writer == null) {
            throw new IOException("stream closed");
        }
        
        _writer.flush();
    }
    
    public synchronized void close() throws IOException {
        if (_writer != null) {
            _writer.close();
            _writer = null;
        }
        
        _encoding = null;
    }
}
