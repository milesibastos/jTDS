//jTDS JDBC Driver for Microsoft SQL Server and Sybase
//Copyright (C) 2004 The jTDS Project
//
//This library is free software; you can redistribute it and/or
//modify it under the terms of the GNU Lesser General Public
//License as published by the Free Software Foundation; either
//version 2.1 of the License, or (at your option) any later version.
//
//This library is distributed in the hope that it will be useful,
//but WITHOUT ANY WARRANTY; without even the implied warranty of
//MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
//Lesser General Public License for more details.
//
//You should have received a copy of the GNU Lesser General Public
//License along with this library; if not, write to the Free Software
//Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
//
package net.sourceforge.jtds.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Presents a view on a byte array based on an offset and length.
 *
 * @author Rob Worsnop
 * @version $Id: BytesView.java,v 1.1 2005-01-04 17:13:04 alin_sinpalean Exp $
 */
public class BytesView {
    private byte[] b;
    private int off;
    private int len;

    /**
     * Constructs the object.
     *
     * @param b   the underlying bytes
     * @param off the offset within the underlying bytes
     * @param len the number of bytes to be presented by the view
     */
    public BytesView(byte[] b, int off, int len) {
        checkParams(b.length, off, len);
        this.b = b;
        this.off = off;
        this.len = len;
    }

    /**
     * Returns the number of bytes represented by the view.
     */
    public int getLength() {
        return len;
    }

    /**
     * Returns a stream that can be used to read the bytes presented by the
     * view.
     *
     * @return the stream
     */
    public InputStream getInputStream() {
        return new ByteArrayInputStream(b, off, len);
    }

    /**
     * Writes out the bytes presented by the view to an output stream.
     *
     * @param out the stream
     */
    public void write(OutputStream out) throws IOException {
        out.write(b, off, len);
    }

    /**
     * Extracts a subset of the bytes presented by the view.
     *
     * @param off the offset with the view
     * @param len the length of the subset
     * @return a <code>BytesView</code> for the subset
     */
    public BytesView getSubset(int off, int len) {
        checkParams(this.len, off, len);
        return new BytesView(b, this.off + off, len);
    }

    /**
     * Extracts a subset of the bytes presented by the view.
     *
     * @param off the offset with the view
     * @return a <code>BytesView</code> for the subset
     */
    public BytesView getSubset(int off) {
        return getSubset(off, this.len - off);
    }

    /**
     * Clones the underlying bytes so they can't be modified elsewhere.
     *
     * @return the new <code>BytesView</code>
     */
    public BytesView deepClone() {
        byte[] buf = new byte[len];
        System.arraycopy(b, off, buf, 0, len);
        return new BytesView(buf, 0, len);
    }

    private static void checkParams(int len, int newoff, int newlen) {
        if (newoff < 0 || newlen < 0 || (newoff + newlen) > len) {
            throw new IndexOutOfBoundsException();
        }
    }
}
