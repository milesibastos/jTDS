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
//MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
//Lesser General Public License for more details.
//
//You should have received a copy of the GNU Lesser General Public
//License along with this library; if not, write to the Free Software
//Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
//
package net.sourceforge.jtds.ssl;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

import net.sourceforge.jtds.util.KnownLengthInputStream;

/**
 * An input stream that filters out TDS headers so they are not returned to
 * JSSE (which will not recognize them).
 *
 * @author Rob Worsnop
 * @version $Id: TdsTlsInputStream.java,v 1.1 2005-01-04 17:13:04 alin_sinpalean Exp $
 */
class TdsTlsInputStream extends FilterInputStream {

    int bytesOutstanding = 0;

    byte[] readBuffer = new byte[17 * 1024];

    InputStream bufferStream = null;

    /**
     * Constructs a TdsTlsInputStream and bases it on an underlying stream.
     *
     * @param in the underlying stream
     */
    public TdsTlsInputStream(InputStream in) {
        super(in);
    }

    /*
     * (non-Javadoc)
     *
     * @see java.io.InputStream#read(byte[], int, int)
     */
    public int read(byte[] b, int off, int len) throws IOException {

        // If this is the start of a new TLS record or
        // TDS packet we need to read in entire record/packet.
        if (bufferStream == null) {
            primeBuffer();
        }

        // Feed the client code bytes from the buffer
        int ret = bufferStream.read(b, off, len);
        bytesOutstanding -= ret;
        if (bytesOutstanding == 0) {
            // All bytes in the buffer have been read.
            // The next read will prime it again.
            bufferStream = null;
        }
        return ret;
    }

    /**
     * Read in entire TLS record or TDS packet and store the TLS record in the
     * buffer. (TDS packets will always contain a TLS record.)
     */
    private void primeBuffer() throws IOException {
        // first read the type (first byte for TDS and TLS).
        in.read(readBuffer, 0, 1);
        int len;
        if (readBuffer[0] == TdsPacket.TYPE_RESPONSE) {
            len = readTDSPacket(readBuffer, 1);
        } else {
            len = readTLSRecord(readBuffer, 1);
        }

        bufferStream = new ByteArrayInputStream(readBuffer, 0, len);
        bytesOutstanding = len;
    }

    /**
     * Reads all but the first byte of a TLS Record into a buffer.
     *
     * @param buf the buffer into which to read the TLS record
     * @param off the offset within the buffer at which to start
     * @return the size of the TLS Record
     */
    private int readTLSRecord(byte[] buf, int off) throws IOException {
        in.read(buf, off, TlsRecord.HEADER_SIZE - 1);
        short length = getShort(buf, 3);
        new KnownLengthInputStream(in).read(buf, off + (TlsRecord.HEADER_SIZE - 1), length);
        return length + TlsRecord.HEADER_SIZE;
    }

    /**
     * Reads all but the first byte of a TDS packet into a buffer.
     *
     * @param buf the buffer into which to read the TDS packet
     * @param off the offset within the buffer at which to start
     * @return the size of the TLS Record contained within the TDS packet
     */
    private int readTDSPacket(byte[] buf, int off) throws IOException {
        in.read(buf, off, TdsPacket.HEADER_SIZE - 1);
        short length = getShort(buf, 2);
        new KnownLengthInputStream(in).read(buf, 0, length - TdsPacket.HEADER_SIZE);
        return length - TdsPacket.HEADER_SIZE;
    }

    /**
     * Extracts a <code>short</code> from a byte buffer.
     *
     * @param b   the byte buffer containing a <code>short</code>
     * @param off the location within <code>buf</code> of the <code>short</code>
     * @return the <code>short</code>
     */
    private short getShort(byte[] b, int off) throws IOException {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(b,
                off, 2));
        return dis.readShort();
    }
}

