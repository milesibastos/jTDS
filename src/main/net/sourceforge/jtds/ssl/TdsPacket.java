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

package net.sourceforge.jtds.ssl;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Encapsulates a TDS packet.
 *
 * @author Rob Worsnop
 * @version $Id: TdsPacket.java,v 1.1 2005-01-04 17:13:04 alin_sinpalean Exp $
 */
class TdsPacket {
    public final static int HEADER_SIZE = 8;
    public final static int TYPE_RESPONSE = 4;

    byte type;
    byte status;
    short size;
    short channel;
    byte pktNumber;
    byte window;

    byte[] data;

    /**
     * Constructs a TdsPacket from an input stream.
     *
     * @param in the stream
     */
    public TdsPacket(InputStream in) throws IOException {
        read(in);
    }

    /**
     * Constructs a TdsPacket from header attributes and packet body data.
     *
     * @param type      packet type
     * @param status    status
     * @param size      packet size
     * @param channel   channel
     * @param pktNumber packet number
     * @param window    window
     * @param data      packet data
     */
    public TdsPacket(byte type, byte status, short size, short channel, byte pktNumber,
                     byte window, byte[] data) {

        this.type = type;
        this.status = status;
        this.size = size;
        this.channel = channel;
        this.pktNumber = pktNumber;
        this.window = window;
        this.data = data;
    }

    /**
     * Returns the size of the packet.
     *
     * @return the size of the packet
     */
    public int getSize() {
        return size;
    }

    /**
     * Returns the packet's data.
     *
     * @return the packet's data
     */
    public byte[] getData() {
        return data;
    }

    /**
     * Writes a TdsPacket out to an output stream.
     *
     * @param out the output stream
     */
    public void write(OutputStream out) throws IOException {
        DataOutputStream dos = new DataOutputStream(out);
        dos.writeByte(type);
        dos.writeByte(status);
        dos.writeShort(size);
        dos.writeShort(channel);
        dos.writeByte(pktNumber);
        dos.writeByte(window);
        dos.write(data);
    }

    /**
     * Reads in a TdsPacket from an input stream.
     *
     * @param in the input stream
     */
    private void read(InputStream in) throws IOException {
        DataInputStream dis = new DataInputStream(in);
        type = dis.readByte();
        status = dis.readByte();
        size = dis.readShort();
        channel = dis.readShort();
        pktNumber = dis.readByte();
        window = dis.readByte();
        int datasize = size - HEADER_SIZE;
        data = new byte[datasize];
        int read = dis.read(data);
        if (read < datasize) {
            throw new EOFException("Read " + (read + HEADER_SIZE)
                    + " bytes for TDS packet; expected " + size);
        }
    }
}
