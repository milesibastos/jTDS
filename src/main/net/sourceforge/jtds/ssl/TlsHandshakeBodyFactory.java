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
import java.io.IOException;

import net.sourceforge.jtds.util.BytesView;

/**
 * Class for creating handshake bodies from raw bytes.
 *
 * @author Rob Worsnop
 * @version $Id: TlsHandshakeBodyFactory.java,v 1.1 2005-01-04 17:13:04 alin_sinpalean Exp $
 */
class TlsHandshakeBodyFactory {
    /**
     * Creates a handshake body from raw bytes. A <code>null</code> body will be returned if the data cannot be
     * recognized as a handshake body. This will most likely be because the body is encrypted (as is the case with a
     * Finish message).
     *
     * @param data the data holding the body
     * @return the handshake body, or <code>null</code> if none can be decoded
     */
    public static TlsHandshakeBody create(BytesView data) throws IOException {
        DataInputStream dis = new DataInputStream(data.getInputStream());
        byte handshakeType = dis.readByte();


        int ch1 = dis.readByte();
        int ch2 = dis.readByte();
        int ch3 = dis.readByte();
        int length = ((ch1 << 16) + (ch2 << 8) + (ch3 << 0));


        // Apparent length does not match actual length. This is
        // not a readable handshake body.
        if (data.getLength() != (length + TlsHandshakeBody.HEADER_SIZE)) {
            return null;
        }

        return new TlsHandshakeBody(handshakeType, length);
    }
}
