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

/**
 * Encapsulates a TLS handshake body.
 *
 * @author Rob Worsnop
 * @version $Id: TlsHandshakeBody.java,v 1.1 2005-01-04 17:13:04 alin_sinpalean Exp $
 */
class TlsHandshakeBody {
    public final static int HEADER_SIZE = 4;
    public final static int TYPE_CLIENTKEYEXCHANGE = 16;
    public final static int TYPE_CLIENTHELLO = 1;
    private byte handshakeType;
    private int length;

    /**
     * Constructs a TlsHandshakeBody.
     */
    public TlsHandshakeBody(byte handshakeType, int length) {
        this.handshakeType = handshakeType;
        this.length = length;
    }

    /**
     * Returns the handshake type.
     */
    public byte getHandshakeType() {
        return handshakeType;
    }

    /**
     * Returns the handshake length.
     */
    public int getLength() {
        return length;
    }
}
