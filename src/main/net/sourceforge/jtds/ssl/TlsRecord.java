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

import net.sourceforge.jtds.util.BytesView;

/**
 * The base class for TLS records.
 *
 * @author Rob Worsnop
 * @version $Id: TlsRecord.java,v 1.1 2005-01-04 17:13:04 alin_sinpalean Exp $
 */
abstract class TlsRecord extends Record {
    public static final int HEADER_SIZE = 5;
    public static final byte TYPE_CHANGECIPHERSPEC = 20;
    public static final byte TYPE_ALERT = 21;
    public static final byte TYPE_HANDSHAKE = 22;
    public static final byte TYPE_APPLICATIONDATA = 23;

    private byte contentType;
    private byte[] version;
    private short length;

    /**
     * Constructs a TlsRecord.
     */
    public TlsRecord(byte contentType, byte[] version, short length, BytesView data) {
        super(data);
        this.contentType = contentType;
        this.version = version;
        this.length = length;
    }

    /**
     * Returns the content type.
     */
    public byte getContentType() {
        return contentType;
    }


}
