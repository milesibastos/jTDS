// jTDS JDBC Driver for Microsoft SQL Server and Sybase
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

import java.io.DataInputStream;
import java.io.IOException;

import net.sourceforge.jtds.util.BytesView;

/**
 * Used for analyzing raw bytes and converting them to SSL (and TLS) records.
 *
 * @author Rob Worsnop
 * @version $Id: RecordFactory.java,v 1.1 2005-01-04 17:13:04 alin_sinpalean Exp $
 */
class RecordFactory {
    /**
     * Creates a record from raw bytes.
     */
    public static Record create(BytesView data) throws IOException {

        DataInputStream dis = new DataInputStream(data.getInputStream());
        byte contentType = dis.readByte();
        byte[] version = new byte[2];
        dis.read(version);
        short length = dis.readShort();

        if (!isTlsRecord(contentType, length, data)) {
            return new Sslv2ClientHelloRecord(data);
        }

        if (contentType == TlsRecord.TYPE_APPLICATIONDATA) {
            return new TlsApplicationDataRecord(contentType, version, length,
                    data);
        } else if (contentType == TlsRecord.TYPE_CHANGECIPHERSPEC) {
            return new TlsChgCipherSpecRecord(contentType, version, length,
                    data);
        } else if (contentType == TlsRecord.TYPE_HANDSHAKE) {
            return new TlsHandshakeRecord(contentType, version, length, data);
        } else if (contentType == TlsRecord.TYPE_ALERT) {
            return new TlsAlertRecord(contentType, version, length, data);
        } else {
            return null; // shouldn't get here
        }

    }

    /**
     * Guesses whether or not the record is a TLS record, based on its apparent
     * content type, apparent length, and actual length. A TLS record is
     * assumed if the apparent type is in the correct range and the apparent
     * length corresponds with the actual length of the record data.
     *
     * @param contentType the apparent content type
     * @param length      the apparent length
     * @param data        the raw data
     */
    private static boolean isTlsRecord(byte contentType, short length,
                                       BytesView data) {
        boolean isTlsType = contentType >= TlsRecord.TYPE_CHANGECIPHERSPEC
                && contentType <= TlsRecord.TYPE_APPLICATIONDATA;
        return (isTlsType && (length + TlsRecord.HEADER_SIZE == data.getLength()));
    }
}