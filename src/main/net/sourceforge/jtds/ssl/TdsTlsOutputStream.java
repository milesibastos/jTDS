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

import java.io.ByteArrayOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import net.sourceforge.jtds.util.BytesView;

/**
 * An output stream that mediates between JSSE and the DB server.
 * <p/>
 * SQL Server 2000 has the following requirements:
 * <ul>
 *   <li>All handshake records are delivered in TDS packets.
 *   <li>The "Client Key Exchange" (CKE), "Change Cipher Spec" (CCS) and
 *     "Finished" (FIN) messages are to be submitted in the delivered in both
 *     the same TDS packet and the same TCP packet.
 *   <li>From then on TLS/SSL records should be transmitted as normal -- the
 *     TDS packet is part of the encrypted application data.
 *
 * @author Rob Worsnop
 * @version $Id: TdsTlsOutputStream.java,v 1.1 2005-01-04 17:13:04 alin_sinpalean Exp $
 */
class TdsTlsOutputStream extends FilterOutputStream {
    /**
     * Used for holding back CKE, CCS and FIN records.
     */
    private List bufferedRecords = new ArrayList();

    /**
     * Constructs a TdsTlsOutputStream based on an underlying output stream.
     *
     * @param out the underlying output stream.
     */
    TdsTlsOutputStream(OutputStream out) {
        super(out);
    }

    /**
     * Holds back a record for batched transmission.
     */
    private void deferRecord(TlsRecord record) {
        record.detachData();
        bufferedRecords.add(record);
    }

    /**
     * Transmits the buffered batch of records.
     */
    private void flushBufferedRecords() throws IOException {
        Iterator iter = bufferedRecords.iterator();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        while (iter.hasNext()) {
            TlsRecord record = (TlsRecord) iter.next();
            record.write(bos);
        }
        putPacket(bos.toByteArray());
        bufferedRecords.clear();
    }

    /**
     * Transmits a record within a TDS packet.
     */
    private void putPacket(Record record) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(record
                .getLength());
        record.write(bos);
        putPacket(bos.toByteArray());
    }

    /**
     * Transmits bytes within a TDS packet.
     */
    private void putPacket(byte[] data) throws IOException {
        Util.putPacket(out, data);
    }

    /*
     * (non-Javadoc)
     *
     * @see java.io.OutputStream#write(byte[], int, int)
     */
    public void write(byte[] b, int off, int len) throws IOException {
        // JSSE always writes some kind of SSL/TLS record.
        Record r = RecordFactory.create(new BytesView(b, off, len));
        if (r instanceof Sslv2ClientHelloRecord) {
            // Client Hello records go in their own TDS packets.
            putPacket(r);
            return;
        }

        // We are using TLS, so the Client Hello message is the only
        // non-TLS record possible. From now on, assume TLS.
        TlsRecord record = (TlsRecord) r;
        if (record.getContentType() == TlsRecord.TYPE_APPLICATIONDATA) {
            // TDS header in the encrypted application data. Don't add
            // another one.
            record.write(out);
            return;
        }

        if (record.getContentType() == TlsRecord.TYPE_CHANGECIPHERSPEC) {
            // CCS records are batched.
            deferRecord(record);
            return;
        }

        if (record.getContentType() == TlsRecord.TYPE_HANDSHAKE) {
            TlsHandshakeRecord hsrec = (TlsHandshakeRecord) record;
            TlsHandshakeBody hsbody = hsrec.getHandshakeBody();
            if (hsbody == null) {
                // must be Finish record; we could not
                // decipher encrypted body
                deferRecord(record);
                // Finish is the signal to send the batch.
                flushBufferedRecords();
                return;
            } else if (hsbody.getHandshakeType() == TlsHandshakeBody.TYPE_CLIENTKEYEXCHANGE) {
                // CKE records are deferred
                deferRecord(record);
                return;
            } else if (hsbody.getHandshakeType() == TlsHandshakeBody.TYPE_CLIENTHELLO) {
                // Client Hello records go in their own TDS packets.
                putPacket(record);
                return;
            } else {
                // probably a Finished record whose encrypted data
                // coincidentally mimicked that of a plaintext record.
                deferRecord(record);
                flushBufferedRecords();
                return;
            }
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see java.io.OutputStream#flush()
     */
    public void flush() throws IOException {
        super.flush();
    }

}

