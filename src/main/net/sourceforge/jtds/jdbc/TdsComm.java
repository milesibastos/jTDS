//
// Copyright 1998 CDS Networks, Inc., Medford Oregon
//
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
// 1. Redistributions of source code must retain the above copyright
//    notice, this list of conditions and the following disclaimer.
// 2. Redistributions in binary form must reproduce the above copyright
//    notice, this list of conditions and the following disclaimer in the
//    documentation and/or other materials provided with the distribution.
// 3. All advertising materials mentioning features or use of this software
//    must display the following acknowledgement:
//      This product includes software developed by CDS Networks, Inc.
// 4. The name of CDS Networks, Inc.  may not be used to endorse or promote
//    products derived from this software without specific prior
//    written permission.
//
// THIS SOFTWARE IS PROVIDED BY CDS NETWORKS, INC. ``AS IS'' AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED.  IN NO EVENT SHALL CDS NETWORKS, INC. BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
// OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
// HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
// LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
// OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
// SUCH DAMAGE.
//

package net.sourceforge.jtds.jdbc;

import net.sourceforge.jtds.util.HexDump;
import net.sourceforge.jtds.util.Logger;

import java.sql.Timestamp;

/**
 * Handle the communications for a Tds instance.
 *
 * @author     Craig Spannring
 * @author     Igor Petrovski
 * @created    14 September 2001
 * @version    $Id: TdsComm.java,v 1.13 2004-04-04 22:12:03 alin_sinpalean Exp $
 */
public class TdsComm implements TdsDefinitions {

    /**
     * Used to construct the physical packets that will be sent to the database
     * server.
     */
    byte outBuffer[];

    /**
     * Index into <code>outBuffer</code> where the next byte of data will be
     * stored while constructing a packet.
     */
    int nextOutBufferIndex = 0;

    /**
     * The type of the TDS packet that is being constructed in outBuffer.
     */
    volatile int packetType = 0; // MJH - 06/02/04 Made volatile to ensure thread safe

    /**
     * Place to store the incoming data from the DB server.
     */
    byte inBuffer[];

    /**
     * Index of next byte that will be 'read' from <code>inBuffer</code>.
     */
    int inBufferIndex = 0;

    /**
     * Total number of bytes stored in <code>inBuffer</code>. The number includes
     * bytes that have been 'read' as well as bytes that still need to be 'read'.
     */
    int inBufferLen = 0;

    /**
     * Track how many packets we have been sent.
     */
    int packetsSent = 0;
    /**
     * Track how many packets we have been received.
     */
    int packetsReceived = 0;

    /**
     * TDS protocol version. Used to control TDS version-specific behavior.
     */
    private int tdsVer;

    /**
     * Buffer that will be used to return byte[] values by getByte(int, boolean)
     * to avoid allocating a new buffer each time if not necessary.
     */
    byte resBuffer[] = new byte[256];

    public final static String cvsVersion = "$Id: TdsComm.java,v 1.13 2004-04-04 22:12:03 alin_sinpalean Exp $";

    final static int headerLength = 8;

    //
    // The following constants are the packet types.
    //
    // They are the first databayte in the packet and
    // define the type of data in that packet.
    /**
     * TDS packet type QUERY. Regular SQL queries.
     */
    public final static byte QUERY = 1;
    /**
     * TDS 4.2 packet type LOGON. First packet sent by client.
     */
    public final static byte LOGON = 2;
    /**
     * TDS packet type PROC. Remote procedure call request.
     */
    public final static byte PROC = 3;
    /**
     * TDS packet type REPLY. The only packet type sent by the server.
     */
    public final static byte REPLY = 4;
    /**
     * TDS packet type CANCEL. Cancels the currently executing query/procedure
     * call.
     */
    public final static byte CANCEL = 6;
    /**
     * TDS 7.0 packet type LOGON. First packet sent by client.
     */
    public final static byte LOGON70 = 16;
    // Added 2000-06-05

    /**
     * Added by 12 Nov 03 by mdb: sends the response to the ntlm auth challenge
     */
    public final static byte NTLMAUTH = 0x11;

    /**
     * For debuging purposes it would be nice to uniquely identify each TDS
     * stream. <code>id</code> will be a unique value for each instance of this
     * class.
     */
    private static int nextId = 0;

    /**
     * Unique identifier for the TDS stream.
     */
    private int id;

    private TdsSocket tdsSocket = null;

    public TdsComm(TdsSocket tdsSocket, int tdsVer, int networkPacketSize) {
        this.tdsSocket = tdsSocket;

        outBuffer = new byte[networkPacketSize];
        inBuffer = new byte[networkPacketSize];

        // Added 2000-06-07
        this.tdsVer = tdsVer;

        id = ++nextId;
    }

    public void close() {
        // nop for now.
    }

    public int getId() {
        return this.id;
    }

    public int getTdsVer() {
        return this.tdsVer;
    }

    /**
     * Start a TDS packet. <br>
     * This method should be called to start a logical TDS packet.
     *
     * @param  type  Type of the packet. Can be QUERY, LOGON, PROC, REPLY, or
     *               CANCEL.
     */
    public synchronized void startPacket(int type) throws TdsException {
        if (type != CANCEL && inBufferIndex != inBufferLen) {
            // SAfe It's ok to throw this exception so that we will know there
            //      is a design flaw somewhere, but we should empty the buffer
            //      however. Otherwise the connection will never close (e.g. if
            //      SHOWPLAN_ALL is ON, a resultset will be returned by commit
            //      or rollback and we will never get rid of it). It's true
            //      that we should find a way to actually process these packets
            //      but for now, just dump them (we have thrown an exception).
            inBufferIndex = inBufferLen;
            if (Logger.isActive())
                Logger.println("Unprocessed data in input buffer. Dumping. [" +
                               inBufferIndex + "/" + inBufferLen + "]");
            throw new TdsException("Unprocessed data in input buffer.");
        }

        // Only one thread at a time can be building an outbound packet.
        // This is primarily a concern with building cancel packets.
        //  XXX: as why should more than one thread work with the same tds-stream ??? would be fatal anyway
        while (packetType != 0) {
            try {
                wait();
            } catch (java.lang.InterruptedException e) {
                // nop
            }
        }

        packetType = type;
        nextOutBufferIndex = headerLength;
    }

    /**
     * Append a byte onto the end of the logical TDS packet.
     * <p>
     * Append a byte onto the end of the logical TDS packet. When a physical
     * packet is full send it to the server.
     *
     * @param  b                        byte to add to the TDS packet
     * @exception  java.io.IOException  if an I/O error occurs
     */
    public void appendByte(byte b) throws java.io.IOException {
        if (nextOutBufferIndex == outBuffer.length) {
            // If we have a full physical packet then ship it out to the
            // network.
            sendPhysicalPacket(false);
            nextOutBufferIndex = headerLength;
        }

        outBuffer[nextOutBufferIndex++] = b;
    }

    /**
     * Append an array of bytes onto the end of the logical TDS packet.
     *
     * @param  b                        bytes to add to the TDS packet
     * @exception  java.io.IOException  if an I/O error occurs
     */
    public void appendBytes(byte[] b) throws java.io.IOException {
        appendBytes(b, b.length, (byte) 0);
    }

    /**
     * Append an array of bytes onto the end of the logical TDS packet.
     *
     * @param  b                        bytes to add to the TDS packet
     * @param  len                      maximum number of bytes to transmit
     * @param  pad                      fill with this byte until len is reached
     * @exception  java.io.IOException  if an I/O error occurs
     */
    public void appendBytes(byte[] b, int len, byte pad) throws java.io.IOException {
        int i = 0;
        // TODO Use System.arraycopy() instead of moving bytes one by one,
        //      maybe only for larger lengths to justify the overhead.
        for (; i < b.length && i < len; i++) {
            appendByte(b[i]);
        }
        for (; i < len; i++) {
            appendByte(pad);
        }
    }

    /**
     * Append a short int onto the end of the logical TDS packet.
     *
     * @param  s                        short int to add to the TDS packet
     * @exception  java.io.IOException  if an I/O error occurs
     */
    public void appendShort(short s)
            throws java.io.IOException {
        appendByte((byte) ((s >> 8) & 0xff));
        appendByte((byte) ((s >> 0) & 0xff));
    }

    /**
     * Appends a short int onto the end of the logical TDS packet.
     *
     * @param  s                        short int to add to the TDS packet
     * @exception  java.io.IOException  if an I/O error occurs
     */
    public void appendTdsShort(short s)
            throws java.io.IOException {
        appendByte((byte) ((s >> 0) & 0xff));
        appendByte((byte) ((s >> 8) & 0xff));
    }

    /**
     * Append the Double value onto the end of the TDS packet as a SYBFLT8.
     *
     * @param  value                    Double to add to the TDS packet
     * @exception  java.io.IOException  if an I/O error occurs
     */
    public void appendFlt8(Double value)
            throws java.io.IOException {
        long l = Double.doubleToLongBits(value.doubleValue());

        appendByte((byte) ((l >> 0) & 0xff));
        appendByte((byte) ((l >> 8) & 0xff));
        appendByte((byte) ((l >> 16) & 0xff));
        appendByte((byte) ((l >> 24) & 0xff));
        appendByte((byte) ((l >> 32) & 0xff));
        appendByte((byte) ((l >> 40) & 0xff));
        appendByte((byte) ((l >> 48) & 0xff));
        appendByte((byte) ((l >> 56) & 0xff));
    }

    public void appendInt(int i)
            throws java.io.IOException {
        appendByte((byte) ((i >> 24) & 0xff));
        appendByte((byte) ((i >> 16) & 0xff));
        appendByte((byte) ((i >> 8) & 0xff));
        appendByte((byte) ((i >> 0) & 0xff));
    }

    public void appendTdsInt(int i)
            throws java.io.IOException {
        appendByte((byte) ((i >> 0) & 0xff));
        appendByte((byte) ((i >> 8) & 0xff));
        appendByte((byte) ((i >> 16) & 0xff));
        appendByte((byte) ((i >> 24) & 0xff));
    }

    public void appendInt64(long i)
            throws java.io.IOException {
        appendByte((byte) ((i >> 56) & 0xff));
        appendByte((byte) ((i >> 48) & 0xff));
        appendByte((byte) ((i >> 40) & 0xff));
        appendByte((byte) ((i >> 32) & 0xff));
        appendByte((byte) ((i >> 24) & 0xff));
        appendByte((byte) ((i >> 16) & 0xff));
        appendByte((byte) ((i >> 8) & 0xff));
        appendByte((byte) ((i >> 0) & 0xff));
    }

    /**
     * Appends the 16-bit characters from the caller's string, without
     * narrowing the characters. Sybase lets the client decide what byte order
     * to use but it appears that SQLServer 7.0 only uses little-endian byte
     * order.
     *
     * @param  s                        <code>String</code> to append
     * @exception  java.io.IOException  if an I/O error occurs
     */
    public void appendChars(String s) throws java.io.IOException {
        for (int i = 0; i < s.length(); ++i) {
            int c = s.charAt(i);
            byte b1 = (byte) (c & 0xFF);
            byte b2 = (byte) ((c >> 8) & 0xFF);
            appendByte(b1);
            appendByte(b2);
        }
    }

    /**
     * Appends the 16-bit characters from the <code>char</code> array, without
     * narrowing the characters. Sybase lets the client decide what byte order
     * to use but it appears that SQLServer 7.0 only uses little-endian byte
     * order.
     *
     * @param  chars                    <code>char</code> array to append
     * @param  len                      number of characters to append
     * @exception  java.io.IOException  if an I/O error occurs
     */
    public void appendChars(char[] chars, int len) throws java.io.IOException {
        for (int i = 0; i < len; ++i) {
            int c = chars[i];
            byte b1 = (byte) (c & 0xFF);
            byte b2 = (byte) ((c >> 8) & 0xFF);
            appendByte(b1);
            appendByte(b2);
        }
    }

    /**
     * Send the logical packet that has been constructed.
     *
     * @exception  java.io.IOException  if an I/O error occurs
     */
    public synchronized void sendPacket()
            throws java.io.IOException {
        sendPhysicalPacket(true);
        nextOutBufferIndex = headerLength;
        packetType = 0;
        notify();
    }

    /**
     * Store a byte of data at a particular location in <code>outBuffer</code>.
     *
     * @param  index  position in <code>outBuffer</code> to store data
     * @param  value  value to store in the <code>outBuffer</code>
     */
    private void storeByte(int index, byte value) {
        outBuffer[index] = value;
    }

    /**
     * Store a short integer of data at a particular location in the outBuffer.
     *
     * @param  index  position in outBuffer to store data
     * @param  s      <code>short</code> value to store
     */
    private void storeShort(int index, short s) {
        outBuffer[index] = (byte) ((s >> 8) & 0xff);
        outBuffer[index + 1] = (byte) ((s >> 0) & 0xff);
    }

    /**
     * Send the data in the outBuffer.
     * <p>
     * Fill in the TDS packet header data and send the data in outBuffer to the
     * DB server.
     *
     * @param  isLastSegment            is this the last segment that makes up
     *                                  the physical packet?
     * @exception  java.io.IOException  if an I/O error occurs
     */
    private void sendPhysicalPacket(boolean isLastSegment)
            throws java.io.IOException {
        if (nextOutBufferIndex > headerLength
                || packetType == CANCEL) {
            // packet type
            storeByte(0, (byte) (packetType & 0xff));
            storeByte(1, isLastSegment ? (byte) 1 : (byte) 0);
            storeShort(2, (short) nextOutBufferIndex);
            storeShort(4, (short) 0);
            storeByte(6, (byte) (tdsVer == TDS70 ? 1 : 0));
            storeByte(7, (byte) 0);
            //
            // Store the returned buffer as the original output
            // buffer may be cached by TdsSocket
            //
            // SAfe Also log before sending, to make sure we don't log zeros.
            //      Probably should move this to TdsSocket at some point.
            if (Logger.isActive()) {
                String dump = HexDump.hexDump(outBuffer, nextOutBufferIndex);
                String t = (new Timestamp(
                        System.currentTimeMillis())).toString();
                Logger.println("Instance " + id + " @ " + t
                               + " sent packet #" + packetsSent + "\n" + dump);
            }

            outBuffer = tdsSocket.sendNetPacket(this, outBuffer);
            packetsSent++;
        }
    }

    /**
     * Peek at the next byte of data.
     * <p>
     * This returns the next byte of data that would be returned by getByte(),
     * but does not actually consume the data.
     * <p>
     * <b>Note-</b> We can't synchronize this method (or most of the other
     * methods in this class) because of the way cancels are handled. If a
     * thread is waiting for a response from the server the cancelling thread
     * must be able to call <code>sendPacket()</code> to cancel the request.
     *
     * @return                          the next byte of data that will be
     *                                  returned by <code>getByte()</code>
     * @exception  TdsException
     * @exception  java.io.IOException  if an I/O error occurs
     */
    public byte peek()
            throws TdsException, java.io.IOException {
        // If out of data, read another physical packet.
        if (inBufferIndex >= inBufferLen)
            getPhysicalPacket();

        return inBuffer[inBufferIndex];
    }

    /**
     * Read a byte of data from the DB server.
     * <p>
     * <B>Warning</B> If there is not data available this method will block.
     *
     * @return                          the next byte of data from the DB
     *                                  server
     * @exception  TdsException
     * @exception  java.io.IOException  if an I/O error occurs
     */
    public byte getByte()
            throws net.sourceforge.jtds.jdbc.TdsException,
            java.io.IOException {
        if (inBufferIndex >= inBufferLen) {
            // out of data, read another physical packet.
            getPhysicalPacket();
        }

        return inBuffer[inBufferIndex++];
    }

    /**
     * Reads and returns <code>len</code> bytes. If
     * <code>exclusiveBuffer</code> is <code>true</code>, a new buffer of that
     * size will be allocated. Otherwise, {@link #resBuffer} will be used.
     * <p>
     * Note that in the second case the returned buffer will very likely have a
     * larger size than the number of bytes requested.
     */
    public byte[] getBytes(int len, boolean exclusiveBuffer)
            throws net.sourceforge.jtds.jdbc.TdsException, java.io.IOException {
        byte result[];

        // Do not keep an internal result buffer larger than 16k.
        // This would unnecessarily use up memory.
        if (exclusiveBuffer || len > 16384) {
            result = new byte[len];
        } else {
            if (resBuffer.length < len) {
                resBuffer = new byte[len];
            }

            result = resBuffer;
        }

        for (int i = 0; i < len;) {
            if (inBufferIndex >= inBufferLen) {
                getPhysicalPacket();
            }

            int avail = inBufferLen - inBufferIndex;
            avail = avail > len - i ? len - i : avail;

            System.arraycopy(inBuffer, inBufferIndex, result, i, avail);
            i += avail;
            inBufferIndex += avail;
        }

        return result;
    }

    /**
     * Reads bytes or characters (depending on TDS version) and constructs a
     * string with them. Sybase will let the client choose byte ordering, but
     * SQLServer 7.0 wants little endian only. In the interest of simplicity,
     * just use little endian regardless of the type of server.
     *
     * @param  len                      length of the String to read
     * @param  enc                      <code>EncodingHelper</code> instance to
     *                                  use for decoding the String
     * @return                          the String value
     * @exception  TdsException
     * @exception  java.io.IOException  if an I/O error occurs
     */
    public String getString(int len, EncodingHelper enc) throws TdsException, java.io.IOException {
        if (tdsVer == TDS70) {
            char[] chars = new char[len];

            for (int i = 0; i < len; i++) {
                int lo = getByte() & 0xFF;
                int hi = getByte() & 0xFF;
                chars[i] = (char) (lo | (hi << 8));
            }

            return new String(chars);
        } else
            return enc.getString(getBytes(len, false), 0, len);
    }

    public void skip(int i)
            throws net.sourceforge.jtds.jdbc.TdsException,
            java.io.IOException {
        for (; i > 0; i--) {
            getByte();
        }
    }

    public int getNetShort()
            throws TdsException, java.io.IOException {
        byte tmp[] = new byte[2];
        tmp[0] = getByte();
        tmp[1] = getByte();
        return ntohs(tmp, 0);
    }

    public int getTdsShort()
            throws net.sourceforge.jtds.jdbc.TdsException, java.io.IOException {
        int lo = ((int) getByte() & 0xff);
        int hi = ((int) getByte() & 0xff) << 8;
        return lo | hi;
    }

    public int getTdsInt()
            throws net.sourceforge.jtds.jdbc.TdsException, java.io.IOException {
        int b1 = ((int) getByte() & 0xff);
        int b2 = ((int) getByte() & 0xff) << 8;
        int b3 = ((int) getByte() & 0xff) << 16;
        int b4 = ((int) getByte() & 0xff) << 24;

        return b4 | b3 | b2 | b1;
    }

    public long getTdsInt64()
            throws net.sourceforge.jtds.jdbc.TdsException, java.io.IOException {
        long b1 = ((long) getByte() & 0xff);
        long b2 = ((long) getByte() & 0xff) << 8;
        long b3 = ((long) getByte() & 0xff) << 16;
        long b4 = ((long) getByte() & 0xff) << 24;
        long b5 = ((long) getByte() & 0xff) << 32;
        long b6 = ((long) getByte() & 0xff) << 40;
        long b7 = ((long) getByte() & 0xff) << 48;
        long b8 = ((long) getByte() & 0xff) << 56;
        return b1 | b2 | b3 | b4 | b5 | b6 | b7 | b8;
    }

    /**
     * Read a physical packet.
     * <p>
     * <B>Warning</B> This method will block until it gets all of the input.
     * <p>
     * <b>Note:</b> This method should not be synchronized (at least not on
     * <code>this</code> object) because it would prevent us from sending
     * CANCEL packets, which are sent asynchronously.
     *
     * @exception  TdsException
     * @exception  java.io.IOException
     */
    private void getPhysicalPacket()
            throws TdsException, java.io.IOException {
        inBuffer = tdsSocket.getNetPacket(this, inBuffer);
        packetsReceived++;

        // adjust the bookkeeping info about the incoming buffer
        inBufferLen = ntohs(inBuffer, 2);
        inBufferIndex = 8;

        if (Logger.isActive()) {
            String dump = net.sourceforge.jtds.util.HexDump.hexDump(inBuffer, ntohs(inBuffer, 2));
            String t = (new Timestamp(
                    System.currentTimeMillis())).toString();

            Logger.println("Instance " + id + " @ " + t
                           + " received data #" + (packetsReceived)
                           + "\n" + dump);
        }
    }

    void resizeOutbuf(int newsize) {
        if (newsize > outBuffer.length) {
            byte[] newBuf = new byte[newsize];
            System.arraycopy(outBuffer, 0, newBuf, 0, outBuffer.length);
            outBuffer = newBuf;
        }
    }

    /**
     * Convert two bytes (in network byte order) in a byte array into a Java
     * short integer.
     *
     * @param  buf     array of data
     * @param  offset  index into the buf array where the short integer is
     *      stored
     * @return         the 16 bit unsigned value as an <code>int</code>
     */
    public static int ntohs(byte buf[], int offset) {
        int lo = ((int) buf[offset + 1] & 0xff);
        int hi = (((int) buf[offset] & 0xff) << 8);

        return hi | lo;
        // return an int since we really want an _unsigned_
    }
}
