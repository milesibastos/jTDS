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
package net.sourceforge.jtds.jdbc;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import net.sourceforge.jtds.util.*;

/**
 * Class to implement an input stream for the server response.
 * <p>
 * Implementation note:
 * <ol>
 * <li>This class contains methods to read different types of data from the
 *     server response stream in TDS format.
 * <li>Character translation of String items is carried out.
 * </ol>
 *
 * @author Mike Hutchinson.
 * @version $Id: ResponseStream.java,v 1.14 2005-01-06 15:45:06 alin_sinpalean Exp $
 */
public class ResponseStream {
    /** The shared network socket. */
    private SharedSocket socket;
    /** The Input packet buffer. */
    private byte[] buffer;
    /** The offset of the next byte to read. */
    private int bufferPtr;
    /** The length of current input packet. */
    private int bufferLen;
    /** The unique stream id. */
    private int streamId;
    /** True if stream is closed. */
    private boolean isClosed = false;
    /** A shared byte buffer. */
    private byte[] byteBuffer = new byte[255];
    /** A shared char buffer. */
    private char[] charBuffer = new char[255];

    /**
     * Construct a RequestStream object.
     *
     * @param socket The shared socket object to write to.
     * @param streamId The unique id for this stream (from ResponseStream).
     */
    ResponseStream(SharedSocket socket, int streamId){
        this.streamId = streamId;
        this.socket = socket;
        this.bufferLen = TdsCore.MIN_PKT_SIZE;
        this.buffer = new byte[bufferLen];
        this.bufferPtr = bufferLen;
    }

    /**
     * Retrieve the unique stream id.
     *
     * @return the unique stream id as an <code>int</code>.
     */
    int getStreamId() {
        return this.streamId;
    }

    /**
     * Retrieve the next input byte without reading forward.
     *
     * @return The next byte in the input stream as an <code>int</code>.
     * @throws IOException
     */
    int peek() throws IOException {
        int b = read();

        bufferPtr--; // Backup one

        return b;
    }

    /**
     * Retrieve the next input byte from the server response stream.
     *
     * @return The next byte in the input stream as an <code>int</code>.
     * @throws IOException
     */
    int read() throws IOException {
        if (bufferPtr >= bufferLen) {
            getPacket();
        }

        return (int) buffer[bufferPtr++] & 0xFF;
    }

    /**
     * Retrieve a byte array from the server response stream.
     *
     * @param b The byte array.
     * @return The number of bytes read as an <code>int</code>.
     * @throws IOException
     */
    int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    /**
     * Retrieve a byte array from the server response stream, specifying
     * a start offset and length.
     *
     * @param b The byte array.
     * @param off The starting offset in the array.
     * @param len The number of bytes to read.
     * @return The number of bytes read as an <code>int</code>.
     * @throws IOException
     */
    int read(byte[] b, int off, int len) throws IOException {
        int bytesToRead = len;

        while (bytesToRead > 0) {
            if (bufferPtr >= bufferLen) {
                getPacket();
            }

            int available = bufferLen - bufferPtr;
            int bc = (available > bytesToRead) ? bytesToRead : available;

            System.arraycopy(buffer, bufferPtr, b, off, bc);
            off += bc;
            bytesToRead -= bc;
            bufferPtr += bc;
        }

        return len;
    }

    /**
     * Retrieve a char array from the server response stream.
     *
     * @param c The char array.
     * @return The byte array as a <code>byte[]</code>.
     * @throws IOException
     */
    int read(char[] c) throws IOException {
        for (int i = 0; i < c.length; i++) {
            if (bufferPtr >= bufferLen) {
                getPacket();
            }

            int b1 = buffer[bufferPtr++] & 0xFF;

            if (bufferPtr >= bufferLen) {
                getPacket();
            }

            int b2 = buffer[bufferPtr++] << 8;

            c[i] = (char) (b2 | b1);
        }

        return c.length;
    }

    /**
     * Retrieve a String object from the server response stream.
     * If the TDS protocol is 4.2 or 5.0 decode the string using the default
     * server charset, otherwise use UCS2-LE (Unicode).
     *
     * @param len the length of the string to read <b>in bytes</b> in the case
     *            of TDS 4.2/5.0 and <b>in characters</b> for TDS 7.0+
     *            (UCS2-LE encoded strings)
     * @return the result as a <code>String</code>
     * @throws IOException if an I/O error occurs
     */
    String readString(int len) throws IOException {
        if (socket.getTdsVersion() >= Driver.TDS70) {
            return readUnicodeString(len);
        }

        return readNonUnicodeString(len);
    }

    /**
     * Skip a string from the server response stream. If the TDS protocol is
     * 4.2 or 5.0 <code>len</code> is the length in bytes, otherwise it's the
     * length in UCS2-LE characters (length in bytes == 2 * <code>len</code>).
     *
     * @param len the length of the string to skip <b>in bytes</b> in the case
     *            of TDS 4.2/5.0 and <b>in characters</b> for TDS 7.0+
     *            (UCS2-LE encoded strings)
     * @throws IOException if an I/O error occurs
     */
    void skipString(int len) throws IOException {
        if (len <= 0) {
            return;
        }

        if (socket.getTdsVersion() >= Driver.TDS70) {
            skip(len * 2);
        } else {
            skip(len);
        }
    }

    /**
     * Retrieve a UCS2-LE (Unicode) encoded String object from the server
     * response stream.
     *
     * @param len the length of the string to read <b>in characters</b>
     * @return the result as a <code>String</code>
     * @throws IOException if an I/O error occurs
     */
    String readUnicodeString(int len) throws IOException {
        char[] chars = (len > charBuffer.length) ? new char[len] : charBuffer;

        for (int i = 0; i < len; i++) {
            if (bufferPtr >= bufferLen) {
                getPacket();
            }

            int b1 = buffer[bufferPtr++] & 0xFF;

            if (bufferPtr >= bufferLen) {
                getPacket();
            }

            int b2 = buffer[bufferPtr++] << 8;

            chars[i] = (char) (b2 | b1);
        }

        return new String(chars, 0, len);
    }

    /**
     * Retrieve a String object from the server response stream,
     * creating the string from a translated byte array.
     *
     * @param len the length of the string to read <b>in bytes</b>
     * @return the result as a <code>String</code>
     * @throws IOException if an I/O error occurs
     */
    String readNonUnicodeString(int len) throws IOException {
        CharsetInfo info = socket.getCharsetInfo();

        return readString(len, info);
    }

    /**
     * Retrieve a String object from the server response stream, translating it
     * from a byte array using the specified character set.
     *
     * @param len the length of the string to read <b>in bytes</b>
     * @return the result as a <code>String</code>
     * @throws IOException if an I/O error occurs
     */
    String readNonUnicodeString(int len, CharsetInfo charsetInfo)
            throws IOException {
        return readString(len, charsetInfo);
    }

    /**
     * Retrieve a String object from the server response stream,
     * creating the string from a translated byte array.
     *
     * @param len  the length of the string to read <b>in bytes</b>
     * @param info descriptor of the charset to use
     * @return the result as a <code>String</code>
     * @throws IOException if an I/O error occurs
     */
    String readString(int len, CharsetInfo info) throws IOException {
        String charsetName = info.getCharset();
        byte[] bytes = (len > byteBuffer.length) ? new byte[len] : byteBuffer;

        read(bytes, 0, len);

        try {
            return new String(bytes, 0, len, charsetName);
        } catch (UnsupportedEncodingException e) {
            return new String(bytes, 0, len);
        }
    }

    /**
     * Retrieve a short value from the server response stream.
     *
     * @return The result as a <code>short</code>.
     * @throws IOException
     */
    short readShort() throws IOException {
        int b1 = read();

        return (short) (b1 | (read() << 8));
    }

    /**
     * Retrieve an int value from the server response stream.
     *
     * @return The result as a <code>int</code>.
     * @throws IOException
     */
    int readInt() throws IOException {
        int b1 = read();
        int b2 = read() << 8;
        int b3 = read() << 16;
        int b4 = read() << 24;

        return b4 | b3 | b2 | b1;
    }

    /**
     * Retrieve a long value from the server response stream.
     *
     * @return The result as a <code>long</code>.
     * @throws IOException
     */
    long readLong() throws IOException {
        long b1 = ((long) read());
        long b2 = ((long) read()) << 8;
        long b3 = ((long) read()) << 16;
        long b4 = ((long) read()) << 24;
        long b5 = ((long) read()) << 32;
        long b6 = ((long) read()) << 40;
        long b7 = ((long) read()) << 48;
        long b8 = ((long) read()) << 56;

        return b1 | b2 | b3 | b4 | b5 | b6 | b7 | b8;
    }

    /**
     * Discard bytes from the server response stream.
     *
     * @param skip The number of bytes to discard.
     * @return The skip parameter as an <code>int</code>.
     */
    int skip(int skip) throws IOException {
        int tmp = skip;

        while (skip > 0) {
            if (bufferPtr >= bufferLen) {
                getPacket();
            }

            int available = bufferLen - bufferPtr;

            if (skip > available) {
                skip -= available;
                bufferPtr = bufferLen;
            } else {
                bufferPtr += skip;
                skip = 0;
            }
        }

        return tmp;
    }

    /**
     * Close this response stream. The stream id is unlinked from the
     * underlying shared socket as well.
     */
    void close() {
        isClosed = true;
        socket.closeStream(streamId);
    }

    /**
     * Retrieve the TDS version number.
     *
     * @return The TDS version as an <code>int</code>.
     */
    int getTdsVersion() {
        return socket.getTdsVersion();
    }

    /**
     * Retrieve the Server type.
     *
     * @return The Server type as an <code>int</code>.
     */
    int getServerType() {
        return socket.getServerType();
    }

    /**
     * Read the next TDS packet from the network.
     *
     * @throws IOException
     */
    private void getPacket() throws IOException {
        while (bufferPtr >= bufferLen) {
            if (isClosed) {
                throw new IOException("ResponseStream is closed");
            }

            buffer = socket.getNetPacket(streamId, buffer);
            bufferLen = (((int) buffer[2] & 0xFF) << 8) | ((int) buffer[3] & 0xFF);
            bufferPtr = TdsCore.PKT_HDR_LEN;

            if (Logger.isActive()) {
                Logger.logPacket(streamId, true, buffer);
            }
        }
    }
}
