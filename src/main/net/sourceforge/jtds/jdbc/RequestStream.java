// jTDS JDBC Driver for Microsoft SQL Server
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
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import net.sourceforge.jtds.util.*;

/**
 * Class to implement an output stream for the server request.
 * <p>
 * Implementation note:
 * <ol>
 * <li>This class contains methods to write different types of data to the
 *     server request stream in TDS format.
 * <li>Character translation of String items is carried out.
 * </ol>
 *
 * @author Mike Hutchinson.
 * @version $Id: RequestStream.java,v 1.6 2004-07-30 01:05:20 ddkilzer Exp $
 */
public class RequestStream {
    /** The shared network socket. */
    private SharedSocket socket;
    /** The output packet buffer. */
    private byte[] buffer;
    /** The offset of the next byte to write. */
    private int bufferPtr;
    /** The request packet type. */
    private byte pktType;
    /** The unique stream id. */
    private int streamId;
    /** True if stream is closed. */
    private boolean isClosed = false;
    /** The current output buffer size*/
    private int bufferSize = TdsCore.MIN_PKT_SIZE;
    /** The TDS version in use. */
    private int tdsVersion;
    /** The type of server (MS SQL/Sybase). */
    private int serverType;
    /** The maximum decimal precision. */
    private int maxPrecision;

    /**
     * Construct a RequestStream object.
     *
     * @param socket The shared socket object to write to.
     * @param streamId The unique id for this stream (from ResponseStream).
     */
    RequestStream(SharedSocket socket, int streamId) {
        this.streamId = streamId;
        this.socket = socket;
        this.buffer = new byte[bufferSize];
        this.bufferPtr = TdsCore.PKT_HDR_LEN;
        this.tdsVersion = socket.getTdsVersion();
        this.serverType = socket.getServerType();
    }

    /**
     * Set the output buffer size
     *
     * @param size The new buffer size (>= {@link TdsCore.MIN_PKT_SIZE} <= {@link TdsCore.MAX_PKT_SIZE}).
     */
    void setBufferSize(int size) {
        if (size < bufferPtr || size == bufferSize) {
            return; // Can't shrink buffer size;
        }

        if (size < TdsCore.MIN_PKT_SIZE || size > TdsCore.MAX_PKT_SIZE) {
            throw new IllegalArgumentException("Invalid buffer size parameter " + size);
        }

        byte[] tmp = new byte[size];
        System.arraycopy(buffer, 0, tmp, 0, bufferPtr);
        buffer = tmp;
    }

    /**
     * Retrieve the current output packet size.
     *
     * @return the packet size as an <code>int</code>.
     */
    int getBufferSize() {
        return bufferSize;
    }

    /**
     * Retrive the maximum decimal precision.
     *
     * @return The precision as an <code>int</code>.
     */
    int getMaxPrecision() {
        return this.maxPrecision;
    }

    /**
     * Set the maximum decimal precision.
     *
     * @param precision The precision either 28 or 38.
     */
    void setMaxPrecision(int precision) {
        this.maxPrecision = precision;
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
     * Set the current output packet type.
     *
     * @param pktType The packet type eg TdsCore.QUERY_PKT.
     */
    void setPacketType(byte pktType) {
        this.pktType = pktType;
    }

    /**
     * Write a byte to the output stream.
     *
     * @param b The byte value to write.
     * @throws IOException
     */
    void write(byte b) throws IOException {
        if (bufferPtr == buffer.length) {
            putPacket(0);
        }

        buffer[bufferPtr++] = b;
    }

    /**
     * Write an array of bytes to the output stream.
     *
     * @param b The byte array to write.
     * @throws IOException
     */
    void write(byte[] b) throws IOException {
        int bytesToWrite = b.length;
        int off = 0;

        while (bytesToWrite > 0) {
            int available = buffer.length - bufferPtr;

            if (available == 0) {
                putPacket(0);
                continue;
            }

            int bc = (available > bytesToWrite) ? bytesToWrite : available;
            System.arraycopy(b, off, buffer, bufferPtr, bc);
            off += bc;
            bufferPtr += bc;
            bytesToWrite -= bc;
        }
    }

    /**
     * Write a partial byte buffer to the output stream.
     *
     * @param b The byte array buffer.
     * @param off The offset into the byte array.
     * @param len The number of bytes to write.
     * @throws IOException
     */
    void write(byte[] b, int off, int len) throws IOException {
        int limit = (off + len) > b.length? b.length: off + len;
        int bytesToWrite = limit - off;
        int i = len - bytesToWrite;

        while (bytesToWrite > 0) {
            int available = buffer.length - bufferPtr;

            if (available == 0) {
                putPacket(0);
                continue;
            }

            int bc = (available > bytesToWrite)? bytesToWrite: available;
            System.arraycopy(b, off, buffer, bufferPtr, bc);
            off += bc;
            bufferPtr += bc;
            bytesToWrite -= bc;
        }

        for (; i > 0; i--) {
            write((byte) 0);
        }
    }

    /**
     * Write an int value to the output stream.
     *
     * @param i The int value to write.
     * @throws IOException
     */
    void write(int i) throws IOException {
        write((byte) i);
        write((byte) (i >> 8));
        write((byte) (i >> 16));
        write((byte) (i >> 24));
    }

    /**
     * Write a short value to the output stream.
     *
     * @param s The short value to write.
     * @throws IOException
     */
    void write(short s) throws IOException {
        write((byte) s);
        write((byte) (s >> 8));
    }

    /**
     * Write a long value to the output stream.
     *
     * @param l The long value to write.
     * @throws IOException
     */
    void write(long l) throws IOException {
        write((byte) l);
        write((byte) (l >> 8));
        write((byte) (l >> 16));
        write((byte) (l >> 24));
        write((byte) (l >> 32));
        write((byte) (l >> 40));
        write((byte) (l >> 48));
        write((byte) (l >> 56));
    }

    /**
     * Write a double value to the output stream.
     *
     * @param f The double value to write.
     * @throws IOException
     */
    void write(double f) throws IOException {
        long l = Double.doubleToLongBits(f);

        write((byte) l);
        write((byte) (l >> 8));
        write((byte) (l >> 16));
        write((byte) (l >> 24));
        write((byte) (l >> 32));
        write((byte) (l >> 40));
        write((byte) (l >> 48));
        write((byte) (l >> 56));
    }

    /**
     * Write a float value to the output stream.
     *
     * @param f The float value to write.
     * @throws IOException
     */
    void write(float f) throws IOException {
        int l = Float.floatToIntBits(f);

        write((byte) l);
        write((byte) (l >> 8));
        write((byte) (l >> 16));
        write((byte) (l >> 24));
    }

    /**
     * Write a String object to the output stream.
     * If the TDS version is >= 7.0 write a UNICODE string otherwise
     * wrote a translated byte stream.
     *
     * @param s The String to write.
     * @throws IOException
     */
    void write(String s) throws IOException {
        if (tdsVersion >= Driver.TDS70) {
            int len = s.length();

            for (int i = 0; i < len; ++i) {
                int c = s.charAt(i);

                if (bufferPtr == buffer.length) {
                    putPacket(0);
                }

                buffer[bufferPtr++] = (byte) c;

                if (bufferPtr == buffer.length) {
                    putPacket(0);
                }

                buffer[bufferPtr++] = (byte) (c >> 8);
            }
        } else {
            writeAscii(s);
        }
    }

    /**
     * Write a char array object to the output stream.
     *
     * @param s The char[] to write.
     * @throws IOException
     */
    void write(char s[], int off, int len) throws IOException {
        int i = off;
        int limit = (off + len) > s.length ? s.length : off + len;

        for ( ; i < limit; i++) {
            char c = s[i];

            if (bufferPtr == buffer.length) {
                putPacket(0);
            }

            buffer[bufferPtr++] = (byte) c;

            if (bufferPtr == buffer.length) {
                putPacket(0);
            }

            buffer[bufferPtr++] = (byte) (c >> 8);
        }
    }

    /**
     * Write a String to the output stream as translated bytes.
     *
     * @param s The String to write.
     * @throws IOException
     */
    void writeAscii(String s) throws IOException {
        String charsetName = socket.getCharset();

        if (charsetName != null) {
            try {
                write(s.getBytes(charsetName));
            } catch (UnsupportedEncodingException e) {
                write(s.getBytes());
            }
        } else {
            write(s.getBytes());
        }
    }

    /**
     * Copy the contents of an InputStream to the server.
     *
     * @param in The InputStream to read.
     * @param length The length of the stream.
     * @throws IOException
     */
    void writeStreamBytes(InputStream in, int length) throws IOException {
        while (length-- > 0) {
            int b = in.read();

            if (b < 0) {
                throw new java.io.IOException(
                                             "Data in stream less than specified by length");
            }

            write((byte) b);
        }

        if (in.read() >= 0) {
            throw new java.io.IOException(
                                         "More data in stream than specified by length");
        }
    }

    /**
     * Copy the contents of a Reader stream to the server.
     *
     * @param in The Reader object with the data.
     * @param length The length of the data in characters.
     * @throws IOException
     */
    void writeReaderChars(Reader in, int length) throws IOException {
        while (length-- > 0) {
            int b = in.read();

            if (b < 0) {
                throw new java.io.IOException(
                                             "Data in stream less than specified by length");
            }

            write((byte) b);
            write((byte) (b >> 8));
        }

        if (in.read() >= 0) {
            throw new java.io.IOException(
                                         "More data in stream than specified by length");
        }
    }

    /**
     * Copy the contents of a Reader stream to the server as bytes.
     * <p>
     * NB. Only reliable where the charset is single byte.
     *
     * @param in The Reader object with the data.
     * @param length The length of the data in bytes.
     * @throws IOException
     */
    void writeReaderBytes(Reader in, int length) throws IOException {
        char buffer[] = new char[1024];

        for (int i = 0; i < length; i += buffer.length) {
            int result = in.read(buffer);

            if (result == -1 && i < length) {
                throw new java.io.IOException(
                                             "Data in stream less than specified by length");
            } else if (i + result > length) {
                throw new java.io.IOException(
                                             "More data in stream than specified by length");
            }

            write(Support.encodeString(socket.getCharset(), new String(buffer, 0, result)));
        }
    }

    /**
     * Write a BigDecimal value to the output stream.
     *
     * @param value The BigDecimal value to write.
     * @throws IOException
     */
    void write(BigDecimal value) throws IOException {
        final byte prec = (byte)maxPrecision;
        final byte maxLen = (prec <= 28) ? (byte) 13 : (byte) 17;

        if (value == null) {
            write((byte) 0);
        } else {
            byte signum = (byte) (value.signum() < 0 ? 0 : 1);
            BigInteger bi = value.unscaledValue();
            byte mantisse[] = bi.abs().toByteArray();
            byte len = (byte) (mantisse.length + 1);

            if (len > maxLen) {
                // Should never happen now as value is normalized elsewhere
                throw new IOException("BigDecimal to big to send");
            }

            if (serverType == Driver.SYBASE) {
                write((byte)len);
                // Sybase TDS5 stores MSB first opposite sign!
                // length, prec, scale already sent in parameter descriptor.
                write((byte) ((signum == 0)? 1: 0));

                for (int i = 0; i < mantisse.length; i++) {
                    write((byte) mantisse[i]);
                }
            } else {
                write((byte) len);
                write((byte) signum);

                for (int i = mantisse.length - 1; i >= 0; i--) {
                    write((byte) mantisse[i]);
                }
            }
        }
    }

    /**
     * Flush the packet to the output stream setting the last packet flag.
     *
     * @throws IOException
     */
    void flush() throws IOException {
        putPacket(1);
    }

    /**
     * Close the output stream.
     *
     * @throws IOException
     */
    void close() throws IOException {
        isClosed = true;
    }

    /**
     * Retrieve the TDS version number.
     *
     * @return The TDS version as an <code>int</code>.
     */
    int getTdsVersion() {
        return this.tdsVersion;
    }

    /**
     * Retrieve the Server type.
     *
     * @return The Server type as an <code>int</code>.
     */
    int getServerType() {
        return this.serverType;
    }

    /**
     * Write the TDS packet to the network.
     *
     * @param last Set to 1 if this is the last packet else 0.
     * @throws IOException
     */
    private void putPacket(int last) throws IOException {
        if (isClosed) {
            throw new IOException("RequestStream is closed");
        }

        buffer[0] = pktType;
        buffer[1] = (byte) last; // last segment indicator
        buffer[2] = (byte) (bufferPtr >> 8);
        buffer[3] = (byte) bufferPtr;
        buffer[4] = 0;
        buffer[5] = 0;
        buffer[6] = (byte) ((tdsVersion >= Driver.TDS70) ? 1 : 0);
        buffer[7] = 0;

        if (Logger.isActive()) {
            Logger.logPacket(streamId, false, buffer);
        }

        buffer = socket.sendNetPacket(streamId, buffer);
        bufferPtr = TdsCore.PKT_HDR_LEN;
    }
}
