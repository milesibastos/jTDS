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

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.LinkedList;

import net.sourceforge.jtds.util.*;

/**
 * This class mananges the physical connection to the SQL Server and
 * serialises its use amongst a number of virtual sockets.
 * This allows one physical connection to service a number of concurrent
 * statements.
 * <p>
 * Constraints and assumptions:
 * <ol>
 * <li>Callers will not attempt to read from the server without issuing a request first.
 * <li>The end of a server reply can be identified as byte 2 of the header is non zero.
 * </ol>
 * </p>
 * Comments:
 * <ol>
 * <li>This code will discard unread server data if a new request is issued.
 *    Currently the higher levels of the driver attempt to do this but may be
 *    we can just rely on this code instead.
 * <li>A cancel can be issued by a caller only if the server is currently sending
 *    data for the caller otherwise the cancel is ignored.
 * <li>Query timeouts are implemented via socket timeout on the first byte of a packet.
 *    A cancel is issued in this case to terminate the reply and an exception issued
 *    to the caller but only after the input stream has be entirely read.
 * <li>Stray cancel packets on their own are filtered out so that the higher levels
 *    do not need to.
 * </ol>
 * This version of the class will start to cache results to disk once a predetermined
 * maximum buffer memory threshold has been passed. Small result sets that will fit
 * within a specified limit (default 8 packets) will continue to be held in memory
 * (even if the memory threshold has been passed) in the interests of efficiency.
 *
 * @author Mike Hutchinson.
 * @version $Id: SharedSocket.java,v 1.13 2004-09-10 16:22:53 alin_sinpalean Exp $
 */
class SharedSocket {
    /**
     * This inner class contains the state information for the virtual socket.
     */
    private static class VirtualSocket {
        /**
         * The stream ID of the stream objects owning this state.
         */
        private int owner;
        /**
         * Query timeout value in msec or 0.
         */
        private int timeOut;
        /**
         * Memory resident packet queue.
         */
        private LinkedList pktQueue;
        /**
         * True to discard network data.
         */
        private boolean flushInput;
        /**
         * True if output is complete TDS packet.
         */
        private boolean complete;
        /**
         * File object for disk packet queue.
         */
        private File queueFile;
        /**
         * I/O Stream for disk packet queue.
         */
        private RandomAccessFile diskQueue;
        /**
         * Number of packets cached to disk.
         */
        private int pktsOnDisk;
        /**
         * Total of input packets in memory or disk.
         */
        private int inputPkts;
        /**
         * Total of output packets in memory or disk.
         */
        private int outputPkts;

        /**
         * Constuct object to hold state information for each caller.
         * @param streamId the Response/Request stream id.
         */
        VirtualSocket(int streamId) {
            this.owner = streamId;
            this.pktQueue = new LinkedList();
            this.flushInput = false;
            this.complete = false;
            this.timeOut = 0;
            this.queueFile = null;
            this.diskQueue = null;
            this.pktsOnDisk = 0;
            this.inputPkts = 0;
            this.outputPkts = 0;
        }
    }

    /**
     * The shared network socket.
     */
    private Socket socket = null;
    /**
     * Output stream for network socket.
     */
    private DataOutputStream out = null;
    /**
     * Input stream for network socket.
     */
    private DataInputStream in = null;
    /**
     * Current maxium input buffer size.
     */
    private int maxBufSize = TdsCore.MIN_PKT_SIZE;
    /**
     * Table of stream objects sharing this socket.
     */
    private ArrayList socketTable = new ArrayList();
    /**
     * The Stream ID of the object that is expecting a response from the server.
     */
    private int responseOwner = -1;
    /**
     * The Stream ID of the object that is currently building a send packet.
     */
    private int currentSender = -1;
    /**
     * Buffer for packet header.
     */
    private byte hdrBuf[] = new byte[8];
    /**
     * Total memory usage in all instances of the driver
     * NB. Access to this field should probably be synchronized
     * but in practice lost updates will not matter much and I think
     * all VMs tend to do atomic saves to integer variables.
     */
    private static int globalMemUsage = 0;
    /**
     * Peak memory usage for debug purposes.
     */
    private static int peakMemUsage = 0;
    /**
     * Max memory limit to use for buffers.
     * Only when this limit is exceeded will the driver
     * start caching to disk.
     */
    private static int memoryBudget = 100000; // 100K
    /**
     * Minimum number of packets that will be cached in memory
     * before the driver tries to write to disk even if
     * memoryBudget has been exceeded.
     */
    private static int minMemPkts = 8;
    /**
     * Global flag to indicate that security constraints mean
     * that attempts to create work files will fail.
     */
    private static boolean securityViolation = false;
    /**
     * Tds protocol version
     */
    private int tdsVersion;
    /**
     * Actual TDS protocol version, as returned by the server, including minor
     * version (e.g. 0x71000001).
     *
     * @todo Consolidate this and tdsVersion
     */
    private int internalTdsVersion;
    /**
     * The servertype one of Driver.SQLSERVER or Driver.SYBASE
     */
    private int serverType;
    /**
     * The character set to use for converting strings to/from bytes
     */
    private String charsetName;
    /**
     * True if this is a multi byte charset.
     */
    private boolean wideChars;
    /**
     * Count of packets received.
     */
    private int packetCount = 0;

    /**
     * TDS done token.
     */
    private static final byte TDS_DONE_TOKEN = (byte) 253;

    protected SharedSocket() {
    }

    /**
     * Construct a TdsSocket object specifying host name and port.
     *
     * @param host The SQL Server host name.
     * @param port The connection port eg 1433.
     * @param tdsVersion The TDS protocol version
     * @throws IOException If Socket open fails.
     */
    SharedSocket(String host, int port, int tdsVersion, int serverType)
            throws IOException, UnknownHostException {
        setTdsVersion(tdsVersion);
        setServerType(serverType);
        this.socket = new Socket(host, port);
        setOut(new DataOutputStream(socket.getOutputStream()));
        setIn(new DataInputStream(socket.getInputStream()));
        this.socket.setTcpNoDelay(true);
    }

    /**
     * Set the character set name to be used to translate byte arrays to
     * or from Strings.
     *
     * @param charsetName The character set name.
     */
    void setCharset(String charsetName) {
        this.charsetName = charsetName;
    }

    /**
     * Retrieve the character set name used to translate byte arrays to
     * or from Strings.
     *
     * @return The character set name as a <code>String</code>.
     */
    String getCharset() {
        return this.charsetName;
    }

    /**
     * Set the character set width.
     *
     * @param value True if multi byte character set.
     */
    void setWideChars(boolean value) {
        this.wideChars = value;
    }

    /**
     * Retrieve the character width.
     *
     * @return <code>boolean</code> true if this is a multi byte character set.
     */
    boolean isWideChars() {
        return this.wideChars;
    }

    /**
     * Obtain an instance of a server request stream for this socket.
     *
     * @return The server request stream as a <code>RequestStream</code>.
     */
    RequestStream getRequestStream() {
        synchronized (socketTable) {
            int id;
            for (id = 0; id < socketTable.size(); id++) {
                if (socketTable.get(id) == null) {
                    break;
                }
            }

            VirtualSocket vsock = new VirtualSocket(id);

            if (id >= socketTable.size()) {
                socketTable.add(vsock);
            } else {
                socketTable.set(id, vsock);
            }

            return new RequestStream(this, id);
        }
    }

    /**
     * Obtain an instance of a server response stream for this socket.
     * NB. getRequestStream() must be used first to obtain the RequestStream
     * needed as a parameter for this method.
     *
     * @param requestStream An existing server request stream object.
     * @return The server response stream as a <code>ResponseStream</code>.
     */
    ResponseStream getResponseStream(RequestStream requestStream) {
        return new ResponseStream(this, requestStream.getStreamId());
    }

    /**
     * Retrieve the TDS version that is active on the connection
     * supported by this socket.
     *
     * @return The TDS version as an <code>int</code>.
     */
    int getTdsVersion() {
        return tdsVersion;
    }

    /**
     * Set the TDS version field.
     * @param tdsVersion The TDS version as an <code>int</code>.
     */
    protected void setTdsVersion(int tdsVersion) {
        this.tdsVersion = tdsVersion;
    }

    /**
     * Retrieve the complete TDS version ({@see #internalTdsVersion}) that is
     * active on the connection * supported by this socket.
     *
     * @return The TDS version as an <code>int</code>.
     */
    int getInternalTdsVersion() {
        return internalTdsVersion;
    }

    /**
     * Set the complete TDS version ({@see #internalTdsVersion}) field.
     *
     * @param internalTdsVersion The TDS version as returned by the server.
     */
    protected void setInternalTdsVersion(int internalTdsVersion) {
        this.internalTdsVersion = internalTdsVersion;
    }

    /**
     * Retrieve the SQL Server type that is associated with the connection
     * supported by this socket.
     * <ol>
     * <li>Microsoft SQL Server.
     * <li>Sybase SQL Server.
     * </ol>
     * @return The SQL Server type as an <code>int</code>.
     */
    int getServerType() {
        return serverType;
    }

    /**
     * Set the SQL Server type field.
     *
     * @param serverType The SQL Server type as an <code>int</code>.
     */
    protected void setServerType(int serverType) {
        this.serverType = serverType;
    }

    /**
     * Set the global buffer memory limit for all instances of this driver.
     *
     * @param memoryBudget The global memory budget.
     */
    static void setMemoryBudget(int memoryBudget) {
        SharedSocket.memoryBudget = memoryBudget;
    }

    /**
     * Get the global buffer memory limit for all instancs of this driver.
     *
     * @return The memory limit as an <code>int</code>.
     */
    static int getMemoryBudget() {
        return SharedSocket.memoryBudget;
    }

    /**
     * Set the minimum number of packets to cache in memory before
     * writing to disk.
     *
     * @param minMemPkts The minimum number of packets to cache.
     */
    static void setMinMemPkts(int minMemPkts) {
        SharedSocket.minMemPkts = minMemPkts;
    }

    /**
     * Get the minimum number of memory cached packets.
     *
     * @return Minimum memory packets as an <code>int</code>.
     */
    static int getMinMemPkts() {
        return SharedSocket.minMemPkts;
    }

    /**
     * Set the Socket time out in msec.
     *
     * @param streamId The ResponseStream associated with this timeout.
     * @param timeOut The time  out value in milliseconds.
     */
    void setSoTimeout(int streamId, int timeOut) {
        VirtualSocket vsock = lookup(streamId);

        vsock.timeOut = timeOut;
    }

    /**
     * Get the Socket time out value.
     *
     * @param streamId The ResponseStream associated with this timeout.
     * @return The <code>int</code> value of the timeout.
     */
    int getSoTimeout(int streamId) {
        VirtualSocket vsock = lookup(streamId);

        return vsock.timeOut;
    }

    /**
     * Get the connected status of this socket.
     *
     * @return True if the underlying socket is connected.
     */
    boolean isConnected() {
        return this.socket != null;
    }

    /**
     * Send a TDS cancel packet to the server.
     *
     * @param streamId The RequestStream id.
     */
    void cancel(int streamId) {
        //
        // Only send if response pending for the caller
        //
        synchronized (socketTable) {
            if (responseOwner != -1
                && responseOwner == streamId
				&& currentSender == -1) {
                try {
                    sendCancel(streamId);
                } catch (IOException e) {
                    // Ignore error as network is probably dead anyway
                }
            }
        }
    }

    /**
     * Close the socket (noop if in shared mode)
     *
     * @throws IOException
     */
    void close() throws IOException {
        if (Logger.isActive()) {
            Logger.println("TdsSocket: Max buffer memory used = " + peakMemUsage + "KB");
        }

        synchronized (socketTable) {
            // See if any temporary files need deleting
            for (int i = 0; i < socketTable.size(); i++) {
                VirtualSocket vsock = (VirtualSocket) socketTable.get(i);

                if (vsock != null && vsock.diskQueue != null) {
                    try {
                        vsock.diskQueue.close();
                        vsock.queueFile.delete();
                    } catch (IOException ioe) {
                        // Ignore errors
                    }
                }
            }

            // Close physical socket
            if (socket != null) {
                socket.close();
            }
        }
    }

    /**
     * Force close the socket causing any pending reads/writes to fail.
     * <p>Used by the login timer to abort a login attempt.
     */
    void forceClose() {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ioe) {
                // Ignore
            } finally {
                socket = null;
            }
        }
    }

    /**
     * Deallocate the streamID linked to this socket.
     *
     * @param streamId The ResponseStream id.
     */
    void closeStream(int streamId) {
        synchronized (socketTable) {
            VirtualSocket vsock = lookup(streamId);

            if (vsock.diskQueue != null) {
                try {
                    vsock.diskQueue.close();
                    vsock.queueFile.delete();
                } catch (IOException ioe) {
                    // Ignore errors
                }
            }

            socketTable.set(streamId, null);
        }
    }

    /**
     * Send an network packet. If output for another virtual socket is
     * in progress this packet will be sent later.
     *
     * @param streamId The originating RequestStream object.
     * @param buffer  The data to send.
     * @throws IOException
     */
    byte[] sendNetPacket(int streamId, byte buffer[])
            throws IOException {
        synchronized (socketTable) {

            VirtualSocket vsock = lookup(streamId);

            while (vsock.inputPkts > 0) {
                // There is unread data in the input buffer.
                // This may indicate a problem in the higher layers
                // of the driver but for now just clear the buffer.
                if (Logger.isActive()) {
                    Logger.println("TdsSocket: Unread data in input packet queue");
                }

                dequeueInput(vsock);
            }

            if (responseOwner != -1 && responseOwner == streamId) {
                // This means that we are sending again while data from our
                // previous response is still being read across the network.
                // This may indicate an error in the driver. See above.
                if (Logger.isActive()) {
                    Logger.println("TdsSocket: Unread data on network");
                }

                // Tell the driver to discard the rest of the server packets
                vsock.flushInput = true;
            }

            if (responseOwner != -1
            	|| (currentSender != -1 && currentSender != streamId)) {
                //
                // This means that another virtual socket is building an
                // output packet or reading input. We need to enqueue the
                // data to send later.
                //
                enqueueOutput(vsock, buffer);
                // Return a new buffer to the caller so that
                // the cached data is not overwritten.
                buffer = new byte[buffer.length];
            } else {
                //
                // At this point we know that we are able to send the first
                // or subsequent packet of a new request.
                //
                // First, send out any output that might have been queued
                //
                byte[] tmpBuf = dequeueOutput(vsock);

                while (tmpBuf != null) {
                    getOut().write(tmpBuf, 0, getPktLen(tmpBuf, 2));
                    tmpBuf = dequeueOutput(vsock);
                }

                // Now we can safely send this packet too
                getOut().write(buffer, 0, getPktLen(buffer, 2));

                if (buffer[1] != 0) {
                    // This means the TDS Packet is complete
                    currentSender = -1;
                    responseOwner = streamId;
                } else {
                    // This is the first of maybe several buffers to
                    // make up the complete TDS packet.
                    currentSender = streamId;
                }
            }

            return buffer;
        }
    }

    /**
     * Get a network packet. This may be read from the network
     * directly or from previously cached buffers.
     *
     * @param streamId The originating ResponseStream object.
     * @param buffer The data buffer to receive the object (may be replaced)
     * @return The data in a <code>byte[]</code> buffer.
     * @throws IOException
     */
    byte[] getNetPacket(int streamId, byte buffer[]) throws IOException {
        synchronized (socketTable) {
            VirtualSocket vsock = lookup(streamId);

            //
            // Return any cached input
            //
            if (vsock.inputPkts > 0) {
                return dequeueInput(vsock);
            }

            //
            // See if we need to:
            //  1. Send a request
            //  2. Read and save another callers request
            //  3. Flush the end of our previous request and send a new one
            //
            if (responseOwner == -1 || responseOwner != streamId || vsock.flushInput) {
                byte[] tmpBuf;

                if (responseOwner != -1) {
                    // Complex case there is another socket's data in the network pipe
                    // or we had our own incomplete request to discard first
                    // Read and store other socket's data or flush our own
                    VirtualSocket other = (VirtualSocket)socketTable.get(responseOwner);

                    do {
                        tmpBuf = readPacket(null, other, 0);

                        if (!other.flushInput) {
                        	// We need to save this input
                            enqueueInput(other, tmpBuf);
                        }
                    } while (tmpBuf[1] == 0); // Read all data to complete TDS packet
                    other.flushInput = false;
                }

                // OK All input either read and stored or read and discarded
                // now send our cached request packet.
                tmpBuf = dequeueOutput(vsock);

                if (tmpBuf == null) {
                    // Oops something has gone wrong. Trying to read but no
                    // complete request packet to send first.
                    throw new IOException("No client request to send");
                }

                while (tmpBuf != null) {
                    getOut().write(tmpBuf, 0, getPktLen(tmpBuf, 2));
                    tmpBuf = dequeueOutput(vsock);
                }

                responseOwner = streamId;
            }

            // Simple case we are reading our input directly from the server
            buffer = readPacket(buffer, vsock, vsock.timeOut);

            return buffer;
        }
    }

    /**
     * Return any server packet saved for the specified TdsCore object.
     */
    private byte[] dequeueInput(VirtualSocket vsock) throws IOException {
        byte[] buf = dequeuePacket(vsock);

        if (buf != null) {
            vsock.inputPkts--;
        }

        return buf;
    }

    /**
     * Save a server packet for the specified TdsCore object.
     */
    private void enqueueInput(VirtualSocket vsock, byte[] buffer)
            throws IOException {
        enqueuePacket(vsock, buffer);
        vsock.inputPkts++;
    }

    /**
     * Return any client request packet saved for the specified TdsCore object.
     */
    private byte[] dequeueOutput(VirtualSocket vsock)
            throws IOException {
        byte[] buf = dequeuePacket(vsock);

        if (buf == null) {
            vsock.complete = false;
        } else {
            vsock.outputPkts--;
        }

        return buf;
    }

    /**
     * Save a client request packet for the specified TdsCore object.
     */
    private void enqueueOutput(VirtualSocket vsock, byte[] buffer)
            throws IOException {
        enqueuePacket(vsock, buffer);
        vsock.complete = buffer[1] != 0;
        vsock.outputPkts++;
    }

    /**
     * Save a packet buffer in a memory queue or to a disk queue
     * if the global memory limit for the driver has been exceeded.
     */
    private void enqueuePacket(VirtualSocket vsock,
                               byte[] buffer)
            throws IOException {
        //
        // Check to see if we should start caching to disk
        //
        if (globalMemUsage + buffer.length > memoryBudget &&
                vsock.pktQueue.size() >= minMemPkts &&
                !securityViolation &&
                vsock.diskQueue == null) {
            // Try to create a disk file for the queue
            try {
                vsock.queueFile = File.createTempFile("jtds", ".tmp");
                vsock.diskQueue = new RandomAccessFile(vsock.queueFile, "rw");

                // Write current cache contents to disk and free memory
                byte[] tmpBuf;

                while (vsock.pktQueue.size() > 0) {
                    tmpBuf = (byte[]) vsock.pktQueue.removeFirst();
                    vsock.diskQueue.write(tmpBuf, 0, getPktLen(tmpBuf, 2));
                    vsock.pktsOnDisk++;
                }
            } catch (java.lang.SecurityException se) {
                // Not allowed to cache to disk so carry on in memory
                securityViolation = true;
                vsock.queueFile = null;
                vsock.diskQueue = null;
            }
        }

        if (vsock.diskQueue != null) {
            // Cache file exists so append buffer to it
            vsock.diskQueue.write(buffer, 0, getPktLen(buffer, 2));
            vsock.pktsOnDisk++;
        } else {
            // Will cache in memory
            vsock.pktQueue.addLast(buffer);
            globalMemUsage += buffer.length;

            if (globalMemUsage > peakMemUsage) {
                peakMemUsage = globalMemUsage;
            }
        }

        return;
    }

    /**
     * Read a cached packet from the in memory queue or from a disk based queue.
     */
    private byte[] dequeuePacket(VirtualSocket vsock)
            throws IOException {
        byte[] buffer = null;

        if (vsock.pktsOnDisk > 0) {
            // Data is cached on disk
            if (vsock.diskQueue.getFilePointer() == vsock.diskQueue.length()) {
                // First read so rewind() file
                vsock.diskQueue.seek(0L);
            }

            vsock.diskQueue.read(hdrBuf, 0, 8);

            int len = getPktLen(hdrBuf, 2);

            buffer = new byte[len];
            System.arraycopy(hdrBuf, 0, buffer, 0, 8);
            vsock.diskQueue.read(buffer, 8, len - 8);
            vsock.pktsOnDisk--;

            if (vsock.pktsOnDisk < 1) {
                // File now empty so close and delete it
                try {
                    vsock.diskQueue.close();
                    vsock.queueFile.delete();
                } finally {
                    vsock.queueFile = null;
                    vsock.diskQueue = null;
                }
            }
        } else if (vsock.pktQueue.size() > 0) {
            buffer = (byte[]) vsock.pktQueue.removeFirst();
            globalMemUsage -= buffer.length;
        }

        return buffer;
    }

    /**
     * Read a physical TDS packet from the network.
     */
    private byte[] readPacket(byte buffer[], VirtualSocket vsock, int timeOut)
            throws IOException {
        boolean queryTimedOut = false;

        do {
            // read the header with timeout specified
            if (timeOut > 0) {
                setTimeout(timeOut);
            }

            int len = 0;

            while (len == 0) {
                try {
                    len = getIn().read(hdrBuf, 0, 1);
                } catch (InterruptedIOException e) {
                    queryTimedOut = true;
                    sendCancel(vsock.owner);
                } finally {
                    if (timeOut > 0) {
                        setTimeout(0);
                    }

                    timeOut = 0;
                }
            }

            if (len == -1) {
            	// this appears to happen when the remote host closes the connection...
                throw new IOException("DB server closed connection.");
            }

            //
            // Read rest of header
            try {
                getIn().readFully(hdrBuf, 1, 7);
            } catch (EOFException e) {
                throw new IOException("DB server closed connection.");
            }

            byte packetType = hdrBuf[0];

            if (packetType != TdsCore.LOGIN_PKT
                    && packetType != TdsCore.QUERY_PKT
                    && packetType != TdsCore.REPLY_PKT) {
                throw new IOException("Unknown packet type 0x" +
                                        Integer.toHexString(packetType));
            }

            // figure out how many bytes are remaining in this packet.
            len = getPktLen(hdrBuf, 2);

            if (len < 8 || len > 65536) {
                throw new IOException("Invalid network packet length " + len);
            }

            if (buffer == null || len > buffer.length) {
                // Create or expand the buffer as required
                buffer = new byte[len];

                if (len > maxBufSize) {
                    maxBufSize = len;
                }
            }

            // Preserve the packet header in the buffer
            System.arraycopy(hdrBuf, 0, buffer, 0, 8);

            try {
                getIn().readFully(buffer, 8, len - 8);
            } catch (EOFException e) {
                throw new IOException("DB server closed connection.");
            }

            //
            // SQL Server 2000 < SP3 does not set the last packet
            // flag in the NT challenge packet.
            // If this is the first packet and the length is correct
            // force the last packet flag on.
            //
            if (++packetCount == 1 && serverType == Driver.SQLSERVER
                    && "NTLMSSP".equals(new String(buffer, 11, 7))) {
                buffer[1] = 1;
            }
        } while ((queryTimedOut && buffer[1] == 0)
        		 || (!queryTimedOut && isUnwantedCancelAck(buffer)));

        //
        // Track TDS packet complete
        //
        if (buffer[1] != 0) {
            responseOwner = -1;

            if (queryTimedOut) {
                // We had timed out so return an exception to notify the caller
                // All input will have been read
                throw new InterruptedIOException("Query timed out");
            }
        }

        return buffer;
    }

    /**
     * Identify isolated cancel packets so that we can ignore them.
     */
    private boolean isUnwantedCancelAck(byte[] buffer) {
        if (buffer[1] == 0) {
            return false; // Not complete TDS packet
        }

        if (getPktLen(buffer, 2) != 17) {
            return false; // To short to contain cancel or has other stuff
        }

        if (buffer[8] != TDS_DONE_TOKEN
            || (buffer[9] & 0x20) == 0) {
            return false; // Not a cancel packet
        }

        if (Logger.isActive()) {
            Logger.println("TdsSocket: Cancel packet discarded");
        }

        return true;
    }

    /**
     * Send a cancel packet.
     */
    private void sendCancel(int streamId) throws IOException {
        byte[] cancel = new byte[8];
        cancel[0] = TdsCore.CANCEL_PKT;
        cancel[1] = 1;
        cancel[2] = 0;
        cancel[3] = 8;
        cancel[4] = 0;
        cancel[5] = 0;
        cancel[6] = (getTdsVersion() >= Driver.TDS70) ? (byte) 1 : 0;
        cancel[7] = 0;
        getOut().write(cancel, 0, 8);

        if (Logger.isActive()) {
            Logger.logPacket(streamId, false, cancel);
        }
    }

    private VirtualSocket lookup(int streamId) {
        if (streamId < 0 || streamId > socketTable.size()) {
            throw new IllegalArgumentException("Invalid parameter stream ID "
            		+ streamId);
        }

        VirtualSocket vsock = (VirtualSocket)socketTable.get(streamId);

        if (vsock.owner != streamId) {
            throw new IllegalStateException("Internal error: bad stream ID "
            		+ streamId);
        }

        return vsock;
    }

    /**
     * Convert two bytes (in network byte order) in a byte array into a Java
     * short integer.
     *
     * @param buf array of data
     * @param offset index into the buf array where the short integer is
     *        stored
     * @return the 16 bit unsigned value as an <code>int</code>
     */
    static int getPktLen(byte buf[], int offset) {
        int lo = ((int) buf[offset + 1] & 0xff);
        int hi = (((int) buf[offset] & 0xff) << 8);

        return hi | lo;
    }

    /**
     * Set the socket timeout.
     *
     * @param timeout the timeout value in milliseconds
     */
    protected void setTimeout(int timeout) throws SocketException {
        socket.setSoTimeout(timeout);
    }

    /**
     * Getter for {@link SharedSocket#in} field.
     *
     * @return {@link InputStream} used for communication.
     */
    protected DataInputStream getIn() {
        return in;
    }

    /**
     * Setter for {@link SharedSocket#in} field.
     *
     * @param in The {@link InputStream} to be used for communication.
     */
    protected void setIn(DataInputStream in) {
        this.in = in;
    }

    /**
     * Getter for {@link SharedSocket#out} field.
     *
     * @return {@link OutputStream} used for communication.
     */
    protected DataOutputStream getOut() {
        return out;
    }

    /**
     * Setter for {@link SharedSocket#out} field.
     *
     * @param out The {@link OutputStream} to be used for communication.
     */
    protected void setOut(DataOutputStream out) {
        this.out = out;
    }
}
