/*
 * Created on 10-Feb-2004
 *
 */
package net.sourceforge.jtds.jdbc;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.LinkedList;

import net.sourceforge.jtds.util.Logger;

/**
 * This class mananges the physical connection to the SQL Server and
 * serialises its use amongst a number of virtual sockets.
 * This allows one physical connection to service a number of concurrent
 * statements.
 *
 * Constraints and assumptions:
 * 1. Callers will not attempt to read from the server without issuing a request first.
 * 2. The end of a server reply can be identified as byte 2 of the header is non zero.
 *
 * Comments:
 * 1. This code will discard unread server data if a new request is issued.
 *    Currently the higher levels of the driver attempt to do this but may be
 *    we can just rely on this code instead.
 * 2. A cancel can be issued by a caller only if the server is currently sending
 *    data for the caller otherwise the cancel is ignored.
 * 3. Query timeouts are implemented via socket timeout on the first byte of a packet.
 *    A cancel is issued in this case to terminate the reply and an exception issued
 *    to the caller but only after the input stream has be entirely read.
 * 4. Stray cancel packets on their own are filtered out so that the higher levels
 *    do not need to.
 *
 * This version of the class will start to cache results to disk once a predetermined
 * maximum buffer memory threshold has been passed. Small result sets that will fit
 * within a specified limit (default 8 packets) will continue to be held in memory
 * (even if the memory threshold has been passed) in the interests of efficiency.
 *
 * @author Mike Hutchinson.
 * @version $Id: TdsSocket.java,v 1.8 2004-05-03 23:29:08 bheineman Exp $
 */
public class TdsSocket {

    /**
     * This inner class contains the state information for the virtual socket.
     */
    private static class VirtualSocket {
        /**
         * The TdsComm object owning this state.
         */
        private TdsComm owner;
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
         * @param comm
         */
        VirtualSocket(TdsComm comm) {
            this.owner = comm;
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
    private int maxBufSize = 512;
    /**
     * Table of TdsComm 'VirtualSocket' objects sharing this socket.
     */
    private ArrayList socketTable = null;
    /**
     * The TdsComm object that is expecting a response from the server.
     */
    private TdsComm responseOwner = null;
    /**
     * The TdsComm object that is currently building a send packet.
     */
    private TdsComm currentSender = null;
    /**
     * The last network packet completed a TDS packet.
     */
    private boolean tdsPacketComplete = false;
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
     * Construct a TdsSocket object specifying host name and port.
     * @param host The SQL Server host name.
     * @param port The connection port eg 1433.
     * @throws IOException If Socket open fails.
     */
    public TdsSocket(String host, int port)
            throws IOException, UnknownHostException {
        socket = new Socket(host, port);
        socket.setTcpNoDelay(true);
        out = new DataOutputStream(socket.getOutputStream());
        in = new DataInputStream(socket.getInputStream());
        socketTable = new ArrayList();
    }

    /**
     * Set the global buffer memory limit for all instances of this driver.
     * @param memoryBudget The global memory budget.
     */
    public static void setMemoryBudget(int memoryBudget) {
        TdsSocket.memoryBudget = memoryBudget;
    }

    /**
     * Get the global buffer memory limit for all instancs of this driver.
     * @return The memory limit as an <code>int</code>.
     */
    public static int getMemoryBudget() {
        return TdsSocket.memoryBudget;
    }

    /**
     * Set the minimum number of packets to cache in memory before
     * writing to disk.
     * @param minMemPkts The minimum number of packets to cache.
     */
    public static void setMinMemPkts(int minMemPkts) {
        TdsSocket.minMemPkts = minMemPkts;
    }

    /**
     * Get the minimum number of memory cached packets.
     * @return Minimum memory packets as an <code>int</code>.
     */
    public static int getMinMemPkts() {
        return TdsSocket.minMemPkts;
    }

    /**
     * Get the local host address from the underlying socket.
     * @return The local host as a <code>InetAddress</code>.
     */
    public InetAddress getLocalAddress() {
        return (socket != null) ? socket.getLocalAddress() : null;
    }

    /**
     * Get the local port from the underlying socket.
     * @return The <code>int</code> value of the local port.
     */
    public int getLocalPort() {
        return (socket != null) ? socket.getLocalPort() : 0;
    }

    /**
     * Get the remote port from the underlying socket.
     * @return The <code>int</code> value of the remote port.
     */
    public int getPort() {
        return (socket != null) ? socket.getPort() : 0;
    }

    /**
     * Get the remote host address from the underlying socket.
     * @return The remote host as a <code>InetAddress</code>.
     */
    public InetAddress getInetAddress() {
        return (socket != null) ? socket.getInetAddress() : null;
    }

    /**
     * Set the Socket time out in msec.
     * @param comm The TdsObject associated with this timeout.
     * @param timeOut The time  out value in milliseconds.
     */
    public void setSoTimeout(TdsComm comm, int timeOut) {
        VirtualSocket entry = lookup(comm);
        entry.timeOut = timeOut;
    }

    /**
     * Get the Socket time out value.
     * @param comm The TdsObject associated with this timeout.
     * @return The <code>int</code> value of the timeout.
     */
    public int getSoTimeout(TdsComm comm) {
        VirtualSocket entry = lookup(comm);
        return entry.timeOut;
    }

    /**
     * Get the connected status of this socket.
     * @return True if the underlying socket is connected.
     */
    public boolean isConnected() {
        // Socket.isClosed() contains a  synchronized block and I do not see
        // the need to impose the additional overhead on each call to a
        // Connection / Statement / ResultSet...
        // Socket.isConnection() and Socket.isClosed() are only available in 1.4
        return this.socket != null;
    }

    /**
     * Send a TDS cancel packet to the server.
     * @param tdsComm The virtual socket.
     */
    public void cancel(TdsComm tdsComm) {
        //
        // Only send if response pending for the caller
        //
        if (responseOwner != null &&
                responseOwner == tdsComm &&
                currentSender == null) {
            try {
                sendCancel(tdsComm);
            } catch (IOException e) {
                // Ignore error as network is probably dead anyway
                try {
                    close();
                } catch (IOException e1) {
                    //  swallow the exception. it is not interesting
                }
            }
        }
    }

    /**
     * Close the socket (noop if in shared mode)
     * @throws IOException
     */
    public void close() throws IOException {
        if (Logger.isActive())
            Logger.println("TdsSocket: Max buffer memory used = " + peakMemUsage + "KB");

        // See if any temporary files need deleting
        for (int i = 0; i < socketTable.size(); i++) {
            VirtualSocket vsock = (VirtualSocket) socketTable.get(i);
            if (vsock.diskQueue != null) {
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

    /**
     * Send an network packet. If output for another virtual socket is
     * in progress this packet will be sent later.
     * @param tdsComm The originating TdsComm object.
     * @param buffer  The data to send.
     * @throws IOException
     */
    public byte[] sendNetPacket(TdsComm tdsComm, byte buffer[])
            throws IOException {
        synchronized (socketTable) {

            VirtualSocket vsock = lookup(tdsComm);

            while (vsock.inputPkts > 0) {
                // There is unread data in the input buffer.
                // This may indicate a problem in the higher layers
                // of the driver but for now just clear the buffer.
                if (Logger.isActive())
                    Logger.println("TdsSocket: Unread data in input packet queue");
                dequeueInput(vsock);
            }

            if (responseOwner != null && responseOwner == tdsComm) {
                // This means that we are sending again while data from our
                // previous response is still being read across the network.
                // This may indicate an error in the driver. See above.
                if (Logger.isActive())
                    Logger.println("TdsSocket: Unread data on network");
                // Tell the driver to discard the rest of the server packets
                vsock.flushInput = true;
            }

            if ((responseOwner != null) ||
                    (currentSender != null && currentSender != tdsComm)) {
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
                try {
                    byte[] tmpBuf = dequeueOutput(vsock);

                    while (tmpBuf != null) {
                        out.write(tmpBuf, 0, TdsComm.ntohs(tmpBuf, 2));
                        tmpBuf = dequeueOutput(vsock);
                    }

                    // Now we can safely send this packet too
                    out.write(buffer, 0, TdsComm.ntohs(buffer, 2));
                } catch (IOException e) {
                    try {
                        close();
                    } catch (IOException e1) {
                        //  swallow the exception. it is not interesting
                    }

                    throw e;
                }


                if (buffer[1] != 0) {
                    // This means the TDS Packet is complete
                    currentSender = null;
                    responseOwner = tdsComm;
                } else {
                    // This is the first of maybe several buffers to
                    // make up the complete TDS packet.
                    currentSender = tdsComm;
                }
            }
            return buffer;
        }
    }

    /**
     * Get a network packet. This may be read from the network
     * directly or from previously cached buffers.
     * @param tdsComm The originating TdsComm object
     * @param buffer The data buffer to receive the object (may be replaced)
     * @return The data in a <code>byte[]</code> buffer.
     * @throws IOException
     * @throws TdsUnknownPacketType
     * @throws TdsException
     */
    public byte[] getNetPacket(TdsComm tdsComm, byte buffer[])
            throws IOException,
            TdsUnknownPacketType,
            TdsException {
        synchronized (socketTable) {
            VirtualSocket vsock = lookup(tdsComm);

            //
            // Return any cached input
            //
            if (vsock.inputPkts > 0)
                return dequeueInput(vsock);
            //
            // See if we need to:
            //  1. Send a request
            //  2. Read and save another callers request
            //  3. Flush the end of our previous request and send a new one
            //
            try {
                if (responseOwner == null || responseOwner != tdsComm || vsock.flushInput) {
                    byte[] tmpBuf;
                    if (responseOwner != null) {
                        // Complex case there is another socket's data in the network pipe
                        // or we had our own incomplete request to discard first
                        // Read and store other socket's data or flush our own
                        VirtualSocket other = lookup(responseOwner);
                        do {
                            tmpBuf = readPacket(null, other, 0);
                            if (!other.flushInput)
                            // We need to save this input
                                enqueueInput(other, tmpBuf);
                        } while (tmpBuf[1] == 0); // Read all data to complete TDS packet
                        other.flushInput = false;
                    }
                    // OK All input either read and stored or read and discarded
                    // now send our cached request packet.
                    tmpBuf = dequeueOutput(vsock);
                    if (tmpBuf == null) {
                        // Oops something has gone wrong. Trying to read but no
                        // complete request packet to send first.
                        throw new TdsException("No client request to send");
                    }
                    while (tmpBuf != null) {
                        out.write(tmpBuf, 0, TdsComm.ntohs(tmpBuf, 2));
                        tmpBuf = dequeueOutput(vsock);
                    }
                    responseOwner = tdsComm;
                }

                // Simple case we are reading our input directly from the server
                buffer = readPacket(buffer, vsock, vsock.timeOut);
            } catch (IOException e) {
                try {
                    close();
                } catch (IOException e1) {
                    //  swallow the exception. it is not interesting
                }

                throw e;
            }

            return buffer;
        }
    }

    /**
     * Lookup the TdsComm object in our virtual socket table.
     */
    private synchronized VirtualSocket lookup(TdsComm comm) {
        VirtualSocket entry;
        for (int i = 0; i < socketTable.size(); i++) {
            entry = (VirtualSocket) socketTable.get(i);
            if (entry.owner == comm)
                return entry;
        }
        entry = new VirtualSocket(comm);
        socketTable.add(entry);
        return entry;
    }

    /**
     * Return any server packet saved for the specified TdsComm object.
     */
    private byte[] dequeueInput(VirtualSocket vsock)
            throws IOException {
        byte[] buf = dequeuePacket(vsock);
        if (buf != null)
            vsock.inputPkts--;
        return buf;
    }

    /**
     * Save a server packet for the specified TdsComm object.
     */
    private void enqueueInput(VirtualSocket vsock, byte[] buffer)
            throws IOException {
        enqueuePacket(vsock, buffer);
        vsock.inputPkts++;
    }

    /**
     * Return any client request packet saved for the specified TdsComm object.
     */
    private byte[] dequeueOutput(VirtualSocket vsock)
            throws IOException {
        byte[] buf = dequeuePacket(vsock);
        if (buf == null)
            vsock.complete = false;
        else
            vsock.outputPkts--;
        return buf;
    }

    /**
     * Save a client request packet for the specified TdsComm object.
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
                    vsock.diskQueue.write(tmpBuf, 0, TdsComm.ntohs(tmpBuf, 2));
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
            vsock.diskQueue.write(buffer, 0, TdsComm.ntohs(buffer, 2));
            vsock.pktsOnDisk++;
        } else {
            // Will cache in memory
            vsock.pktQueue.addLast(buffer);
            globalMemUsage += buffer.length;
            if (globalMemUsage > peakMemUsage)
                peakMemUsage = globalMemUsage;
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
            int len = TdsComm.ntohs(hdrBuf, 2);
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
            throws IOException,
            TdsUnknownPacketType,
            TdsException {
        boolean queryTimedOut = false;
        do {
            // read the header with timeout specified
            if (timeOut > 0)
                socket.setSoTimeout(timeOut);
            int len = 0;
            while (len == 0) {
                try {
                    len = in.read(hdrBuf, 0, 1);
                } catch (InterruptedIOException e) {
                    queryTimedOut = true;
                    sendCancel(vsock.owner);
                } finally {
                    if (timeOut > 0)
                        socket.setSoTimeout(0);
                }
            }
            if (len == -1)
            // this appears to happen when the remote host closes the connection...
                throw new IOException("Db server closed connection.");
            //
            // Read rest of header
            try {
                in.readFully(hdrBuf, 1, 7);
            } catch (EOFException e) {
                throw new IOException("Db server closed connection.");
            }
            byte packetType = hdrBuf[0];
            if (packetType != TdsComm.LOGON
                    && packetType != TdsComm.QUERY
                    && packetType != TdsComm.REPLY) {
                throw new TdsUnknownPacketType(packetType, hdrBuf);
            }

            // figure out how many bytes are remaining in this packet.
            len = TdsComm.ntohs(hdrBuf, 2);

            if (len < 8 || len > 65536) {
                throw new TdsException("Invalid network packet length " + len);
            }

            if (buffer == null || len > buffer.length) {
                // Create or expand the buffer as required
                buffer = new byte[len];
                if (len > maxBufSize)
                    maxBufSize = len;
            }
            // Preserve the packet header in the buffer
            System.arraycopy(hdrBuf, 0, buffer, 0, 8);

            try {
                in.readFully(buffer, 8, len - 8);
            } catch (EOFException e) {
                throw new IOException("Db server closed connection.");
            }
            //
            // Eliminate stray cancel acks if they are in a packet by
            // themselves.
            //
        } while (isUnwantedCancelAck(buffer));

        //
        // Track TDS packet complete
        //
        if (buffer[1] != 0) {
            tdsPacketComplete = true;
            responseOwner = null;
            if (queryTimedOut) {
                // We had timed out so return an exception to notify the caller
                // All input will have been read
                throw new TdsTimeoutException("Query timed out");
            }
        }

        return buffer;
    }

    /**
     * Identify isolated cancel packets so that we can ignore them.
     */
    private boolean isUnwantedCancelAck(byte[] buffer) {
        if (!tdsPacketComplete)
            return false; // Previous packet was not the last in a response
        if (buffer[1] == 0)
            return false; // Not complete TDS packet
        if (TdsComm.ntohs(buffer, 2) != 17)
            return false; // To short to contain cancel or has other stuff
        if (buffer[8] != TdsDefinitions.TDS_DONE ||
                (buffer[9] & 0x20) == 0)
            return false; // Not a cancel packet
        if (Logger.isActive())
            Logger.println("TdsSocket: Cancel packet discarded");
        return true;
    }

    /**
     * Send a cancel packet.
     */
    private void sendCancel(TdsComm comm)
            throws IOException {
        byte[] cancel = new byte[8];
        cancel[0] = TdsComm.CANCEL;
        cancel[1] = 1;
        cancel[2] = 0;
        cancel[3] = 8;
        cancel[4] = 0;
        cancel[5] = 0;
        cancel[6] = (comm.getTdsVer() == Tds.TDS70)? (byte)1: (byte)0;
        cancel[7] = 0;
        out.write(cancel, 0, 8);
        if (Logger.isActive())
            Logger.println("TdsSocket: Send cancel packet");
    }
}
