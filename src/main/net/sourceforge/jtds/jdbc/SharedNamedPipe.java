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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.UnknownHostException;

import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbNamedPipe;


/**
 * This class implements inter-process communication (IPC) to the
 * database server using named pipes.
 *
 * @todo Extract abstract base class SharedIpc from {@link SharedSocket} and this class.
 * @todo Implement connection timeouts for named pipes.
 * 
 * @author David D. Kilzer
 * @version $Id: SharedNamedPipe.java,v 1.12 2004-08-28 19:10:01 bheineman Exp $
 */
public class SharedNamedPipe extends SharedSocket {

    /**
     * The shared named pipe.
     */
    private SmbNamedPipe pipe = null;


    /**
     * Default constructor.
     */
    private SharedNamedPipe() {
    }


    /**
     * Construct a SharedNamedPipe to the server.
     * 
     * @param host The SQL Server host name.
     * @param tdsVersion The TDS protocol version.
     * @param serverType The server type (SQL Server or Sybase).
     * @param packetSize The data packet size (used for buffering the named pipe input stream).
     * @param instance The database instance name.
     * @param domain The domain used for Windows (NTLM) authentication.
     * @param user The username.
     * @param password The password.
     * @throws IOException If named pipe or its input or output streams do not open.
     * @throws UnknownHostException If host cannot be found for the named pipe.
     */
    static SharedNamedPipe instance(
            String host, int tdsVersion, int serverType, int packetSize, String instance,
            String domain, String user, String password)
            throws IOException, UnknownHostException {

        SharedNamedPipe newInstance = new SharedNamedPipe();

        newInstance.setTdsVersion(tdsVersion);
        newInstance.setServerType(serverType);

        NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication(domain, user, password);

        StringBuffer url = new StringBuffer();

        url.append("smb://");
        url.append(host);
        url.append("/IPC$/");

        if (instance != null && !instance.equals("")) {
            url.append("MSSQL$");
            url.append(instance);
            url.append("/");
        }

        url.append(DefaultProperties.NAMED_PIPE_PATH_SQLSERVER);

        newInstance.setPipe(
                new SmbNamedPipe(url.toString(), SmbNamedPipe.PIPE_TYPE_RDWR, auth));

        newInstance.setOut(
                new DataOutputStream(
                        newInstance.getPipe().getNamedPipeOutputStream()));

        newInstance.setIn(
                new DataInputStream(
                        new BufferedInputStream(
                                newInstance.getPipe().getNamedPipeInputStream(),
                                newInstance.calculateBufferSize(tdsVersion, packetSize))));

        return newInstance;
    }


    /**
     * Get the connected status of this socket.
     * 
     * @return True if the underlying socket is connected.
     */
    boolean isConnected() {
        return getPipe() != null;
    }


    /**
     * Close the socket (noop if in shared mode)
     */
    void close() throws IOException {
        super.close();
        getOut().close();
        getIn().close();
        //getPipe().close();
    }


    /**
     * Force close the socket causing any pending reads/writes to fail.
     * <p>Used by the login timer to abort a login attempt.
     */
    void forceClose() {
        try {
            getOut().close();
        }
        catch (IOException e) {
            // Ignore
        }
        finally {
            setOut(null);
        }

        try {
            getIn().close();
        }
        catch (IOException e) {
            // Ignore
        }
        finally {
            setIn(null);
        }

        setPipe(null);
    }


    /**
     * Getter for {@link SharedNamedPipe#pipe} field.
     * 
     * @return The {@link SmbNamedPipe} used for communication.
     */ 
    private SmbNamedPipe getPipe() {
        return pipe;
    }


    /**
     * Setter for {@link SharedNamedPipe#pipe} field.
     * 
     * @param pipe The {@link SmbNamedPipe} to be used for communication.
     */ 
    private void setPipe(SmbNamedPipe pipe) {
        this.pipe = pipe;
    }


    /**
     * Set the socket timeout.
     * 
     * @param timeout the timeout value in milliseconds
     */
    protected void setTimeout(int timeout) {
        // FIXME - implement timeout functionality
    }


    /**
     * Calculate the buffer size to use when buffering the {@link SmbNamedPipe}
     * <code>InputStream</code>.  The buffer size is tied directly to the packet
     * size because each request to the <code>SmbNamedPipe</code> will send a
     * request for a particular size of packet.  In other words, if you only
     * request 1 byte, the <code>SmbNamedPipe</code> will send a request out
     * and only ask for 1 byte back.  Buffering the expected packet size ensures
     * that all of the data will be returned in the buffer without wasting any
     * space.
     * <p/>
     * <code>assert (packetSize == 0 || (packetSize >= {@link TdsCore#MIN_PKT_SIZE}
     * && packetSize <= {@link TdsCore#MAX_PKT_SIZE}))</code>
     * 
     * @param packetSize The requested packet size for the connection.
     * @return minimum default packet size if <code>packetSize == 0</code>, else <code>packetSize</code>
     */
    private int calculateBufferSize(final int tdsVersion, final int packetSize) {

        if (packetSize == 0) {
            if (tdsVersion >= Driver.TDS70) {
                return TdsCore.DEFAULT_MIN_PKT_SIZE_TDS70;
            }

            return TdsCore.MIN_PKT_SIZE;
        }

        return packetSize;
    }
}
