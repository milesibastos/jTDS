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
 * @author David D. Kilzer.
 * @version $Id: SharedNamedPipe.java,v 1.2 2004-07-26 18:45:26 bheineman Exp $
 */
public class SharedNamedPipe extends SharedSocket {
    /**
     * The shared named pipe.
     */
    private SmbNamedPipe pipe = null;


    /**
     * Constructed a SharedNamedPipe to the server.
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
    public SharedNamedPipe(String host, int tdsVersion, int serverType,
            int packetSize, String instance, String domain, String user,
            String password)
            throws IOException, UnknownHostException {

        this.tdsVersion = tdsVersion;
        this.serverType = serverType;

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

        url.append("/sql/query");

        pipe = new SmbNamedPipe(url.toString(), SmbNamedPipe.PIPE_TYPE_RDWR, auth);

        this.out = new DataOutputStream(pipe.getNamedPipeOutputStream());
        this.in = new DataInputStream(new BufferedInputStream(pipe.getNamedPipeInputStream(), packetSize));
    }

    /**
     * Get the connected status of this socket.
     *
     * @return True if the underlying socket is connected.
     */
    boolean isConnected() {
        return this.pipe != null;
    }


    /**
     * Close the socket (noop if in shared mode)
     */
    void close() throws IOException {
        super.close();
        this.out.close();
        this.in.close();
        //this.pipe.close();
    }


    /**
     * Force close the socket causing any pending reads/writes to fail.
     * <p>Used by the login timer to abort a login attempt.
     */
    void forceClose() {
        try {
            this.out.close();
        } catch (IOException e) {
            // Ignore
        } finally {
            this.out = null;
        }

        try {
            this.in.close();
        } catch (IOException e) {
            // Ignore
        } finally {
            this.in = null;
        }

        this.pipe = null;
    }

    /**
     * Set the socket timeout.
     * 
     * @param timeout the timeout value in milliseconds
     */
    protected void setTimeout(int timeout) {
        // FIXME - implement timeout functionality
    }
}
