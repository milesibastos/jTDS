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

import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.GeneralSecurityException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import javax.net.SocketFactory;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import com.sun.net.ssl.SSLContext;
import com.sun.net.ssl.TrustManager;
import com.sun.net.ssl.X509TrustManager;

import net.sourceforge.jtds.jdbc.Messages;
import net.sourceforge.jtds.util.Logger;

/**
 * Used for acquiring a socket factory when SSL is enabled.
 *
 * @author Rob Worsnop
 * @version $Id: SocketFactories.java,v 1.2 2005-01-06 15:45:12 alin_sinpalean Exp $
 */
public class SocketFactories {
    /**
     * Returns a socket factory, the behavior of which will depend on the SSL
     * setting and whether or not the DB server supports SSL.
     *
     * @param ssl      the SSL setting
     * @param instance the DB instance name
     */
    public static SocketFactory getSocketFactory(String ssl, String instance) {
        return new TdsTlsSocketFactory(ssl, instance);
    }

    /**
     * Requests a secure connection with the DB server.
     *
     * @param socket   the socket on which to make the request
     * @param instance the DB instance to which to connect
     * @return <code>true</code> if SSL is enabled, otherwise <code>false</code>
     */
    private static boolean requestEncryption(Socket socket, String instance)
            throws IOException {
        byte buf[] = new byte[instance.length() + 39];
        buf[2] = 0x1A;
        buf[4] = 0x06;
        buf[5] = 0x01;
        buf[7] = 0x20;
        buf[9] = 0x01;
        buf[10] = 0x02;
        buf[12] = 0x21;
        // this is an offset and depends on length of instance name.
        buf[14] = (byte) (instance.length() + 1);
        buf[15] = 0x03;
        // this is an offset and depends on length of instance name.
        buf[17] = (byte) (instance.length() + 34);
        buf[19] = 0x04;
        buf[20] = 0x04;
        // this is an offset and depends on length of instance name.
        buf[22] = (byte) (instance.length() + 38);
        buf[24] = 0x01;
        buf[25] = (byte) 0xFF;
        buf[26] = 0x09;
        System.arraycopy(instance.getBytes(), 0, buf, 33, instance.length());
        buf[34 + instance.length()] = (byte) 0xC4;
        buf[35 + instance.length()] = 0x0F;
        buf[38 + instance.length()] = 0x01;
        Util.putPacket(socket.getOutputStream(), buf);

        int read = new TdsTlsInputStream(socket.getInputStream()).read(buf);
        if (read < 29) {
            throw new EOFException("Server returned " + read
                    + " bytes; expected " + 29);
        }
        return buf[(buf[2] & 0xff) + 6] != 0x02;
    }

    /**
     * The socket factory for creating sockets based on the SSL setting.
     */
    private static class TdsTlsSocketFactory extends SocketFactory {

        private String ssl;
        private String instance;
        private static SSLSocketFactory factorySingleton;

        /**
         * Constructs a TdsTlsSocketFactory.
         *
         * @param ssl      the SSL setting
         * @param instance the DB instance name
         */
        public TdsTlsSocketFactory(String ssl, String instance) {
            this.ssl = ssl;
            this.instance = instance;
        }

        /*
         * (non-Javadoc)
         *
         * @see javax.net.SocketFactory#createSocket(java.lang.String, int)
         */
        public Socket createSocket(String host, int port) throws IOException,
                UnknownHostException {
            return createSocket(new Socket(host, port));
        }

        /*
         * (non-Javadoc)
         *
         * @see javax.net.SocketFactory#createSocket(java.net.InetAddress, int)
         */
        public Socket createSocket(InetAddress host, int port)
                throws IOException {
            return createSocket(host.getHostName(), port);
        }

        /*
         * (non-Javadoc)
         *
         * @see javax.net.SocketFactory#createSocket(java.lang.String, int,
         *      java.net.InetAddress, int)
         */
        public Socket createSocket(String host, int port,
                                   InetAddress localHost, int localPort) throws IOException,
                UnknownHostException {
            return createSocket(new Socket(host, port, localHost, localPort));
        }

        /*
         * (non-Javadoc)
         *
         * @see javax.net.SocketFactory#createSocket(java.net.InetAddress, int,
         *      java.net.InetAddress, int)
         */
        public Socket createSocket(InetAddress host, int port,
                                   InetAddress localHost, int localPort) throws IOException {
            return createSocket(host.getHostName(), port, localHost, localPort);
        }

        /**
         * Creates a socket based on an existing socket and the SSL setting.
         *
         * @param socket the existing socket
         * @return a new socket, which may or may not be an SSL socket
         */
        private Socket createSocket(Socket socket) throws IOException {
            boolean sslOK = requestEncryption(socket, instance);
            if (!sslOK) {
                if (ssl.equals(Ssl.SSL_REQUEST)) {
                    // if SSL requested but not available, simply use the plain
                    // socket.
                    return socket;
                } else {
                    // else throw an exception.
                    throw new IOException(Messages
                            .get("error.ssl.encryptionoff"));
                }
            }
            // Now we definitely want to create an SSLSocket.
            // The chain will end up looking like this:
            //
            // SSLSocket-->TdsTlsSocket-->plainsocket
            return (SSLSocket) getFactory().createSocket(new TdsTlsSocket(socket),
                    socket.getInetAddress().getHostName(), socket.getPort(),
                    true);

        }

        /**
         * Returns an SSLSocketFactory whose behavior will depend on the SSL
         * setting.
         *
         * @return an <code>SSLSocketFactory</code>
         */
        private SSLSocketFactory getFactory() throws IOException {
            try {
                if (ssl.equals(Ssl.SSL_AUTHENTICATE)) {
                    // the default factory will produce a socket that authenticates
                    // the server using its certificate chain.
                    return (SSLSocketFactory) SSLSocketFactory.getDefault();
                } else {
                    // Our custom factory will not authenticate the server.
                    return factory();
                }
            } catch (GeneralSecurityException e) {
                Logger.logException(e);
                throw new IOException(e.getMessage());
            }
        }

        /**
         * Returns an SSLSocketFactory whose sockets will not authenticate the
         * server.
         *
         * @return an <code>SSLSocketFactory</code>
         */
        private static SSLSocketFactory factory()
                throws NoSuchAlgorithmException, KeyManagementException {
            if (factorySingleton == null) {
                SSLContext ctx = SSLContext.getInstance("TLS");
                ctx.init(null, trustManagers(), null);
                factorySingleton = ctx.getSocketFactory();
            }
            return factorySingleton;
        }

        private static TrustManager[] trustManagers() {
            X509TrustManager tm = new X509TrustManager() {
                public X509Certificate[] getAcceptedIssuers() {
                    return new X509Certificate[0];
                }

                public boolean isClientTrusted(X509Certificate[] chain) {
                    return true;
                }

                public boolean isServerTrusted(X509Certificate[] chain) {
                    return true;
                }
            };

            return new X509TrustManager[]{tm};
        }

    }
}