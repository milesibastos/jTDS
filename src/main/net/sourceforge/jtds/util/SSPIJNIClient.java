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
package net.sourceforge.jtds.util;

import java.sql.SQLException;

/**
 * A JNI client to SSPI based CPP program (DLL) that returns the user
 * credentials for NTLM authentication.
 * <p/>
 * The DLL name is ntlmauth.dll.
 *
 * @author Magendran Sathaiah (mahi@aztec.soft.net)
 */
public class SSPIJNIClient {
    /** Singleton instance. */
    private static SSPIJNIClient thisInstance = null;

    /** SSPI native library loaded flag. */
    private static boolean libraryLoaded = false;

    /** SSPI client initialized flag. */
    private boolean initialized = false;

    /** Initializes the SSPI client. */
    private native void initialize();

    /** Uninitializes the SSPI client. */
    private native void unInitialize();

    /**
     * Prepares the NTLM TYPE-1 message and returns it as a
     * <code>byte[]</code>.
     */
    private native byte[] prepareSSORequest();

    /**
     * Prepares the NTLM TYPE-3 message using the current user's credentials.
     * <p>
     * It needs the challenge BLOB and it's size as input. The challenge BLOB
     * is nothig but the TYPE-2 message that is received from the SQL Server.
     *
     * @param buf  challenge BLOB
     * @param size challenge BLOB size
     * @return NTLM TYPE-3 message
     */
    private native byte[] prepareSSOSubmit(byte[] buf, long size);

    static {
        try {
            System.loadLibrary("ntlmauth");
            SSPIJNIClient.libraryLoaded = true;
        } catch (UnsatisfiedLinkError err) {
            Logger.println("Unable to load library: " + err);
        }
    }

    /**
     * Private constructor for singleton.
     */
    private SSPIJNIClient() {
        //empty constructor
    }

    /**
     * Returns the singleton <code>SSPIJNIClient</code> instance.
     *
     * @throws SQLException if an error occurs during initialization
     */
    public static SSPIJNIClient getInstance() throws Exception {

        if (thisInstance == null) {
            if (!libraryLoaded) {
                throw new Exception("Native SSPI library not loaded. "
                        + "Check the java.library.path system property.");
            }
            thisInstance = new SSPIJNIClient();
            thisInstance.invokeInitialize();
        }
        return thisInstance;
    }

    /**
     * Calls {@link #initialize()} if the SSPI client is not already inited.
     */
    public void invokeInitialize() {
        if (!initialized) {
            initialize();
            initialized = true;
        }
    }

    /**
     * Calls {@link #unInitialize()} if the SSPI client is inited.
     */
    public void invokeUnInitialize() {
        if (initialized) {
            unInitialize();
            initialized = false;
        }
    }

    /**
     * Calls {@link #prepareSSORequest()} to prepare the NTLM TYPE-1 message.
     *
     * @throws Exception if an error occurs during the call or the SSPI client
     *                   is uninitialized
     */
    public byte[] invokePrepareSSORequest() throws Exception {
        if (!initialized) {
            throw new Exception("SSPI Not Initialized");
        }
        return prepareSSORequest();
    }

    /**
     * Calls {@link #prepareSSOSubmit(byte[], long)} to prepare the NTLM TYPE-3
     * message.
     *
     * @throws Exception if an error occurs during the call or the SSPI client
     *                   is uninitialized
     */
    public byte[] invokePrepareSSOSubmit(byte[] buf) throws Exception {
        if (!initialized) {
            throw new Exception("SSPI Not Initialized");
        }
        return prepareSSOSubmit(buf, buf.length);
    }

    public static void main(String[] args) {
        byte[] str;
        try {
            System.out.println("instance request.");
            SSPIJNIClient client = new SSPIJNIClient();
            System.out.println("instance done..");
            System.out.println("init request..");
            client.invokeInitialize();
            System.out.println("init..done");
            System.out.println("preare req request..");
            str = client.invokePrepareSSORequest();
            hexDump(str);
            System.out.println("\nRequest.length: " + str.length);
            System.out.println("prepare done...");
            //str = client.invokePrepareSSOSubmit(str.getBytes(), str.length());
            System.out.println("Submit: " + str);
            System.out.println("uninit request..");
            client.invokeUnInitialize();
            System.out.println("uninit done..");
        } catch (Error e) {
            System.out.println("Error occured");
            e.printStackTrace();
        } catch (Exception e) {
            System.out.println("Exception occured");
            e.printStackTrace();
        }
    }

    private static void hexDump(byte[] data) {
        int loc;
        int end;
        int off = 0;
        int len = data.length;
        int base = 10;
        System.out.println("Buffer.Size :" + len);
        // Print a hexadecimal dump of 'data[off...off+len-1]'
        if (off >= data.length) {
            off = data.length;
        }

        end = off + len;
        if (end >= data.length) {
            end = data.length;
        }

        len = end - off;
        if (len <= 0) {
            return;
        }

        loc = (off / 0x10) * 0x10;

        for (int i = loc; i < end; i += 0x10, loc += 0x10) {
            int j;

            // Print the location/offset
            {
                int v;

                v = base + loc;
                for (j = (8 - 1) * 4; j >= 0; j -= 4) {
                    int d;

                    d = (v >>> j) & 0x0F;
                    d = (d < 0xA ? d + '0' : d - 0xA + 'A');
                    System.out.print((char) d);
                }
            }

            // Print a row of hex bytes
            System.out.print("  ");
            for (j = 0x00; i + j < off; j++) {
                System.out.print(".. ");
            }

            for (; j < 0x10 && i + j < end; j++) {
                int ch;
                int d;

                if (j == 0x08) {
                    System.out.print(' ');
                }

                ch = data[i + j] & 0xFF;

                d = (ch >>> 4);
                d = (d < 0xA ? d + '0' : d - 0xA + 'A');
                System.out.print((char) d);

                d = (ch & 0x0F);
                d = (d < 0xA ? d + '0' : d - 0xA + 'A');
                System.out.print((char) d);

                System.out.print(' ');
            }

            for (; j < 0x10; j++) {
                if (j == 0x08) {
                    System.out.print(' ');
                }

                System.out.print(".. ");
            }

            // Print a row of printable characters
            System.out.print(" |");
            for (j = 0x00; i + j < off; j++) {
                System.out.print(' ');
            }

            for (; j < 0x10 && i + j < end; j++) {
                int ch;

                ch = data[i + j] & 0xFF;

                if (ch < 0x20 ||
                        ch >= 0x7F && ch < 0xA0 ||
                        ch > 0xFF) {
                    // The character is unprintable
                    ch = '.';
                }

                System.out.print((char) ch);
            }

            for (; j < 0x10; j++) {
                System.out.print(' ');
            }

            System.out.println("|");
        }
    }
}
