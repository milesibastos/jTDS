package net.sourceforge.jtds.jdbc;

import java.io.InterruptedIOException;
import java.sql.SQLException;
import java.net.*;

/**
 * This class communicates with SQL Server 2k to determine what ports its
 * instances are listening to. It does this by sending a UDP packet with the
 * byte 0x02 in it, and parsing the response.
 * <p>
 * An example of the return packet format is as follows:
 * <pre>
 * < 00000000 05 ea 00 53 65 72 76 65 72 4e 61 6d 65 3b 42 52 # ...ServerName;BR
 * < 00000010 49 4e 4b 4c 45 59 3b 49 6e 73 74 61 6e 63 65 4e # INKLEY;InstanceN
 * < 00000020 61 6d 65 3b 4d 53 53 51 4c 53 45 52 56 45 52 3b # ame;MSSQLSERVER;
 * < 00000030 49 73 43 6c 75 73 74 65 72 65 64 3b 4e 6f 3b 56 # IsClustered;No;V
 * < 00000040 65 72 73 69 6f 6e 3b 38 2e 30 30 2e 31 39 34 3b # ersion;8.00.194;
 * < 00000050 74 63 70 3b 31 34 33 33 3b 6e 70 3b 5c 5c 42 52 # tcp;1433;np;\\BR
 * < 00000060 49 4e 4b 4c 45 59 5c 70 69 70 65 5c 73 71 6c 5c # INKLEY\pipe\sql\
 * < 00000070 71 75 65 72 79 3b 3b 53 65 72 76 65 72 4e 61 6d # query;;ServerNam
 * < 00000080 65 3b 42 52 49 4e 4b 4c 45 59 3b 49 6e 73 74 61 # e;BRINKLEY;Insta
 * < 00000090 6e 63 65 4e 61 6d 65 3b 44 4f 47 3b 49 73 43 6c # nceName;DOG;IsCl
 * < 000000a0 75 73 74 65 72 65 64 3b 4e 6f 3b 56 65 72 73 69 # ustered;No;Versi
 * < 000000b0 6f 6e 3b 38 2e 30 30 2e 31 39 34 3b 74 63 70 3b # on;8.00.194;tcp;
 * < 000000c0 33 35 34 36 3b 6e 70 3b 5c 5c 42 52 49 4e 4b 4c # 3546;np;\\BRINKL
 * < 000000d0 45 59 5c 70 69 70 65 5c 4d 53 53 51 4c 24 44 4f # EY\pipe\MSSQL$DO
 * < 000000e0 47 5c 73 71 6c 5c 71 75 65 72 79 3b 3b          # G\sql\query;;
 * </pre>
 */
public class MSSqlServerInfo
{
    private int m_numRetries             = 3;
    private int m_timeout                = 2000;
    private String[] m_serverInfoStrings = null;

    public MSSqlServerInfo(String host) throws SQLException
    {
        try
        {
            InetAddress addr = InetAddress.getByName(host);
            DatagramSocket socket = new DatagramSocket();
            byte[] msg = new byte[] { 0x02 };
            DatagramPacket p = new DatagramPacket( msg, msg.length, addr, 1434 );
            socket.send(p);
            byte[] buf = new byte[4096];
            p = new DatagramPacket( buf, buf.length );
            socket.setSoTimeout(m_timeout);
            for( int i=0; i < m_numRetries; i++ )
            {
                try
                {
                    socket.receive(p);
                    String infoString = extractString(buf, p.getLength());
                    m_serverInfoStrings = split(infoString, ';');
                    return;
                }
                catch ( InterruptedIOException toEx ) {}
            }
        }
        catch (Exception e)
        {
            SQLException sqlEx =
                new SQLException( "Unable to get info from SQL Server:" + host);
            // SAfe initCause is only part of J2SE 1.4, this won't work on 1.3 or lower
            // sqlEx.initCause(e);
            throw sqlEx;
        }

        throw new SQLException("Unable to get info from SQL Server:" + host);
    }

    /**
     * Call getInfo() before calling this method. It parses the info string
     * returned from SQL Server and looks for the port for the given named
     * instance. If it can't be found, or if getInfo() hadsn't been called,
     * it returns -1.
     * @param instanceName
     * @return port the given instance is listening on, or -1 if it can't be
     * found.
     */
    public int getPortForInstance(String instanceName) throws TdsException {
        if (m_serverInfoStrings == null) {
            return -1;
        }

        //NOTE: default instance is called MSSQLSERVER
        if (instanceName == null || instanceName.length() == 0) {
            instanceName = "MSSQLSERVER";
        }

        String curInstance = null;
        String curPort = null;

        for (int index = 0; index < m_serverInfoStrings.length; index++) {
            if (m_serverInfoStrings[index].length() == 0) {
                curInstance = null;
                curPort = null;
            } else {
                String key = m_serverInfoStrings[index];
                index++;
                String value = "";

                if (index < m_serverInfoStrings.length) {
                    value = m_serverInfoStrings[index];
                }

                if (key.equals("InstanceName")) {
                    curInstance = value;
                }

                if (key.equals("tcp")) {
                    curPort = value;
                }
                
                if (curInstance != null
                    && curPort != null
                    && curInstance.equalsIgnoreCase(instanceName)) {

                    try {
                        return Integer.parseInt(curPort);
                    } catch (NumberFormatException e) {
                        throw new TdsException("Could not parse instance port number ["
                            + instanceName + "].");
                    }
                }
            }
        }
        //didn't find it...
        return -1;
    }

    private static final String extractString(byte[] buf, int len)
    {
        // the first three bytes are unknown; after that, it should be a narrow string...
        final int headerLength = 3;

        return new String( buf, headerLength, len-headerLength);
    }

    public static String[] split(String s, int ch)
    {
        int size = 0;
        for( int pos=0; pos!=-1; pos=s.indexOf(ch, pos+1), size++ );

        String res[] = new String[size];
        int i=0, p1=0, p2=s.indexOf(ch);
        do
        {
            res[i++] = s.substring(p1, p2==-1 ? s.length() : p2);
            p1=p2+1;
            p2=s.indexOf(ch, p1);
        }
        while( p1 != 0 );

        return res;
    }
}
