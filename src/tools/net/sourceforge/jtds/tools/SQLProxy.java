package net.sourceforge.jtds.tools;

import java.io.*;
import java.net.*;

public class SQLProxy
{
    static final int    LOCAL_PORT  = 1433;

    static final String SERVER      = "server";
    static final int    SERVER_PORT = 1433;

    public static void main(String args[]) throws IOException
    {
        ServerSocket sock = new ServerSocket(LOCAL_PORT);
        while( true )
        {
            Socket s = sock.accept();
            new DumpThread(s).start();
        }
    }
}

class DumpThread extends Thread
{
    private Socket client, server;
    private byte[] buf = new byte[4096];

    DumpThread(Socket client)
    {
        this.client = client;
    }

    String hex(int value, int len)
    {
        String res = Integer.toHexString(value);
        while( res.length() < len )
            res = '0' + res;
        return res.toUpperCase();
    }

    String packetType(int value)
    {
        switch( value )
        {
            case 0x01:
                return "4.2 or 7.0 query";
            case 0x02:
                return "4.2 or 7.0 login packet";
            case 0x04:
                return "Server response";
            case 0x06:
                return "Cancel";
            case 0x0F:
                return "5.0 query";
            case 0x10:
                return "7.0 login packet";
            default:
                return "Unknown (0x"+hex(value, 2)+')';
        }
    }

    int passByte(InputStream in, OutputStream out) throws IOException
    {
        int res = in.read();
        out.write(res);
        return res;
    }

    void skip(int len, InputStream in, OutputStream out) throws IOException
    {
        while( len > 0 )
        {
            int rd = in.read(buf, 0, len>buf.length ? buf.length : len);
//            for( int i=0; i<rd; i++ )
//                System.out.print((char)buf[i]);
//            System.out.println();
//            for( int i=0; i<rd; i++ )
//                System.out.print(hex(((int)buf[i])&0xFF, 2)+" ");
//            System.out.println();
            out.write(buf, 0, rd);
            len -= rd;
        }
    }

    boolean nextPacket(InputStream in, OutputStream out) throws IOException
    {
        int type = passByte(in, out),
            last = passByte(in, out);

        int size = passByte(in, out)<<8 | passByte(in, out);

        buf[0] = (byte)passByte(in, out);
        buf[1] = (byte)passByte(in, out);
        buf[2] = (byte)passByte(in, out);
        buf[3] = (byte)passByte(in, out);

        System.out.println("Packet type: "+packetType(type));
        System.out.println("Packet size: "+size+" (0x"+hex(size, 4)+")");
        System.out.print("Rest of header:");
        for( int i=0; i<4; i++ )
            System.out.print(" 0x"+hex(((int)buf[i])&0xFF, 2));
        System.out.println();

        skip(size-8, in, out);

        return last==0;
    }

    public void run()
    {
        try
        {
            server = new Socket(SQLProxy.SERVER, SQLProxy.SERVER_PORT);

            InputStream cIn = client.getInputStream();
            OutputStream cOut = client.getOutputStream();
            InputStream sIn = server.getInputStream();
            OutputStream sOut = server.getOutputStream();

            while( true )
            {
                while( nextPacket(cIn, sOut) );
                System.out.println("--- Client done. ---\n");
                while( nextPacket(sIn, cOut) );
                System.out.println("--- Server done. ---\n");
            }
        }
        catch( IOException ex )
        {
            try{ client.close(); }catch( IOException exc ){}
            try{ server.close(); }catch( IOException exc ){}
            System.out.println("Disconnected.");
        }
    }
}
