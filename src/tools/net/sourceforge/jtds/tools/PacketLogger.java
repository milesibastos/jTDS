package net.sourceforge.jtds.tools;

import java.io.*;

public class PacketLogger
{
    PrintStream out;

    static String hexstring = "0123456789ABCDEF";
    static String hex(byte b)
    {
        int ln = (int)(b & 0xf);
        int hn = (int)((b & 0xf0) >> 4);
        return "" + hexstring.charAt(hn) + hexstring.charAt(ln);
    }
    static String hex(short b)
    {
        byte lb = (byte)(b & 0x00ff);
        byte hb = (byte)((b & 0xff00) >> 8);
        return hex(hb) + hex(lb);
    }
    public PacketLogger(String filename) throws IOException
    {
        out = new PrintStream(new FileOutputStream(new File(filename)));
    }

    public void log(byte[] packet)
    {
        short pos = 0;
        while (pos < packet.length)
        {
            out.print(hex(pos) + ": ");
            short startpos = pos;
            pos += 16;
            if (pos > packet.length)
                pos = (short)packet.length;
            for (short i = startpos; i < pos; i++)
            {
                out.print(hex(packet[i]) + " ");
            }
            for (short i = pos; i < startpos + 16; i++)
                out.print("   ");
            out.print("    ");
            for (short i = startpos; i < startpos + 16; i++)
            {
                if (i >= pos)
                    out.print(" ");
                else if (packet[i] < 32)
                    out.print(".");
                else
                    out.print((char)packet[i]);
            }
            out.println("");
        }
        out.println("");
    }
}
