//
// Copyright 1998 CDS Networks, Inc., Medford Oregon
//
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
// 1. Redistributions of source code must retain the above copyright
//    notice, this list of conditions and the following disclaimer.
// 2. Redistributions in binary form must reproduce the above copyright
//    notice, this list of conditions and the following disclaimer in the
//    documentation and/or other materials provided with the distribution.
// 3. All advertising materials mentioning features or use of this software
//    must display the following acknowledgement:
//      This product includes software developed by CDS Networks, Inc.
// 4. The name of CDS Networks, Inc.  may not be used to endorse or promote
//    products derived from this software without specific prior
//    written permission.
//
// THIS SOFTWARE IS PROVIDED BY CDS NETWORKS, INC. ``AS IS'' AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED.  IN NO EVENT SHALL CDS NETWORKS, INC. BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
// OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
// HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
// LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
// OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
// SUCH DAMAGE.
//


package com.internetcds.jdbc.tds;

import java.io.*;
import java.net.*;
import com.internetcds.util.HexDump;
import com.internetcds.util.Logger;
import java.sql.Timestamp;

/**
 * Handle the communications for a Tds instance.
 *
 * @version  $Id: TdsComm.java,v 1.3 2001-09-10 06:08:18 aschoerk Exp $
 * @author Craig Spannring
 * @author Igor Petrovski
 */
public class TdsComm implements TdsDefinitions
{
   public static final String cvsVersion = "$Id: TdsComm.java,v 1.3 2001-09-10 06:08:18 aschoerk Exp $";


   static final int headerLength = 8;

   //
   // The following constants are the packet types.
   //
   // They are the first databayte in the packet and
   // define the type of data in that packet.
   public static final byte QUERY  = 1;
   public static final byte LOGON  = 2;
   public static final byte PROC   = 3;
   public static final byte REPLY  = 4;
   public static final byte CANCEL = 6;
   public static final byte LOGON70 = 16;  // Added 2000-06-05


   // The minimum packet length that a TDS implementation can support
   // is 512 bytes.  This implementation will not support packets longer
   // than 512.  This will simplify the connection negotiation.
   //
   // XXX Some future release of this driver should be modified to
   // negotiate longer packet sizes with the DB server.
   private static final int maxPacketLength = 512;

   // in and out are the sockets used for all communication with the
   // server.
   private DataOutputStream  out = null;
   private DataInputStream   in  = null;


   // outBuffer is used to construct the physical packets that will
   // be sent to the database server.
   byte  outBuffer[];
   
   int   outBufferLen;

   // nextOutBufferIndex is an index into outBuffer where the next
   // byte of data will be stored while constructing a packet.
   int   nextOutBufferIndex = 0;

   // The type of the TDS packet that is being constructed
   // in outBuffer.
   int   packetType   = 0;


   // Place to store the incoming data from the DB server.
   byte  inBuffer[];

   // index of next byte that will be 'read' from inBuffer
   int   inBufferIndex = 0;

   // Total Number of bytes stored in inBuffer.  (The number includes bytes
   // that have been 'read' as well as bytes that still need to be 'read'.
   int   inBufferLen   = 0;

   // Track how many packets we have sent and received
   int    packetsSent     = 0;
   int    packetsReceived = 0;

   // For debuging purposes it would be nice to uniquely identify each Tds
   // stream.  id will be a unique value for each instance of this class.
   static int    id = 0;

   // Added 2000-06-07.  Used to control TDS version-specific behavior.
   private int   tdsVer = TDS42;

   public TdsComm(Socket sock, int tdsVer_)
      throws java.io.IOException
   {
      out = new DataOutputStream(sock.getOutputStream());
      in  = new DataInputStream(sock.getInputStream());

      outBufferLen = maxPacketLength;
      outBuffer = new byte[outBufferLen];
      inBuffer  = new byte[maxPacketLength];

      // Added 2000-06-07
      tdsVer    = tdsVer_;

      id++;
   }

   public void close()
   {
      // nop for now.
   }

   /*
   private void clearInputStream() throws IOException
   {
     int leftOver = in.available();
     if (leftOver > 0) {
       byte[] tmpBuffer = new byte[leftOver];
       for (int tmpread = 0; tmpread < leftOver; )
       {
         int num = in.read(tmpBuffer, tmpread, leftOver - tmpread);
         if (num < 0) break;
         tmpread += num;
       }
       if (Logger.isActive())
       {
         String  dump = com.internetcds.util.HexDump.hexDump(tmpBuffer, leftOver);
         String  t    = (new Timestamp(
             System.currentTimeMillis())).toString();
  
         Logger.println("Instance "  + id + " @ " + t
                         + " recevied leftOver" 
                         + "\n" + dump);
       }
     }   
   }
   */



   /**
    * start a TDS packet.
    *
    * <br>
    * This method should be called to start a logical TDS packet.
    *
    * @param type   Type of the packet.  Can be QUERY, LOGON, PROC,
    *               REPLY, or CANCEL.
    */
   public synchronized void startPacket(int type)
   {
     
     inBufferIndex = inBufferLen;  // drop all leftovers !!
     /*
      try {
        clearInputStream();
      }
      catch (IOException e) {
        e.printStackTrace();
      }
      */
            
      // Only one thread at a time can be building an outboudn packet.
      // This is primarily a concern with building cancel packets.
      //  XXX: as why should more than one thread work with the same tds-stream ??? would be fatal anyway
      while(someThreadIsBuildingPacket())
      {
         try
         {
            wait();
         }
         catch (java.lang.InterruptedException e)
         {
            // nop
         }
      }
       
      packetType    = type;
      nextOutBufferIndex  = headerLength;
   }

   /**
    * Is some thread currently building a logical TDS packet?
    *
    * @return true iff a packet is being built.
    */
   public boolean someThreadIsBuildingPacket()
   {
      return packetType!=0;
   }


   /**
    * append a byte onto the end of the logical TDS packet.
    *
    * <p>
    * Append a byte onto the end of the logical TDS packet.  When a
    * physical packet is full send it to the server.
    *
    * @param b  byte to add to the TDS packet
    */
   public void appendByte(byte b)
      throws java.io.IOException
   {
      if (nextOutBufferIndex == outBufferLen)
      {
         // If we have a full physical packet then ship it out to the
         // network.
         sendPhysicalPacket(false);
         nextOutBufferIndex = headerLength;
      }

      storeByte(nextOutBufferIndex, b);
      nextOutBufferIndex++;
   } // appendByte()


   /**
    * append an array of bytes onto the end of the logical TDS packet.
    *
    * @param b  bytes to add to the TDS packet
    */
   public void appendBytes(byte[] b)
      throws java.io.IOException
   {
      appendBytes(b, b.length, (byte)0);
   } // appendBytes()



   /**
    * append an array of bytes onto the end of the logical TDS packet.
    *
    * @param b   bytes to add to the TDS packet
    * @param len maximum number of bytes to transmit
    * @param pad fill with this byte until len is reached
    */
   public void appendBytes(byte[] b, int len, byte pad)
      throws java.io.IOException
   {
      int i = 0;
      for (; i<b.length && i<len; i++)
      {
         appendByte(b[i]);
      }
      for (; i<len; i++)
      {
         appendByte(pad);
      }
   }


   /**
    * append a short int onto the end of the logical TDS packet.
    * <p>
    * @param s   short int to add to the TDS packet
    */
   public void appendShort(short s)
      throws java.io.IOException
   {
      appendByte((byte)((s>>8)&0xff));
      appendByte((byte)((s>>0)&0xff));
   }

   /**
    * Appends a short int onto the end of the logical TDS packet.
    * <p>
    * @param s   short int to add to the TDS packet
    */
   public void appendTdsShort(short s)
      throws java.io.IOException
   {
      appendByte((byte)((s>>0)&0xff));
      appendByte((byte)((s>>8)&0xff));
   }


   /**
    * append a Double onto the end of the logical TDS packet.
    * <p>
    * Append the Double value onto the end of the TDS packet as a
    * SYBFLT8.
    *
    * @param value   Double to add to the TDS packet
    */
   public void appendFlt8(Double value)
      throws java.io.IOException
   {
      long  l = Double.doubleToLongBits(value.doubleValue());

      appendByte((byte)((l>>0)&0xff));
      appendByte((byte)((l>>8)&0xff));
      appendByte((byte)((l>>16)&0xff));
      appendByte((byte)((l>>24)&0xff));
      appendByte((byte)((l>>32)&0xff));
      appendByte((byte)((l>>40)&0xff));
      appendByte((byte)((l>>48)&0xff));
      appendByte((byte)((l>>56)&0xff));
   }

   public void appendInt(int i)
      throws java.io.IOException
   {
      appendByte((byte)((i>>24)&0xff));
      appendByte((byte)((i>>16)&0xff));
      appendByte((byte)((i>>8)&0xff));
      appendByte((byte)((i>>0)&0xff));
   }

   public void appendTdsInt(int i)
      throws java.io.IOException
   {
      appendByte((byte)((i>>0)&0xff));
      appendByte((byte)((i>>8)&0xff));
      appendByte((byte)((i>>16)&0xff));
      appendByte((byte)((i>>24)&0xff));
   }


   public void appendInt64(long i)
      throws java.io.IOException
   {
      appendByte((byte)((i>>56)&0xff));
      appendByte((byte)((i>>48)&0xff));
      appendByte((byte)((i>>40)&0xff));
      appendByte((byte)((i>>32)&0xff));
      appendByte((byte)((i>>24)&0xff));
      appendByte((byte)((i>>16)&0xff));
      appendByte((byte)((i>>8)&0xff));
      appendByte((byte)((i>>0)&0xff));
   }

   /**
    * Appends the 16-bit characters from the caller's string, without
    * narrowing the characters.
    *
    * Sybase let's the client decide what byte order to use but it \
    * appears that SQLServer 7.0 little-endian byte order.
    *
    * Added 2000-06-05
    */
   public void appendChars(String s) throws java.io.IOException {
       for (int i = 0; i < s.length(); ++i) {
           int c = s.charAt(i);
           byte b1 = (byte)(c & 0xFF);
           byte b2 = (byte)((c >> 8) & 0xFF);
           appendByte(b1);
           appendByte(b2);
       }
   }

    /*
     * Stefan Bodewig 2000-06-21
     *
     * removed appendString() to keep the encoding to and from the
     * server charset in on place - i.e. Tds.
     *
     * It had to be Tds as we need to specify the length for the
     * String as well, sometimes before we send the actual data,
     * sometimes after we've sent them.
     *
     * If we need to know the length beforehand in Tds, we'd have to
     * convert the data twice, once to get the length and once to send
     * them.
     */
//   public void appendString(
//      String s,
//      int    length,
//      byte   pad)
//      throws java.io.IOException
//   {
//      int   i;
//      byte  dst[];
//
//
//      dst = encoder.getBytes(s.substring(0, (length<=s.length() ? length
//                                             : s.length())));
//
//      for(i=0; i<dst.length; i++)
//      {
//         appendByte(dst[i]);
//      }
//
//      for(; i<length; i++)
//      {
//         appendByte(pad);
//      }
//   }


   /**
    * Send the logical packet.
    * <p>
    * Send the logical packet the has been constructed.  */
   public synchronized void sendPacket()
      throws java.io.IOException
   {
      sendPhysicalPacket(true);
      nextOutBufferIndex = 0;
      packetType   = 0;
      notify();
   }


   /**
    * store a byte of data at a particular location in the outBuffer.
    *
    * @param index position in outBuffer to store data
    * @param value value to store in the outBuffer.
    */
   private void storeByte(
      int   index,
      byte  value)
   {
      outBuffer[index] = value;
   }

   /**
    * store a short integer of data at a particular location in the outBuffer.
    *
    * @param index position in outBuffer to store data
    * @param value value to store in the outBuffer.
    */
   private void storeShort(
      int   index,
      short s)
   {
      outBuffer[index] = (byte)((s>>8) & 0xff);
      outBuffer[index+1] = (byte)((s>>0) & 0xff);
   }


   /**
    * send the data in the outBuffer.
    * <p>
    * Fill in the TDS packet header data and send the data in outBuffer
    * to the DB server.
    *
    * @param isLastSegment is this the last physical packet that makes
    *        up the physical packet?
    */
   private void sendPhysicalPacket(boolean isLastSegment)
      throws java.io.IOException
   {
      if (nextOutBufferIndex>headerLength
         || packetType == CANCEL)
      {
         // packet type
         storeByte(0, (byte)(packetType & 0xff));
         storeByte(1, isLastSegment ? (byte)1 : (byte)0);
         storeShort(2, (short)nextOutBufferIndex);
         storeByte(4, (byte)0);
         storeByte(5, (byte)0);
         storeByte(6, (byte)(tdsVer == TDS70 ? 1 : 0));
         storeByte(7, (byte)0);

         out.write(outBuffer, 0, nextOutBufferIndex);
         packetsSent++;

         if (Logger.isActive())
         {
            String  dump = HexDump.hexDump(outBuffer, nextOutBufferIndex);
            String  t    = (new Timestamp(
               System.currentTimeMillis())).toString();
            Logger.println("Instance " + id + " @ " + t
                           + " sent packet #" + packetsSent + "\n" + dump);
         }
      }
   }

   /**
    * peek at the next byte of data.
    * <p>
    * This returns the next byte of data that would be returned
    * by getByte(), but does not actually consume the data.
    *
    * <b>Note-</b>  We can't synchronize this method (or most of the other
    * methods in this class) because of the way cancels are handled.
    * If a thread is waiting for a response from the server the
    * cancelController class must be able to call sendPacket() to
    * cancel the request.
    *
    * @return The next byte of data that will be returned by getByte()
    *
    * @exception com.internetcds.jdbc.tds.TdsException
    * @exception java.io.IOException
    */
   public byte peek()
      throws  com.internetcds.jdbc.tds.TdsException,
      java.io.IOException
   {
      // XXX RACE CONDITION-  It is possible that two threads
      // could be modifying inBuffer at the same time.  We need
      // to synchronized based on inBuffer itself.
      byte   result = getByte();
      backup();
      return result;
   }


   /**
    * put the most recently read byte of data back in the inBuffer.
    * <p>
    * This function effectivly ungets the last byte read.
    * It is guaranteed to be able to unget the last byte read.
    * Trying to unget multiple bytes is not recomended and will
    * only work so long as all the bytes were in the same
    * physical TDS network packet.
    *
    * @author Craig Spannring
    */
   public void backup()
   {
      inBufferIndex--;

      // make sure we have fallen of the beginning of the buffer
      // throw an array out of bounds error if we've gone back too far.
      byte  b = inBuffer[inBufferIndex];
   }


   /**
    * read a byte of data from the DB server.
    * <p>
    * This will return the next byte of data from the DB server.
    * <p>
    * <B>Warning</B>  If there is not data available this method
    * will block.
    */
   public byte getByte()
      throws  com.internetcds.jdbc.tds.TdsException,
      java.io.IOException
   {
      byte   result;

      if (inBufferIndex >= inBufferLen)
      {
         // out of data, read another physical packet.
         getPhysicalPacket();
      }

      result = inBuffer[inBufferIndex++];
      return result;
   }

   public byte[] getBytes(int len)
      throws com.internetcds.jdbc.tds.TdsException,
      java.io.IOException
   {
      byte result[] = new byte[len];
      int  i;

      for(i=0; i<len; i++)
      {
         result[i] = getByte();
      }
      return result;
   }

   /**
    * Reads bytes or characters (depending on TDS version) and constructs a
    * string with them.
    *
    * Sybase will let the client choose byte ordering, but SQLServer 7.0
    * wants little endian only.  In the interest of simplicity, just use
    * little endian regardless of the type of server.
    *
    * Added 2000-06-05.
    */
   public String getString(int len)
      throws com.internetcds.jdbc.tds.TdsException,
      java.io.IOException
   {
      if (tdsVer == TDS70) {
         char[] chars = new char[len];
         for (int i = 0; i < len; ++i) {
            int lo = getByte() & 0xFF;
            int hi = getByte() & 0xFF;
            chars[i] = (char)(lo | (hi << 8));
         }
         return new String(chars);
      }
      else
         return new String(getBytes(len));
   }

   public void skip(int i)
      throws com.internetcds.jdbc.tds.TdsException,
      java.io.IOException
   {
      for(; i>0; i--)
      {
         getByte();
      }
   } // skip()

   public int getNetShort()
      throws TdsException, java.io.IOException
   {
      byte tmp[] = new byte[2];
      tmp[0] = getByte();
      tmp[1] = getByte();
      return ntohs(tmp, 0);
   }

   public int getTdsShort()
      throws com.internetcds.jdbc.tds.TdsException, java.io.IOException
   {
      int lo = ((int)getByte() & 0xff);
      int hi = ((int)getByte() & 0xff) << 8;
      return lo | hi;
   }

   public int getTdsInt()
      throws com.internetcds.jdbc.tds.TdsException, java.io.IOException
   {
      int   result;

      int b1 = ((int)getByte() & 0xff);
      int b2 = ((int)getByte() & 0xff) << 8;
      int b3 = ((int)getByte() & 0xff) << 16;
      int b4 = ((int)getByte() & 0xff) << 24;

      result =  b4 | b3 | b2 | b1;

      return result;
   }

   public long getTdsInt64()
      throws com.internetcds.jdbc.tds.TdsException, java.io.IOException
   {
      long b1 = ((long)getByte() & 0xff);
      long b2 = ((long)getByte() & 0xff) << 8;
      long b3 = ((long)getByte() & 0xff) << 16;
      long b4 = ((long)getByte() & 0xff) << 24;
      long b5 = ((long)getByte() & 0xff) << 32;
      long b6 = ((long)getByte() & 0xff) << 40;
      long b7 = ((long)getByte() & 0xff) << 48;
      long b8 = ((long)getByte() & 0xff) << 56;
      return b1 | b2 | b3 | b4 | b5 | b6 | b7 | b8;
   }


   /**
    * convert two bytes in a byte array into a Java short integer.
    *
    * @param buf array of data
    * @param offset index into the buf array where the short integer
    *               is stored.
    */
   private static int ntohs(byte buf[], int offset)
   {
      int  lo = ((int)buf[offset+1] & 0xff);
      int  hi = (((int)buf[offset] & 0xff) << 8);

      return hi | lo;  // return an int since we really want an _unsigned_
   }

    /** @todo Does this need to be synchronized? */
    byte   tmpBuf[] = new byte[8];

   /**
    * Read a physical packet.
    * <p>
    * <B>Warning</B> This method will block until it gets all of the input.
    */
   private synchronized void getPhysicalPacket()
      throws TdsException, java.io.IOException
   {

      // read the header
      for (int nread = 0; nread < 8; )
      {
         nread += in.read(tmpBuf, nread, 8 - nread);
      }


      if (Logger.isActive())
      {
         String  dump = com.internetcds.util.HexDump.hexDump(tmpBuf, 8);
         String  t    = (new Timestamp(
            System.currentTimeMillis())).toString();

         Logger.println("Instance "  + id + " @ " + t
                        + " recevied header #" + (packetsReceived+1)
                        + "\n" + dump);
      }

      byte   packetType = tmpBuf[0];
      if (packetType!=LOGON
          && packetType!=QUERY
          && packetType!=REPLY)
      {
         throw new TdsUnknownPacketType(packetType, tmpBuf);
      }

      // figure out how many bytes are remaining in this packet.
      int len = ntohs(tmpBuf, 2) - 8;

      // Added 2000-06-05
      if (len >= inBuffer.length)
      {
          inBuffer = new byte[len];
      }

      if (len < 0)
      {
         throw new TdsException("Confused by a length of " + len);
      }

      // now get the data
      for (int nread = 0; nread < len; )
      {
         nread += in.read(inBuffer, nread, len - nread);
      }
      packetsReceived++;


      // adjust the bookkeeping info about the incoming buffer
      inBufferLen   = len;
      inBufferIndex = 0;

      if (Logger.isActive())
      {
         String  dump = com.internetcds.util.HexDump.hexDump(inBuffer, len);
         String  t    = (new Timestamp(
            System.currentTimeMillis())).toString();

         Logger.println("Instance "  + id + " @ " + t 
                        + " recevied data #" + (packetsReceived)
                        + "\n" + dump);
      }
   }
   void resizeOutbuf(int newsize) {
     if (newsize > outBufferLen) {
       byte[] newBuf = new byte[newsize];
       System.arraycopy(outBuffer,0,newBuf,0,outBufferLen);
       outBufferLen = newsize;
       outBuffer = newBuf;
     }
   }
}

