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


package com.internetcds.util;


/**
 * A simple class to convert a raw buffer to a hex dump
 *
 * @version $Id: HexDump.java,v 1.4 2001-09-14 16:05:03 aschoerk Exp $
 * @author Craig Spannring
 */ 
public class HexDump
{
   public static final String cvsVersion = "$Id: HexDump.java,v 1.4 2001-09-14 16:05:03 aschoerk Exp $";

   static void appendByteToHexString(StringBuffer res, byte b)
   {
      appendIntToHexString(res, b, 2, '0');
   } /* byteToHexString()  */
   
   static void appendIntToHexString(StringBuffer res, int num, int width, char fill)
   {
      int      i;

      int insertpoint = res.length();
      if (num==0)
      {
         res.append('0');
         width--;
      }
      else
      {
         while(num!=0 && width>0)
         {
            String  tmp = Integer.toHexString(num & 0xf);
            res.insert(insertpoint,tmp);
            // result = tmp + result;
            num = (num>>4);
            width--;
         }
      }
      for(; width>0; width--)
      {
        res.insert(insertpoint,fill);
         // result = fill + result;
      }
   } /* intToHexString()  */

   public static String hexDump(byte data[])
   {
      return hexDump(data, data.length);
   }


   public static String hexDump(byte data[], int length)
   {
      String     str;
      int        i;
      int        j;  
      final int  bytesPerLine = 16;
      StringBuffer     result = new StringBuffer(length + 4);

      
      for(i=0; i<length; i+=bytesPerLine)
      {
         // print the offset as a 4 digit hex number
         appendIntToHexString(result,i, 4, '0'); result.append("  ");

         // print each byte in hex
         for(j=i; j<length && (j-i)<bytesPerLine; j++)
         {
            appendByteToHexString(result,data[j]); result.append(" ");
         }

         // skip over to the ascii dump column
         for(; 0!=(j % bytesPerLine); j++)
         {
            result.append("   ");
         }
         result.append("  |");         

         // print each byte in ascii
         for(j=i; j<length && (j-i)<bytesPerLine; j++)
         {
            if (((data[j] & 0xff) > 0x001f) && ((data[j] & 0xff) < 0x007f))
            {
               // Character ch = new Character();
               result.append((char) data[j]);
            }
            else
            {
               result.append(".");
            }
         }
         result.append("|\n");
      }
      return result.toString();
   } /* hexDump()  */
}
