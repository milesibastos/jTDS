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


/**
 * Extend the java.io.InputStream class for the TDS driver.
 * <p>
 * This class is used the the <code>getAsciiStream</code>
 * of the <A href=ResultSet.html#getAsciiStream>ResultSet</A> class.
 * <p>
 * The current implementation reads the entire column of data into a 
 * String.  This limits the maximum BLOB size to the largest BLOB 
 * that will fit in memory.
 *
 * @author Craig Spannring
 * @version  $Id: TdsAsciiInputStream.java,v 1.2 2001-08-31 12:47:20 curthagenlocher Exp $
 */
class TdsAsciiInputStream extends java.io.InputStream
{
   public static final String cvsVersion = "$Id: TdsAsciiInputStream.java,v 1.2 2001-08-31 12:47:20 curthagenlocher Exp $";

   // The data that will be read from the stream.
   String   data;

   // next is the index into data of the next byte of data in the stream
   int      next = 0;

   public TdsAsciiInputStream(String data_)
   {
      data = data_;
      next = 0;
   }


   public String getData()
   {
      return data;
   }


   public String toString()
   {
      return getData();
   }


   public int read()
      throws java.io.IOException
   {
      int   result;

      if (data == null)
      {
         throw new java.io.IOException("stream was null");
      }
      else if (next == data.length())
      {
         result = -1;
      }
      else
      {
         result = data.charAt(next);
         next++;
      }
      return result;
   }
}
