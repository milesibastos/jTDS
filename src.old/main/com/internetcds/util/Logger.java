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

import java.io.*;


/**
 * This class will log messages into a file.
 *
 * @version $Id: Logger.java,v 1.5 2002-09-18 16:27:11 alin_sinpalean Exp $
 * @author Craig Spannring
 */
public class Logger
{
   public static final String cvsVersion = "$Id: Logger.java,v 1.5 2002-09-18 16:27:11 alin_sinpalean Exp $";

   private static String       filename = "log.out";
   private static boolean      active   = false;
   private static PrintStream  out      = null;

   /**
    * Initialize the logger facility.
    * <p>
    * If the log facility hasn't been initialized yet this routine will
    * open the log file.
    * <p>
    * The routine must be called before any logging takes place.
    * All of the functions in this class that log messages should
    * call this routine.  It doesn't hurt anything if this is called
    * multiple times.
    */
   private static void init()
   {
      // check to see if the file is already open
      if( out == null )
         try
         {
            if( filename != null )
               out = new PrintStream(new FileOutputStream(filename), true);
         }
         catch( FileNotFoundException ex )
         {
            // Ignore
         }
         finally
         {
            if( out == null )
               out = System.out;
         }
   }

   /**
    * Turn the logging on or off.
    * <p>
    * The first time logging is turned on it will create the log file.
    *
    * @param value   when value is true it will turn the logging on,
    *                if it is false it will turn the logging off.
    */
   synchronized public static void setActive(boolean value)
   {
      if( value )
         init();
      else
         if( out != null )
            out.close();

      active = value;
   }

   /**
    * Is logging turned on?
    */
   public static boolean isActive()
   {
      return active;
   }

   /**
    * set the name of the log file.
    * <p>
    * This method allows you to set the name of the log file.
    * <B>Note-</B> Once the log file is open you can not change the
    * name.
    *
    * @param value  name of the log file.
    */
   public synchronized static void setFilename(String value)
   {
      if( filename != value )
      {
         filename = value;
         if( out != null )
            out.close();
      }
   }

   /**
    * return the name of the log file.
    */
   public static String getFilename()
   {
      return filename;
   }

   /**
    * Print a string into the log file if and only if logging is active
    */
   synchronized public static void print(String msg)
   {
      if (active)
      {
         init();
         out.print(msg);
      }
   }

   /**
    * Print a string into the log file if and only if logging is active
    */
   synchronized public static void println(String msg)
   {
      if (active)
      {
         init();
         out.println(msg);
      }
   }
}
