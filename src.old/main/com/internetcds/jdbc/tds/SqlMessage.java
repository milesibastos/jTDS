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


import java.sql.*;



/**
 * helper class for printing sql messages
 */

class SqlMessage
{
   public static final String cvsVersion = "$Id: SqlMessage.java,v 1.3 2002-09-16 11:13:43 alin_sinpalean Exp $";


   int    number;
   int    state;
   int    severity;
   String message;
   String server;
   String procName;
   int    line;

   /**
    *  Convert a sql message from the server into a human readable string
    *
    * @return human readable string of the SQLServer message.
    */
   public String toString()
   {
      return
         "Msg " + number + ", " +
         "Severity " + severity + ", " +
         "State " + state + ", " +
         "" + message + ", " +
         "Server " + server + ", " +
         "Procedure " + procName + ", " +
         "Line " + line;
   }

   public java.sql.SQLWarning toSQLWarning()
   {
      // XXX have to come up with the X/OPEN sql message strings
      // for now just use S1000 for everything
      return new java.sql.SQLWarning(message, "S1000", number);
   }

   public java.sql.SQLException toSQLException()
   {
      return new java.sql.SQLException(message, "S1000", number);
   }
}
