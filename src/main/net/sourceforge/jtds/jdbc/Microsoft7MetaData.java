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


package net.sourceforge.jtds.jdbc;



import java.sql.*;


public class Microsoft7MetaData extends DatabaseMetaData
{
   public static final String cvsVersion = "$Id: Microsoft7MetaData.java,v 1.1 2002-10-14 10:48:59 alin_sinpalean Exp $";


   protected Microsoft7MetaData(
      TdsConnection connection_,
      Tds           tds_) throws SQLException
   {
      super(connection_, tds_);
      sysnameLength = 128;
   }


   /**
    * What's the maximum number of columns in a "GROUP BY" clause?
    *
    * @return max number of columns
    * @exception SQLException if a database-access error occurs.
    */
   public int getMaxColumnsInGroupBy() throws SQLException
   {
      // Limited only by the number of bytes
      return 0; // per "SQL Server Books Online" Maximum Capacity Specifications
   }


   /**
    * What's the maximum number of columns in an "ORDER BY" clause?
    *
    * @return max columns
    * @exception SQLException if a database-access error occurs.
    */
   public int getMaxColumnsInOrderBy() throws SQLException
   {
      // Limited only by the number of bytes
      return 0; // per "SQL Server Books Online" Maximum Capacity Specifications
   }


   /**
    * What's the maximum number of columns in a table?
    *
    * @return max columns
    * @exception SQLException if a database-access error occurs.
    */
   public int getMaxColumnsInTable() throws SQLException
   {
      // XXX How do we find this out for Sybase?
      return 1024; // per "SQL Server Books Online" Maximum Capacity Specifications
   }


   /**
    * What's the maximum length of a single row?
    *
    * @return max row size in bytes
    * @exception SQLException if a database-access error occurs.
    */
   public int getMaxRowSize() throws SQLException
   {
      return 8060; // per "SQL Server Books Online" Maximum Capacity Specifications
   }


   /**
    * What's the maximum number of tables in a SELECT?
    *
    * @return the maximum
    * @exception SQLException if a database-access error occurs.
    */
   public int getMaxTablesInSelect() throws SQLException
   {
      return 256; // per "SQL Server Books Online" Maximum Capacity Specifications
   }


   /**
    * Is "ALTER TABLE" with drop column supported?
    *
    * @return true if so
    * @exception SQLException if a database-access error occurs.
    */
   public boolean supportsAlterTableWithDropColumn() throws SQLException
   {
      return true;
   }
}
