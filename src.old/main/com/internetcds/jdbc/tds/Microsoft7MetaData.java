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


public class Microsoft7MetaData extends DatabaseMetaData
{
   public static final String cvsVersion = "$Id: Microsoft7MetaData.java,v 1.1 2001-09-18 08:38:07 aschoerk Exp $";


   protected Microsoft7MetaData(
      Object        connection_,
      Tds           tds_)
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


   /**
    * Get a description of stored procedures available in a
    * catalog.
    *
    * <P>Only procedure descriptions matching the schema and
    * procedure name criteria are returned.  They are ordered by
    * PROCEDURE_SCHEM, and PROCEDURE_NAME.
    *
    * <P>Each procedure description has the the following columns:
    *  <OL>
    *   <LI><B>PROCEDURE_CAT</B> String => procedure catalog (may be null)
    *   <LI><B>PROCEDURE_SCHEM</B> String => procedure schema (may be null)
    *   <LI><B>PROCEDURE_NAME</B> String => procedure name
    *  <LI> reserved for future use
    *  <LI> reserved for future use
    *  <LI> reserved for future use
    *   <LI><B>REMARKS</B> String => explanatory comment on the procedure
    *   <LI><B>PROCEDURE_TYPE</B> short => kind of procedure:
    *      <UL>
    *      <LI> procedureResultUnknown - May return a result
    *      <LI> procedureNoResult - Does not return a result
    *      <LI> procedureReturnsResult - Returns a result
    *      </UL>
    *  </OL>
    *
    * @param catalog a catalog name; "" retrieves those without a
    * catalog; null means drop catalog name from the selection criteria
    * @param schemaPattern a schema name pattern; "" retrieves those
    * without a schema
    * @param procedureNamePattern a procedure name pattern
    * @return ResultSet - each row is a procedure description
    * @exception SQLException if a database-access error occurs.
    * @see #getSearchStringEscape
    */
   public java.sql.ResultSet getProcedures(String catalog,
                                           String schemaPattern,
                                           String procedureNamePattern) 
      throws SQLException
   {
     /*
      debugPrintln("Inside of getProcedures");
      debugPrintln("  catalog is |" + catalog + "|");
      debugPrintln("  schemaPattern is " + schemaPattern);
      debugPrintln("  procedurePattern is " + procedureNamePattern);
      */
      
      schemaPattern = schemaPattern.trim();

      String query;
      
      query = ("select PROCEDURE_CAT=?, "
               + " PROCEDURE_SCHEM=substring(u.name, 1, 32), "
               + " PROCEDURE_NAME=substring(o.name, 1, 32), "
               + " '', '', '', "
               + " REMARKS='', PROCEDURE_TYPE=" 
               + java.sql.DatabaseMetaData.procedureResultUnknown
               + " from ");
      if (catalog != null && (!catalog.equals("")))
      {
         // XXX need to make sure user doesn't pass in funky strings
         query = query + catalog + ".";
      }
      query = query + "dbo.sysobjects o,  ";
      
      if (catalog != null && (!catalog.equals("")))
      {
         // XXX need to make sure user doesn't pass in funky strings
         query = query + catalog + ".";
      }
      query = query + "dbo.sysusers u ";
 
      query = query + " where o.uid=u.uid and xtype='P' ";
      query = query + " and u.name like ? "; // schema name
      query = query + " and o.name like ? ";   // procedure name
 
      // debugPrintln("Query is |" + query + "|");
 
      java.sql.PreparedStatement ps = connection.prepareStatement(query);
 
      // debugPrintln("ps.setString(1, \"" + catalog + "\")");
      ps.setString(1, catalog);
      if (schemaPattern==null || schemaPattern.equals(""))
      {
         // debugPrintln("ps.setString(2, \"%\");");
         ps.setString(2, "%");
      }
      else
      {
         // debugPrintln("ps.setString(2, \"" + schemaPattern + "\")");
         ps.setString(2, schemaPattern);
      }
      if (procedureNamePattern==null || procedureNamePattern.equals(""))
      {
         // debugPrintln("ps.setString(3, \"%\");");
         ps.setString(3, "%");
      }
      else
      {
         // debugPrintln("ps.setString(3, \"" + procedureNamePattern + "\")");
         ps.setString(3, procedureNamePattern);
      }
      
      java.sql.ResultSet rs = ps.executeQuery();
 
      return rs;
   }
}
