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
 *  This class provides information about the database as a whole. <P>
 *
 *  Many of the methods here return lists of information in ResultSets. You can
 *  use the normal ResultSet methods such as getString and getInt to retrieve
 *  the data from these ResultSets. If a given form of metadata is not
 *  available, these methods should throw a SQLException. <P>
 *
 *  Some of these methods take arguments that are String patterns. These
 *  arguments all have names such as fooPattern. Within a pattern String, "%"
 *  means match any substring of 0 or more characters, and "_" means match any
 *  one character. Only metadata entries matching the search pattern are
 *  returned. If a search pattern argument is set to a null ref, it means that
 *  argument's criteria should be dropped from the search. <P>
 *
 *  A SQLException will be thrown if a driver does not support a meta data
 *  method. In the case of methods that return a ResultSet, either a ResultSet
 *  (which may be empty) is returned or a SQLException is thrown.
 *
 *@author     Craig Spannring
 *@author     The FreeTDS project
 *@created    17 March 2001
 *@version    $Id: DatabaseMetaData.java,v 1.10 2002-08-08 08:35:41 alin_sinpalean Exp $
 */
public class DatabaseMetaData implements java.sql.DatabaseMetaData {

    final boolean verbose = true;

    /**
     *  PROCEDURE_TYPE - May return a result.
     */
    final int procedureResultUnknown = 0;
    /**
     *  PROCEDURE_TYPE - Does not return a result.
     */
    final int procedureNoResult = 1;
    /**
     *  PROCEDURE_TYPE - Returns a result.
     */
    final int procedureReturnsResult = 2;

    /**
     *  COLUMN_TYPE - nobody knows.
     */
    final int procedureColumnUnknown = 0;

    /**
     *  COLUMN_TYPE - IN parameter.
     */
    final int procedureColumnIn = 1;

    /**
     *  COLUMN_TYPE - INOUT parameter.
     */
    final int procedureColumnInOut = 2;

    /**
     *  COLUMN_TYPE - OUT parameter.
     */
    final int procedureColumnOut = 4;
    /**
     *  COLUMN_TYPE - procedure return value.
     */
    final int procedureColumnReturn = 5;

    /**
     *  COLUMN_TYPE - result column in ResultSet.
     */
    final int procedureColumnResult = 3;

    /**
     *  TYPE NULLABLE - does not allow NULL values.
     */
    final int procedureNoNulls = 0;

    /**
     *  TYPE NULLABLE - allows NULL values.
     */
    final int procedureNullable = 1;

    /**
     *  TYPE NULLABLE - nullability unknown.
     */
    final int procedureNullableUnknown = 2;

    /**
     *  COLUMN NULLABLE - might not allow NULL values.
     */
    final int columnNoNulls = 0;

    /**
     *  COLUMN NULLABLE - definitely allows NULL values.
     */
    final int columnNullable = 1;

    /**
     *  COLUMN NULLABLE - nullability unknown.
     */
    final int columnNullableUnknown = 2;

    /**
     *  BEST ROW SCOPE - very temporary, while using row.
     */
    final int bestRowTemporary = 0;

    /**
     *  BEST ROW SCOPE - valid for remainder of current transaction.
     */
    final int bestRowTransaction = 1;

    /**
     *  BEST ROW SCOPE - valid for remainder of current session.
     */
    final int bestRowSession = 2;

    /**
     *  BEST ROW PSEUDO_COLUMN - may or may not be pseudo column.
     */
    final int bestRowUnknown = 0;

    /**
     *  BEST ROW PSEUDO_COLUMN - is NOT a pseudo column.
     */
    final int bestRowNotPseudo = 1;

    /**
     *  BEST ROW PSEUDO_COLUMN - is a pseudo column.
     */
    final int bestRowPseudo = 2;

    /**
     *  VERSION COLUMNS PSEUDO_COLUMN - may or may not be pseudo column.
     */
    final int versionColumnUnknown = 0;

    /**
     *  VERSION COLUMNS PSEUDO_COLUMN - is NOT a pseudo column.
     */
    final int versionColumnNotPseudo = 1;

    /**
     *  VERSION COLUMNS PSEUDO_COLUMN - is a pseudo column.
     */
    final int versionColumnPseudo = 2;

    /**
     *  IMPORT KEY UPDATE_RULE and DELETE_RULE - for update, change imported key
     *  to agree with primary key update; for delete, delete rows that import a
     *  deleted key.
     */
    final int importedKeyCascade = 0;

    /**
     *  IMPORT KEY UPDATE_RULE and DELETE_RULE - do not allow update or delete
     *  of primary key if it has been imported.
     */
    final int importedKeyRestrict = 1;

    /**
     *  IMPORT KEY UPDATE_RULE and DELETE_RULE - change imported key to NULL if
     *  its primary key has been updated or deleted.
     */
    final int importedKeySetNull = 2;

    /**
     *  IMPORT KEY UPDATE_RULE and DELETE_RULE - do not allow update or delete
     *  of primary key if it has been imported.
     */
    final int importedKeyNoAction = 3;

    /**
     *  IMPORT KEY UPDATE_RULE and DELETE_RULE - change imported key to default
     *  values if its primary key has been updated or deleted.
     */
    final int importedKeySetDefault = 4;

    /**
     *  IMPORT KEY DEFERRABILITY - see SQL92 for definition
     */
    final int importedKeyInitiallyDeferred = 5;

    /**
     *  IMPORT KEY DEFERRABILITY - see SQL92 for definition
     */
    final int importedKeyInitiallyImmediate = 6;

    /**
     *  IMPORT KEY DEFERRABILITY - see SQL92 for definition
     */
    final int importedKeyNotDeferrable = 7;

    /**
     *  TYPE NULLABLE - does not allow NULL values.
     */
    final int typeNoNulls = 0;

    /**
     *  TYPE NULLABLE - allows NULL values.
     */
    final int typeNullable = 1;

    /**
     *  TYPE NULLABLE - nullability unknown.
     */
    final int typeNullableUnknown = 2;

    /**
     *  TYPE INFO SEARCHABLE - No support.
     */
    final int typePredNone = 0;

    /**
     *  TYPE INFO SEARCHABLE - Only supported with WHERE .. LIKE.
     */
    final int typePredChar = 1;

    /**
     *  TYPE INFO SEARCHABLE - Supported except for WHERE .. LIKE.
     */
    final int typePredBasic = 2;

    /**
     *  TYPE INFO SEARCHABLE - Supported for all WHERE ...
     */
    final int typeSearchable = 3;

    /**
     *  INDEX INFO TYPE - this identifies table statistics that are returned in
     *  conjuction with a table's index descriptions
     */
    final short tableIndexStatistic = 0;

    /**
     *  INDEX INFO TYPE - this identifies a clustered index
     */
    final short tableIndexClustered = 1;

    /**
     *  INDEX INFO TYPE - this identifies a hashed index
     */
    final short tableIndexHashed = 2;

    /**
     *  INDEX INFO TYPE - this identifies some other form of index
     */
    final short tableIndexOther = 3;

    //
    // now for the internal data needed by this implemention.
    //
    Tds tds;
   int   sysnameLength = 30;


    java.sql.Connection connection;
    /**
     *  /** @todo Description of the Field
     */
    public final static String cvsVersion = "$Id: DatabaseMetaData.java,v 1.10 2002-08-08 08:35:41 alin_sinpalean Exp $";


    public DatabaseMetaData(
            Object connection_,
            Tds tds_ )
    {
        connection = ( java.sql.Connection ) connection_;
        tds = tds_;
    }


   public static DatabaseMetaData getInstance(
      Object        connection_,
      Tds           tds_)
   {
      // TODO: Figure out other specializations
      if( tds_.getTdsVer() >= Tds.TDS70 )
         // Return a Microsoft7MetaData only if we're using TDS 7.0 or later
         // (otherwise the limitations of the TDS version apply).
         return new com.internetcds.jdbc.tds.Microsoft7MetaData(connection_, tds_);

      return new com.internetcds.jdbc.tds.DatabaseMetaData(connection_, tds_);
   }
   //----------------------------------------------------------------------
   // First, a variety of minor information about the target database.

   /**
    * Can all the procedures returned by getProcedures be called by the
    * current user?
    *
    * @return true if so
    * @exception SQLException if a database-access error occurs.
    */
   public boolean allProceduresAreCallable() throws SQLException
   {
      // XXX Need to check for Sybase

      return true; // per "Programming ODBC for SQLServer" Appendix A
   }


   /**
    * Can all the tables returned by getTable be SELECTed by the
    * current user?
    *
    * @return true if so
    * @exception SQLException if a database-access error occurs.
    */
   public boolean allTablesAreSelectable() throws SQLException
   {
      // XXX Need to check for Sybase

      // XXX This is dependent on the way we are implementing getTables()
      // it may change in the future.
      return false;
   }


   /**
    * Does a data definition statement within a transaction force the
    * transaction to commit?
    *
    * @return true if so
    * @exception SQLException if a database-access error occurs.
    */
   public boolean dataDefinitionCausesTransactionCommit()
      throws SQLException
   {
      // XXX needs to be checked for Sybase
      return false;
   }


   /**
    * Is a data definition statement within a transaction ignored?
    *
    * @return true if so
    * @exception SQLException if a database-access error occurs.
    */
   public boolean dataDefinitionIgnoredInTransactions()
      throws SQLException
   {
      // XXX needs to be checked for Sybase
      return false;
   }



   /**
    * Did getMaxRowSize() include LONGVARCHAR and LONGVARBINARY
    * blobs?
    *
    * @return true if so
    * @exception SQLException if a database-access error occurs.
    */
   public boolean doesMaxRowSizeIncludeBlobs() throws SQLException
   {
      return false;
   }



   /**
    * Get a description of a table's optimal set of columns that
    * uniquely identifies a row. They are ordered by SCOPE.
    *
    * <P>Each column description has the following columns:
     *  <OL>
     *    <LI> <B>SCOPE</B> short =>actual scope of result
     *    <UL>
     *      <LI> bestRowTemporary - very temporary, while using row
     *      <LI> bestRowTransaction - valid for remainder of current transaction
     *
     *      <LI> bestRowSession - valid for remainder of current session
     *    </UL>
     *
     *    <LI> <B>COLUMN_NAME</B> String =>column name
     *    <LI> <B>DATA_TYPE</B> short =>SQL data type from java.sql.Types
     *    <LI> <B>TYPE_NAME</B> String =>Data source dependent type name
     *    <LI> <B>COLUMN_SIZE</B> int =>precision
     *    <LI> <B>BUFFER_LENGTH</B> int =>not used
     *    <LI> <B>DECIMAL_DIGITS</B> short =>scale
     *    <LI> <B>PSEUDO_COLUMN</B> short =>is this a pseudo column like an
     *    Oracle ROWID
     *    <UL>
     *      <LI> bestRowUnknown - may or may not be pseudo column
     *      <LI> bestRowNotPseudo - is NOT a pseudo column
     *      <LI> bestRowPseudo - is a pseudo column
     *    </UL>
     *
     *  </OL>
     *
     *
     *@param  catalog           a catalog name; "" retrieves those without a
     *      catalog; null means drop catalog name from the selection criteria
     *@param  schema            a schema name; "" retrieves those without a
     *      schema
     *@param  table             a table name
     *@param  scope             the scope of interest; use same values as SCOPE
     *@param  nullable          include columns that are nullable?
     *@return                   ResultSet - each row is a column description
     *@exception  SQLException  if a database-access error occurs.
     */
    public java.sql.ResultSet getBestRowIdentifier(
            String catalog,
            String schema,
            String table,
            int scope,
            boolean nullable )
             throws SQLException
    {
      /*
        debugPrintln( "Inside getBestRowIdentifier with catalog=|" + catalog
                 + "|, schema=|" + schema + "|, table=|" + table + "|, "
                 + " scope=" + scope + ", nullable=" + nullable );

       */
        NotImplemented();
        return null;
    }


    /**
     *  Get the catalog names available in this database. The results are
     *  ordered by catalog name. <P>
     *
     *  The catalog column is:
     *  <OL>
     *    <LI> <B>TABLE_CAT</B> String =>catalog name
     *  </OL>
     *
     *
     *@return                   ResultSet - each row has a single String column
     *      that is a catalog name
     *@exception  SQLException  if a database-access error occurs.
     */
    public java.sql.ResultSet getCatalogs()
             throws SQLException
    {
      return getCatalogs(null);
   }
   private java.sql.ResultSet getCatalogs(String catalog)
      throws SQLException
   {
        // XXX We should really clean up all these temporary tables.
        String tmpName = "#t#" + UniqueId.getUniqueId();
      String sql =
                " create table " + tmpName + "                                   " +
                " (                                                              " +
         "    q  sysname not null,                                        " +
         "    o  sysname null,                                            " +
         "    n  sysname null,                                            " +
         "    t  sysname null,                                            " +
                "    r  varchar(255) null                                        " +
                " )                                                              " +
         "                                                                " +
         " insert into " + tmpName + " EXEC sp_tables ' ', ' ', '%', null " +
         "                                                                " +
         " select q TABLE_CAT from " + tmpName + "                        " +
         "";

      if (catalog != null)
      {
         sql = sql + " where q = ? ";
      }
      else
      {
         catalog = " ";
         sql = sql + " where q != ? ";
      }

      java.sql.ResultSet rs = null;

      java.sql.PreparedStatement ps = connection.prepareStatement(sql);
      ps.setString(1, catalog);
      ps.execute();

      while ((rs = ps.getResultSet()) == null)
      {
        }
        return rs;
    }



    /**
     *  What's the separator between catalog and table name?
     *
     *@return                   the separator string
     *@exception  SQLException  if a database-access error occurs.
     */
    public String getCatalogSeparator() throws SQLException
    {
        return ".";
    }



    /**
     *  What's the database vendor's preferred term for "catalog"?
     *
     *@return                   the vendor term
     *@exception  SQLException  if a database-access error occurs.
     */
    public String getCatalogTerm() throws SQLException
    {

        return "database";
    }



    /**
     *  Get a description of the access rights for a table's columns. <P>
     *
     *  Only privileges matching the column name criteria are returned. They are
     *  ordered by COLUMN_NAME and PRIVILEGE. <P>
     *
     *  Each privilige description has the following columns:
     *  <OL>
     *    <LI> <B>TABLE_CAT</B> String =>table catalog (may be null)
     *    <LI> <B>TABLE_SCHEM</B> String =>table schema (may be null)
     *    <LI> <B>TABLE_NAME</B> String =>table name
     *    <LI> <B>COLUMN_NAME</B> String =>column name
     *    <LI> <B>GRANTOR</B> =>grantor of access (may be null)
     *    <LI> <B>GRANTEE</B> String =>grantee of access
     *    <LI> <B>PRIVILEGE</B> String =>name of access (SELECT, INSERT, UPDATE,
     *    REFRENCES, ...)
     *    <LI> <B>IS_GRANTABLE</B> String =>"YES" if grantee is permitted to
     *    grant to others; "NO" if not; null if unknown
     *  </OL>
     *
     *
     *@param  catalog            a catalog name; "" retrieves those without a
     *      catalog; null means drop catalog name from the selection criteria
     *@param  schema             a schema name; "" retrieves those without a
     *      schema
     *@param  table              a table name
     *@param  columnNamePattern  a column name pattern
     *@return                    ResultSet - each row is a column privilege
     *      description
     *@exception  SQLException   if a database-access error occurs.
     *@see                       #getSearchStringEscape
     */
    public java.sql.ResultSet getColumnPrivileges( String catalog, String schema,
            String table, String columnNamePattern )
             throws SQLException
    {
        NotImplemented();
        return null;
    }


    /**
     *  Get a description of table columns available in a catalog. <P>
     *
     *  Only column descriptions matching the catalog, schema, table and column
     *  name criteria are returned. They are ordered by TABLE_SCHEM, TABLE_NAME
     *  and ORDINAL_POSITION. <P>
     *
     *  Each column description has the following columns:
     *  <OL>
     *    <LI> <B>TABLE_CAT</B> String =>table catalog (may be null)
     *    <LI> <B>TABLE_SCHEM</B> String =>table schema (may be null)
     *    <LI> <B>TABLE_NAME</B> String =>table name
     *    <LI> <B>COLUMN_NAME</B> String =>column name
     *    <LI> <B>DATA_TYPE</B> short =>SQL type from java.sql.Types
     *    <LI> <B>TYPE_NAME</B> String =>Data source dependent type name
     *    <LI> <B>COLUMN_SIZE</B> int =>column size. For char or date types this
     *    is the maximum number of characters, for numeric or decimal types this
     *    is precision.
     *    <LI> <B>BUFFER_LENGTH</B> is not used.
     *    <LI> <B>DECIMAL_DIGITS</B> int =>the number of fractional digits
     *    <LI> <B>NUM_PREC_RADIX</B> int =>Radix (typically either 10 or 2)
     *    <LI> <B>NULLABLE</B> int =>is NULL allowed?
     *    <UL>
     *      <LI> columnNoNulls - might not allow NULL values
     *      <LI> columnNullable - definitely allows NULL values
     *      <LI> columnNullableUnknown - nullability unknown
     *    </UL>
     *
     *    <LI> <B>REMARKS</B> String =>comment describing column (may be null)
     *
     *    <LI> <B>COLUMN_DEF</B> String =>default value (may be null)
     *    <LI> <B>SQL_DATA_TYPE</B> int =>unused
     *    <LI> <B>SQL_DATETIME_SUB</B> int =>unused
     *    <LI> <B>CHAR_OCTET_LENGTH</B> int =>for char types the maximum number
     *    of bytes in the column
     *    <LI> <B>ORDINAL_POSITION</B> int =>index of column in table (starting
     *    at 1)
     *    <LI> <B>IS_NULLABLE</B> String =>"NO" means column definitely does not
     *    allow NULL values; "YES" means the column might allow NULL values. An
     *    empty string means nobody knows.
     *  </OL>
     *
     *
     *@param  catalog            a catalog name; "" retrieves those without a
     *      catalog; null means drop catalog name from the selection criteria
     *@param  schemaPattern      a schema name pattern; "" retrieves those
     *      without a schema
     *@param  tableNamePattern   a table name pattern
     *@param  columnNamePattern  a column name pattern
     *@return                    ResultSet - each row is a column description
     *@exception  SQLException   if a database-access error occurs.
     *@see                       #getSearchStringEscape
     */
    public java.sql.ResultSet getColumns( String catalog, String schemaPattern,
            String tableNamePattern, String columnNamePattern )
             throws SQLException
    {
      /*
        debugPrintln( "Inside of getColumn" );
        debugPrintln( "  catalog is |" + catalog + "|" );
        debugPrintln( "  schemaPattern is " + schemaPattern );
        debugPrintln( "  tableNamePattern is " + tableNamePattern );
        debugPrintln( "  columnNamePattern is " + columnNamePattern );
       */
        return getColumns_SQLServer65( catalog, schemaPattern,
                tableNamePattern, columnNamePattern );
    }

   private static String makeTypeTable(java.sql.Statement tmpTableStmt)
      throws SQLException
   {
      // Create a lookup table for mapping between native and jdbc types
      // This table can be reused for all FreeTDS drivers of the same version

      String lookup   = "##freetds#typemap#" + UniqueId.getUniqueId();

      String sql =
         "create table " + lookup + " (       " +
         "   native_type integer primary key, " +
         "   jdbc_type   integer not null)    ";
      tmpTableStmt.execute(sql);

      sql =
         "insert into " + lookup + " values ( 31, 1111) " +   // VOID
         "insert into " + lookup + " values ( 34, 1111) " +   // IMAGE
         "insert into " + lookup + " values ( 35,   -1) " +   // TEXT
         "insert into " + lookup + " values ( 37,   -3) " +   // VARBINARY
         "insert into " + lookup + " values ( 38,    4) " +   // INTN
         "insert into " + lookup + " values ( 39,   12) " +   // VARCHAR
         "insert into " + lookup + " values ( 45,   -2) " +   // BINARY
         "insert into " + lookup + " values ( 47,    1) " +   // CHAR
         "insert into " + lookup + " values ( 48,   -6) " +   // INT1
         "insert into " + lookup + " values ( 50,   -7) " +   // BIT
         "insert into " + lookup + " values ( 52,    5) " +   // INT2
         "insert into " + lookup + " values ( 56,    4) " +   // INT4
         "insert into " + lookup + " values ( 58,   93) " +   // DATETIME4
         "insert into " + lookup + " values ( 59,    7) " +   // REAL
         "insert into " + lookup + " values ( 60, 1111) " +   // MONEY
         "insert into " + lookup + " values ( 61,   93) " +   // DATETIME
         "insert into " + lookup + " values ( 62,    8) " +   // FLT8
         "insert into " + lookup + " values ( 63,    2) " +   // NUMERIC
         "insert into " + lookup + " values (106,    3) " +   // DECIMAL
         "insert into " + lookup + " values (108,    2) " +   // NUMERIC
         "insert into " + lookup + " values (109,    8) " +   // FLTN
         "insert into " + lookup + " values (110, 1111) " +   // MONEYN
         "insert into " + lookup + " values (111,   93) " +   // DATETIMN
         "insert into " + lookup + " values (112, 1111) " +   // MONEY4
         "insert into " + lookup + " values (122, 1111) " +   // SMALLMONEY
         "";
      tmpTableStmt.execute(sql);

      return lookup;
   }

   private static String makeTypeSQL(String colName)
      throws SQLException
   {
      // Create a lookup table for mapping between native and jdbc types
      // This table can be reused for all FreeTDS drivers of the same version

      String sql =
         "CASE " + colName + " " +
         "WHEN  31 THEN 1111 " +   // VOID
         "WHEN  34 THEN 1111 " +   // IMAGE
         "WHEN  35 THEN   -1 " +   // TEXT
         "WHEN  37 THEN   -3 " +   // VARBINARY
         "WHEN  38 THEN    4 " +   // INTN
         "WHEN  39 THEN   12 " +   // VARCHAR
         "WHEN  45 THEN   -2 " +   // BINARY
         "WHEN  47 THEN    1 " +   // CHAR
         "WHEN  48 THEN   -6 " +   // INT1
         "WHEN  50 THEN   -7 " +   // BIT
         "WHEN  52 THEN    5 " +   // INT2
         "WHEN  56 THEN    4 " +   // INT4
         "WHEN  58 THEN   93 " +   // DATETIME4
         "WHEN  59 THEN    7 " +   // REAL
         "WHEN  60 THEN 1111 " +   // MONEY
         "WHEN  61 THEN   93 " +   // DATETIME
         "WHEN  62 THEN    8 " +   // FLT8
         "WHEN  63 THEN    2 " +   // NUMERIC
         "WHEN 106 THEN    3 " +   // DECIMAL
         "WHEN 108 THEN    2 " +   // NUMERIC
         "WHEN 109 THEN    8 " +   // FLTN
         "WHEN 110 THEN 1111 " +   // MONEYN
         "WHEN 111 THEN   93 " +   // DATETIMN
         "WHEN 112 THEN 1111 " +   // MONEY4
         "WHEN 122 THEN 1111 " +   // SMALLMONEY
         "ELSE 0             " +   // Unknown
         "END";

      return sql;
   }

    private java.sql.ResultSet getColumns_SQLServer65(
        String catalog,
        String schemaPattern,
        String tableNamePattern,
        String columnNamePattern)
        throws SQLException
    {

        String sql = null;
        java.sql.Statement statement = connection.createStatement();

        String lookup = makeTypeSQL("c.type");

        java.sql.ResultSet rs;
        String cat = catalog;

        if(catalog == null)
        {
            rs = statement.executeQuery("SELECT DB_NAME()");
            rs.next();

            cat = rs.getString(1);

            rs.close();
        }

         // XXX Security risk.  It 'might' be possible to create
         // a catalog name that when inserted into this sql statement could
         // do other commands.
        sql =
            "select                                           " +
            "   TABLE_CAT='" + cat + "',                      " +
            "   TABLE_SCHEM=USER_NAME(o.uid),                 " +
            "   TABLE_NAME=o.name,                            " +
            "   COLUMN_NAME=c.name,                           " +
            "   DATA_TYPE=" + lookup + ",                     " +
            "   TYPE_NAME=t.name,                             " +
            "   COLUMN_SIZE=c.prec,                           " +
            "   BUFFER_LENGTH=0,                              " +
            "   DECIMAL_DIGITS=c.scale,                       " +
            "   NUM_PREC_RADIX=10,                            " +
            "   NULLABLE=convert(integer,                     " +
            "                    convert(bit, c.status&8)),   " +
            (tds.getDatabaseMajorVersion()>=8 && tds.getServerType()==Tds.SQLSERVER ?
                "   REMARKS=convert(varchar,                  " +
                "       (select value from                    " +
                "       " + cat + ".dbo.sysproperties as rm   " +
                "       where rm.id=o.id and smallid=c.colid  " +
                "       and rm.name='MS_Description')),       " :
                "   REMARKS=NULL,                             ") +
            "   COLUMN_DEF=                                   " +
            "       (select syscom.text from                  " +
            "       " + cat + ".dbo.sysobjects as sysobj,     " +
            "       " + cat + ".dbo.syscomments as syscom     " +
            "       where sysobj.parent_obj=o.id              " +
            "       and sysobj.type='D'                       " +
            "       and sysobj.info=c.colid                   " +
            "       and syscom.id=sysobj.id),                 " +
            "   SQL_DATATYPE=c.type,                          " +
            "   SQL_DATETIME_SUB=0,                           " +
            "   CHAR_OCTET_LENGTH=c.length,                   " +
            "   ORDINAL_POSITION=c.colid,                     " +
            "   IS_NULLABLE=                                  " +
            "      convert(char(3), rtrim(substring           " +
            "                             ('NO      YES',     " +
            "                             (c.status&8)+1,3))) " +
            "from                                             " +
            "   " + cat + ".dbo.sysobjects o,                 " +
            "   " + cat + ".dbo.syscolumns c,                 " +
            "   " + cat + ".dbo.systypes t                    " +
            "where o.type in ('S', 'V', 'U') and o.id=c.id    " +
            "   and t.usertype=c.usertype                     " +
            "   and USER_NAME(o.uid) LIKE                     " +
            "           '" + schemaPattern + "'               " +
            "   and o.name LIKE '" + tableNamePattern + "'    " ;

        if(columnNamePattern != null) sql += "  and c.name like '" + columnNamePattern + "'";

        sql += " order by TABLE_CAT, TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION";

        rs = statement.executeQuery(sql);

        return rs;
    }

    /**
     *  Get a description of the foreign key columns in the foreign key table
     *  that reference the primary key columns of the primary key table
     *  (describe how one table imports another's key.) This should normally
     *  return a single foreign key/primary key pair (most tables only import a
     *  foreign key from a table once.) They are ordered by FKTABLE_CAT,
     *  FKTABLE_SCHEM, FKTABLE_NAME, and KEY_SEQ. <P>
     *
     *  Each foreign key column description has the following columns:
     *  <OL>
     *    <LI> <B>PKTABLE_CAT</B> String =>primary key table catalog (may be
     *    null)
     *    <LI> <B>PKTABLE_SCHEM</B> String =>primary key table schema (may be
     *    null)
     *    <LI> <B>PKTABLE_NAME</B> String =>primary key table name
     *    <LI> <B>PKCOLUMN_NAME</B> String =>primary key column name
     *    <LI> <B>FKTABLE_CAT</B> String =>foreign key table catalog (may be
     *    null) being exported (may be null)
     *    <LI> <B>FKTABLE_SCHEM</B> String =>foreign key table schema (may be
     *    null) being exported (may be null)
     *    <LI> <B>FKTABLE_NAME</B> String =>foreign key table name being
     *    exported
     *    <LI> <B>FKCOLUMN_NAME</B> String =>foreign key column name being
     *    exported
     *    <LI> <B>KEY_SEQ</B> short =>sequence number within foreign key
     *    <LI> <B>UPDATE_RULE</B> short =>What happens to foreign key when
     *    primary is updated:
     *    <UL>
     *      <LI> importedNoAction - do not allow update of primary key if it has
     *      been imported
     *      <LI> importedKeyCascade - change imported key to agree with primary
     *      key update
     *      <LI> importedKeySetNull - change imported key to NULL if its primary
     *      key has been updated
     *      <LI> importedKeySetDefault - change imported key to default values
     *      if its primary key has been updated
     *      <LI> importedKeyRestrict - same as importedKeyNoAction (for ODBC 2.x
     *      compatibility)
     *    </UL>
     *
     *    <LI> <B>DELETE_RULE</B> short =>What happens to the foreign key when
     *    primary is deleted.
     *    <UL>
     *      <LI> importedKeyNoAction - do not allow delete of primary key if it
     *      has been imported
     *      <LI> importedKeyCascade - delete rows that import a deleted key
     *      <LI> importedKeySetNull - change imported key to NULL if its primary
     *      key has been deleted
     *      <LI> importedKeyRestrict - same as importedKeyNoAction (for ODBC 2.x
     *      compatibility)
     *      <LI> importedKeySetDefault - change imported key to default if its
     *      primary key has been deleted
     *    </UL>
     *
     *    <LI> <B>FK_NAME</B> String =>foreign key name (may be null)
     *    <LI> <B>PK_NAME</B> String =>primary key name (may be null)
     *    <LI> <B>DEFERRABILITY</B> short =>can the evaluation of foreign key
     *    constraints be deferred until commit
     *    <UL>
     *      <LI> importedKeyInitiallyDeferred - see SQL92 for definition
     *      <LI> importedKeyInitiallyImmediate - see SQL92 for definition
     *      <LI> importedKeyNotDeferrable - see SQL92 for definition
     *    </UL>
     *
     *  </OL>
     *
     *
     *@param  primaryCatalog    a catalog name; "" retrieves those without a
     *      catalog; null means drop catalog name from the selection criteria
     *@param  primarySchema     a schema name pattern; "" retrieves those
     *      without a schema
     *@param  primaryTable      the table name that exports the key
     *@param  foreignCatalog    a catalog name; "" retrieves those without a
     *      catalog; null means drop catalog name from the selection criteria
     *@param  foreignSchema     a schema name pattern; "" retrieves those
     *      without a schema
     *@param  foreignTable      the table name that imports the key
     *@return                   ResultSet - each row is a foreign key column
     *      description
     *@exception  SQLException  if a database-access error occurs.
     *@see                      #getImportedKeys
     */
    public java.sql.ResultSet getCrossReference(
            String primaryCatalog, String primarySchema, String primaryTable,
            String foreignCatalog, String foreignSchema, String foreignTable
             ) throws SQLException
    {
        NotImplemented();
        return null;
    }


    /**
     *  What's the name of this database product?
     *
     *@return                   database product name
     *@exception  SQLException  if a database-access error occurs.
     */
    public String getDatabaseProductName() throws SQLException
    {
        return tds.getDatabaseProductName();
    }


    /**
     *  What's the version of this database product?
     *
     *@return                   database version
     *@exception  SQLException  if a database-access error occurs.
     */
    public String getDatabaseProductVersion() throws SQLException
    {
        return tds.getDatabaseProductVersion();
    }


    //----------------------------------------------------------------------

    /**
     *  What's the database's default transaction isolation level? The values
     *  are defined in java.sql.Connection.
     *
     *@return                   the default isolation level
     *@exception  SQLException  if a database-access error occurs.
     *@see                      Connection
     */
    public int getDefaultTransactionIsolation() throws SQLException
    {
        // XXX need to check this for Sybase
        return Connection.TRANSACTION_READ_COMMITTED;
    }


    /**
     *  What's this JDBC driver's major version number?
     *
     *@return    JDBC driver major version
     */
    public int getDriverMajorVersion()
    {
        return DriverVersion.getDriverMajorVersion();
    }


    /**
     *  What's this JDBC driver's minor version number?
     *
     *@return    JDBC driver minor version number
     */
    public int getDriverMinorVersion()
    {
        return DriverVersion.getDriverMinorVersion();
    }


    /**
     *  What's the name of this JDBC driver?
     *
     *@return                   JDBC driver name
     *@exception  SQLException  if a database-access error occurs.
     */
    public String getDriverName() throws SQLException
    {
        return "InternetCDS Type 4 JDBC driver for MS SQLServer";
    }


    /**
     *  What's the version of this JDBC driver?
     *
     *@return                   JDBC driver version
     *@exception  SQLException  if a database-access error occurs.
     */
    public String getDriverVersion() throws SQLException
    {
        return getDriverMajorVersion() + "." + getDriverMinorVersion();
    }


    /**
     *  Get a description of the foreign key columns that reference a table's
     *  primary key columns (the foreign keys exported by a table). They are
     *  ordered by FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, and KEY_SEQ. <P>
     *
     *  Each foreign key column description has the following columns:
     *  <OL>
     *    <LI> <B>PKTABLE_CAT</B> String =>primary key table catalog (may be
     *    null)
     *    <LI> <B>PKTABLE_SCHEM</B> String =>primary key table schema (may be
     *    null)
     *    <LI> <B>PKTABLE_NAME</B> String =>primary key table name
     *    <LI> <B>PKCOLUMN_NAME</B> String =>primary key column name
     *    <LI> <B>FKTABLE_CAT</B> String =>foreign key table catalog (may be
     *    null) being exported (may be null)
     *    <LI> <B>FKTABLE_SCHEM</B> String =>foreign key table schema (may be
     *    null) being exported (may be null)
     *    <LI> <B>FKTABLE_NAME</B> String =>foreign key table name being
     *    exported
     *    <LI> <B>FKCOLUMN_NAME</B> String =>foreign key column name being
     *    exported
     *    <LI> <B>KEY_SEQ</B> short =>sequence number within foreign key
     *    <LI> <B>UPDATE_RULE</B> short =>What happens to foreign key when
     *    primary is updated:
     *    <UL>
     *      <LI> importedNoAction - do not allow update of primary key if it has
     *      been imported
     *      <LI> importedKeyCascade - change imported key to agree with primary
     *      key update
     *      <LI> importedKeySetNull - change imported key to NULL if its primary
     *      key has been updated
     *      <LI> importedKeySetDefault - change imported key to default values
     *      if its primary key has been updated
     *      <LI> importedKeyRestrict - same as importedKeyNoAction (for ODBC 2.x
     *      compatibility)
     *    </UL>
     *
     *    <LI> <B>DELETE_RULE</B> short =>What happens to the foreign key when
     *    primary is deleted.
     *    <UL>
     *      <LI> importedKeyNoAction - do not allow delete of primary key if it
     *      has been imported
     *      <LI> importedKeyCascade - delete rows that import a deleted key
     *      <LI> importedKeySetNull - change imported key to NULL if its primary
     *      key has been deleted
     *      <LI> importedKeyRestrict - same as importedKeyNoAction (for ODBC 2.x
     *      compatibility)
     *      <LI> importedKeySetDefault - change imported key to default if its
     *      primary key has been deleted
     *    </UL>
     *
     *    <LI> <B>FK_NAME</B> String =>foreign key name (may be null)
     *    <LI> <B>PK_NAME</B> String =>primary key name (may be null)
     *    <LI> <B>DEFERRABILITY</B> short =>can the evaluation of foreign key
     *    constraints be deferred until commit
     *    <UL>
     *      <LI> importedKeyInitiallyDeferred - see SQL92 for definition
     *      <LI> importedKeyInitiallyImmediate - see SQL92 for definition
     *      <LI> importedKeyNotDeferrable - see SQL92 for definition
     *    </UL>
     *
     *  </OL>
     *
     *
     *@param  catalog           a catalog name; "" retrieves those without a
     *      catalog; null means drop catalog name from the selection criteria
     *@param  schema            a schema name pattern; "" retrieves those
     *      without a schema
     *@param  table             a table name
     *@return                   ResultSet - each row is a foreign key column
     *      description
     *@exception  SQLException  if a database-access error occurs.
     *@see                      #getImportedKeys
     */
    public java.sql.ResultSet getExportedKeys(
        String catalog,
        String schema,
        String table)
        throws SQLException
    {
        String sql = null;
        java.sql.Statement statement = connection.createStatement();

        java.sql.ResultSet rs;

        if(catalog == null)
        {
            rs = statement.executeQuery("SELECT DB_NAME()");
            rs.next();

            catalog = rs.getString(1);

            rs.close();
        }

        // XXX Security risk.  It 'might' be possible to create
        // a catalog name that when inserted into this sql statement could
        // do other commands.
        sql =
            "select                                                         " +
            "  '" + catalog + "' as PKTABLE_CAT,                            " +
            "  PKTABLE_SCHEM=                                               " +
            "    (select USER_NAME(uid) from                                " +
            "    " + catalog + ".dbo.sysobjects                             " +
            "    where f.rkeyid=id),                                        " +
            "  OBJECT_NAME(f.rkeyid) as PKTABLE_NAME,                       " +
            "  PKCOLUMN_NAME=                                               " +
            "    (select name from                                          " +
            "    " + catalog + ".dbo.syscolumns                             " +
            "    where f.rkeyid=id and f.rkey=colid),                       " +
            "  '" + catalog + "'  as FKTABLE_CAT,                           " +
            "  USER_NAME(o.uid) as FKTABLE_SCHEM,                           " +
            "  OBJECT_NAME(o.parent_obj) as FKTABLE_NAME,                   " +
            "  FKCOLUMN_NAME=                                               " +
            "    (select name from                                          " +
            "    " + catalog + ".dbo.syscolumns                             " +
            "    where f.fkeyid=id and f.fkey=colid),                       " +
            "  f.keyno as KEY_SEQ,                                          " +
            "  UPDATE_RULE=                                                 " +
            "    CASE OBJECTPROPERTY(f.constid, 'CnstIsUpdateCascade')      " +
            "    WHEN 1 THEN 0                                              " +
            "    ELSE 1                                                     " +
            "    END,                                                       " +
            " DELETE_RULE=                                                  " +
            "    CASE OBJECTPROPERTY(f.constid, 'CnstIsDeleteCascade')      " +
            "    WHEN 1 THEN 0                                              " +
            "    ELSE 1                                                     " +
            "    END,                                                       " +
            "  o.name as FK_NAME,                                           " +
            "  PK_NAME=                                                     " +
            "    (select name from                                          " +
            "    " + catalog + ".dbo.sysobjects                             " +
            "    where parent_obj=f.rkeyid and xtype='PK'),                 " +
            "  7 as DEFERRABILITY                                           " +
            "from                                                           " +
            "  " + catalog + ".dbo.sysobjects as o,                         " +
            "  " + catalog + ".dbo.sysforeignkeys as f                      " +
            "where                                                          " +
            "  OBJECT_NAME(f.rkeyid)='" + table + "' and                    " +
            "  o.xtype = 'F' and                                            " +
            "  o.id=f.constid                                               " ;

        if(schema != null) sql += " and USER_NAME(o.uid) LIKE '" + schema + "'" ;

        sql += " order by PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, KEY_SEQ";

        rs = statement.executeQuery(sql);

        return rs;
    }


    /**
     *  Get all the "extra" characters that can be used in unquoted identifier
     *  names (those beyond a-z, A-Z, 0-9 and _).
     *
     *@return                   the string containing the extra characters
     *@exception  SQLException  if a database-access error occurs.
     */
    public String getExtraNameCharacters() throws SQLException
    {
        return "#$";
    }


    /**
     *  What's the string used to quote SQL identifiers? This returns a space "
     *  " if identifier quoting isn't supported. A JDBC-Compliant driver always
     *  uses a double quote character.
     *
     *@return                   the quoting string
     *@exception  SQLException  if a database-access error occurs.
     */
    public String getIdentifierQuoteString() throws SQLException
    {
        return "\"";
    }


    /**
     *  Get a description of the primary key columns that are referenced by a
     *  table's foreign key columns (the primary keys imported by a table). They
     *  are ordered by PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, and KEY_SEQ.
     *  <P>
     *
     *  Each primary key column description has the following columns:
     *  <OL>
     *    <LI> <B>PKTABLE_CAT</B> String =>primary key table catalog being
     *    imported (may be null)
     *    <LI> <B>PKTABLE_SCHEM</B> String =>primary key table schema being
     *    imported (may be null)
     *    <LI> <B>PKTABLE_NAME</B> String =>primary key table name being
     *    imported
     *    <LI> <B>PKCOLUMN_NAME</B> String =>primary key column name being
     *    imported
     *    <LI> <B>FKTABLE_CAT</B> String =>foreign key table catalog (may be
     *    null)
     *    <LI> <B>FKTABLE_SCHEM</B> String =>foreign key table schema (may be
     *    null)
     *    <LI> <B>FKTABLE_NAME</B> String =>foreign key table name
     *    <LI> <B>FKCOLUMN_NAME</B> String =>foreign key column name
     *    <LI> <B>KEY_SEQ</B> short =>sequence number within foreign key
     *    <LI> <B>UPDATE_RULE</B> short =>What happens to foreign key when
     *    primary is updated:
     *    <UL>
     *      <LI> importedNoAction - do not allow update of primary key if it has
     *      been imported
     *      <LI> importedKeyCascade - change imported key to agree with primary
     *      key update
     *      <LI> importedKeySetNull - change imported key to NULL if its primary
     *      key has been updated
     *      <LI> importedKeySetDefault - change imported key to default values
     *      if its primary key has been updated
     *      <LI> importedKeyRestrict - same as importedKeyNoAction (for ODBC 2.x
     *      compatibility)
     *    </UL>
     *
     *    <LI> <B>DELETE_RULE</B> short =>What happens to the foreign key when
     *    primary is deleted.
     *    <UL>
     *      <LI> importedKeyNoAction - do not allow delete of primary key if it
     *      has been imported
     *      <LI> importedKeyCascade - delete rows that import a deleted key
     *      <LI> importedKeySetNull - change imported key to NULL if its primary
     *      key has been deleted
     *      <LI> importedKeyRestrict - same as importedKeyNoAction (for ODBC 2.x
     *      compatibility)
     *      <LI> importedKeySetDefault - change imported key to default if its
     *      primary key has been deleted
     *    </UL>
     *
     *    <LI> <B>FK_NAME</B> String =>foreign key name (may be null)
     *    <LI> <B>PK_NAME</B> String =>primary key name (may be null)
     *    <LI> <B>DEFERRABILITY</B> short =>can the evaluation of foreign key
     *    constraints be deferred until commit
     *    <UL>
     *      <LI> importedKeyInitiallyDeferred - see SQL92 for definition
     *      <LI> importedKeyInitiallyImmediate - see SQL92 for definition
     *      <LI> importedKeyNotDeferrable - see SQL92 for definition
     *    </UL>
     *
     *  </OL>
     *
     *
     *@param  catalog           a catalog name; "" retrieves those without a
     *      catalog; null means drop catalog name from the selection criteria
     *@param  schema            a schema name pattern; "" retrieves those
     *      without a schema
     *@param  table             a table name
     *@return                   ResultSet - each row is a primary key column
     *      description
     *@exception  SQLException  if a database-access error occurs.
     *@see                      #getExportedKeys
     */
    public java.sql.ResultSet getImportedKeys(
        String catalog,
        String schema,
        String table)
        throws SQLException
    {
        String sql = null;
        java.sql.Statement statement = connection.createStatement();

        java.sql.ResultSet rs;

        if(catalog == null)
        {
            rs = statement.executeQuery("SELECT DB_NAME()");
            rs.next();

            catalog = rs.getString(1);

            rs.close();
        }

        // XXX Security risk.  It 'might' be possible to create
        // a catalog name that when inserted into this sql statement could
        // do other commands.
        sql =
            "select                                                         " +
            "  '" + catalog + "' as PKTABLE_CAT,                            " +
            "  PKTABLE_SCHEM=                                               " +
            "    (select USER_NAME(uid) from                                " +
            "    " + catalog + ".dbo.sysobjects                             " +
            "    where f.rkeyid=id),                                        " +
            "  OBJECT_NAME(f.rkeyid) as PKTABLE_NAME,                       " +
            "  PKCOLUMN_NAME=                                               " +
            "    (select name from                                          " +
            "    " + catalog + ".dbo.syscolumns                             " +
            "    where f.rkeyid=id and f.rkey=colid),                       " +
            "  '" + catalog + "'  as FKTABLE_CAT,                           " +
            "  USER_NAME(o.uid) as FKTABLE_SCHEM,                           " +
            "  OBJECT_NAME(o.parent_obj) as FKTABLE_NAME,                   " +
            "  FKCOLUMN_NAME=                                               " +
            "    (select name from                                          " +
            "    " + catalog + ".dbo.syscolumns                             " +
            "    where f.fkeyid=id and f.fkey=colid),                       " +
            "  f.keyno as KEY_SEQ,                                          " +
            "  UPDATE_RULE=                                                 " +
            "    CASE OBJECTPROPERTY(f.constid, 'CnstIsUpdateCascade')      " +
            "    WHEN 1 THEN 0                                              " +
            "    ELSE 1                                                     " +
            "    END,                                                       " +
            " DELETE_RULE=                                                  " +
            "    CASE OBJECTPROPERTY(f.constid, 'CnstIsDeleteCascade')      " +
            "    WHEN 1 THEN 0                                              " +
            "    ELSE 1                                                     " +
            "    END,                                                       " +
            "  o.name as FK_NAME,                                           " +
            "  PK_NAME=                                                     " +
            "    (select name from                                          " +
            "    " + catalog + ".dbo.sysobjects                             " +
            "    where parent_obj=f.rkeyid and xtype='PK'),                 " +
            "  7 as DEFERRABILITY                                           " +
            "from                                                           " +
            "  " + catalog + ".dbo.sysobjects as o,                         " +
            "  " + catalog + ".dbo.sysforeignkeys as f                      " +
            "where                                                          " +
            "  OBJECT_NAME(f.fkeyid)='" + table + "' and                    " +
            "  o.xtype = 'F' and                                            " +
            "  o.id=f.constid                                               " ;

        if(schema != null) sql += " and USER_NAME(o.uid) LIKE '" + schema + "'" ;

        sql += " order by PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, KEY_SEQ";

        rs = statement.executeQuery(sql);

        return rs;
    }


    /**
     *  Get a description of a table's indices and statistics. They are ordered
     *  by NON_UNIQUE, TYPE, INDEX_NAME, and ORDINAL_POSITION. <P>
     *
     *  Each index column description has the following columns:
     *  <OL>
     *    <LI> <B>TABLE_CAT</B> String =>table catalog (may be null)
     *    <LI> <B>TABLE_SCHEM</B> String =>table schema (may be null)
     *    <LI> <B>TABLE_NAME</B> String =>table name
     *    <LI> <B>NON_UNIQUE</B> boolean =>Can index values be non-unique? false
     *    when TYPE is tableIndexStatistic
     *    <LI> <B>INDEX_QUALIFIER</B> String =>index catalog (may be null); null
     *    when TYPE is tableIndexStatistic
     *    <LI> <B>INDEX_NAME</B> String =>index name; null when TYPE is
     *    tableIndexStatistic
     *    <LI> <B>TYPE</B> short =>index type:
     *    <UL>
     *      <LI> tableIndexStatistic - this identifies table statistics that are
     *      returned in conjuction with a table's index descriptions
     *      <LI> tableIndexClustered - this is a clustered index
     *      <LI> tableIndexHashed - this is a hashed index
     *      <LI> tableIndexOther - this is some other style of index
     *    </UL>
     *
     *    <LI> <B>ORDINAL_POSITION</B> short =>column sequence number within
     *    index; zero when TYPE is tableIndexStatistic
     *    <LI> <B>COLUMN_NAME</B> String =>column name; null when TYPE is
     *    tableIndexStatistic
     *    <LI> <B>ASC_OR_DESC</B> String =>column sort sequence, "A" =>
     *    ascending, "D" =>descending, may be null if sort sequence is not
     *    supported; null when TYPE is tableIndexStatistic
     *    <LI> <B>CARDINALITY</B> int =>When TYPE is tableIndexStatistic, then
     *    this is the number of rows in the table; otherwise, it is the number
     *    of unique values in the index.
     *    <LI> <B>PAGES</B> int =>When TYPE is tableIndexStatisic then this is
     *    the number of pages used for the table, otherwise it is the number of
     *    pages used for the current index.
     *    <LI> <B>FILTER_CONDITION</B> String =>Filter condition, if any. (may
     *    be null)
     *  </OL>
     *
     *
     *@param  catalog           a catalog name; "" retrieves those without a
     *      catalog; null means drop catalog name from the selection criteria
     *@param  schema            a schema name pattern; "" retrieves those
     *      without a schema
     *@param  table             a table name
     *@param  unique            when true, return only indices for unique
     *      values; when false, return indices regardless of whether unique or
     *      not
     *@param  approximate       when true, result is allowed to reflect
     *      approximate or out of data values; when false, results are requested
     *      to be accurate
     *@return                   ResultSet - each row is an index column
     *      description
     *@exception  SQLException  if a database-access error occurs.
     */
    public java.sql.ResultSet getIndexInfo(
        String catalog,
        String schema,
        String table,
        boolean unique,
        boolean approximate)
        throws SQLException
    {
        String sql = null;
        java.sql.Statement statement = connection.createStatement();

        java.sql.ResultSet rs;

        if(catalog == null)
        {
            rs = statement.executeQuery("SELECT DB_NAME()");
            rs.next();

            catalog = rs.getString(1);

            rs.close();
        }

         // XXX Security risk.  It 'might' be possible to create
         // a catalog name that when inserted into this sql statement could
         // do other commands.
        sql =
            "select                                                                                 " +
            "  '" + catalog + "' as TABLE_CAT,                                                      " +
            "  USER_NAME(o.uid) as TABLE_SCHEM,                                                     " +
            "  o.name as TABLE_NAME,                                                                " +
            "  NON_UNIQUE=                                                                          " +
            "          CASE INDEXPROPERTY(o.id, i.name, 'IsUnique')                                 " +
            "          WHEN 1 THEN convert(bit, 0)                                                  " +
            "          WHEN 0 THEN convert(bit, 1)                                                  " +
            "          END,                                                                         " +
            "  null as INDEX_QUALIFIER,                                                             " +
            "  i.name as INDEX_NAME,                                                                " +
            "  TYPE=                                                                                " +
            "          CASE                                                                         " +
            "          WHEN INDEXPROPERTY(o.id, i.name, 'IsStatistics')=1 THEN 0                    " +
            "          WHEN INDEXPROPERTY(o.id, i.name, 'IsClustered')=1 THEN 1                     " +
            "          ELSE 3                                                                       " +
            "          END,                                                                         " +
            "  ORDINAL_POSITION=                                                                    " +
            "          CASE                                                                         " +
            "          WHEN INDEXPROPERTY(o.id, i.name, 'IsStatistics')=1 THEN 0                    " +
            "          ELSE k.keyno                                                                 " +
            "          END,                                                                         " +
            "  c.name as COLUMN_NAME,                                                               " +
            "  ASC_OR_DESC=                                                                         " +
            (tds.getDatabaseMajorVersion()>=8 && tds.getServerType()==Tds.SQLSERVER ?
               ("          CASE                                                                     " +
                "          WHEN INDEXKEY_PROPERTY(o.id, i.indid, k.keyno, 'IsDescending')=1 THEN 'DESC'" +
                "          ELSE 'ASC'                                                               " +
                "          END,                                                                     ")
               : "NULL,") +
            "  i.rowcnt as CARDINALITY,                                                             " +
            "  i.dpages as PAGES,                                                                   " +
            "  null as FILTER_CONDITION                                                             " +
            "from                                                                                   " +
            "  " + catalog + ".dbo.sysobjects as o,                                                 " +
            "  " + catalog + ".dbo.sysindexes as i,                                                 " +
            "  " + catalog + ".dbo.sysindexkeys as k,                                               " +
            "  " + catalog + ".dbo.syscolumns as c                                                  " +
            "where                                                                                  " +
            "  o.name='" + table + "' and                                                           " +
            "  o.id=i.id and                                                                        " +
            "  k.id=i.id and                                                                        " +
            "  k.indid=i.indid and                                                                  " +
            "  k.id=c.id and                                                                        " +
            "  k.colid=c.colid                                                                      " ;

        if(schema != null) sql += " and USER_NAME(o.uid) LIKE '" + schema + "'" ;
        if(unique) sql += "  and NON_UNIQUE=0";

        sql += " order by NON_UNIQUE, TYPE, INDEX_NAME, ORDINAL_POSITION";

        rs = statement.executeQuery(sql);

        return rs;
    }


    //----------------------------------------------------------------------
    // The following group of methods exposes various limitations
    // based on the target database with the current driver.
    // Unless otherwise specified, a result of zero means there is no
    // limit, or the limit is not known.

    /**
     *  How many hex characters can you have in an inline binary literal?
     *
     *@return                   max literal length
     *@exception  SQLException  if a database-access error occurs.
     */
    public int getMaxBinaryLiteralLength() throws SQLException
    {
        // XXX Need to check for Sybase and SQLServer 7.0

        return 131072;
        // per "Programming ODBC for SQLServer" Appendix A
    }


    /**
     *  What's the maximum length of a catalog name?
     *
     *@return                   max name length in bytes
     *@exception  SQLException  if a database-access error occurs.
     */
    public int getMaxCatalogNameLength() throws SQLException
    {
//      NotImplemented();
        return 0;
    }


    /**
     *  What's the max length for a character literal?
     *
     *@return                   max literal length
     *@exception  SQLException  if a database-access error occurs.
     */
    public int getMaxCharLiteralLength() throws SQLException
    {
        // XXX Need to check for Sybase

        return 131072;
        // per "Programming ODBC for SQLServer" Appendix A
    }


    /**
     *  What's the limit on column name length?
     *
     *@return                   max literal length
     *@exception  SQLException  if a database-access error occurs.
     */
    public int getMaxColumnNameLength() throws SQLException
    {
        // XXX Need to check for Sybase

      return sysnameLength; // per "Programming ODBC for SQLServer" Appendix A
        // per "Programming ODBC for SQLServer" Appendix A
    }


    /**
     *  What's the maximum number of columns in a "GROUP BY" clause?
     *
     *@return                   max number of columns
     *@exception  SQLException  if a database-access error occurs.
     */
    public int getMaxColumnsInGroupBy() throws SQLException
    {
        // XXX Need to check for Sybase

        return 16;
        // per "Programming ODBC for SQLServer" Appendix A
    }


    /**
     *  What's the maximum number of columns allowed in an index?
     *
     *@return                   max columns
     *@exception  SQLException  if a database-access error occurs.
     */
    public int getMaxColumnsInIndex() throws SQLException
    {
        // XXX need to find out if this is still true for SYBASE

        // per SQL Server Books Online "Administrator's Companion",
        // Part 1, Chapter 1.
        return 16;
    }


    /**
     *  What's the maximum number of columns in an "ORDER BY" clause?
     *
     *@return                   max columns
     *@exception  SQLException  if a database-access error occurs.
     */
    public int getMaxColumnsInOrderBy() throws SQLException
    {
        // XXX Need to check for Sybase

        return 16;
        // per "Programming ODBC for SQLServer" Appendix A
    }


    /**
     *  What's the maximum number of columns in a "SELECT" list?
     *
     *@return                   max columns
     *@exception  SQLException  if a database-access error occurs.
     */
    public int getMaxColumnsInSelect() throws SQLException
    {
        // XXX Need to check for Sybase

        return 4000;
        // per "Programming ODBC for SQLServer" Appendix A
    }


    /**
     *  What's the maximum number of columns in a table?
     *
     *@return                   max columns
     *@exception  SQLException  if a database-access error occurs.
     */
    public int getMaxColumnsInTable() throws SQLException
    {
        // XXX How do we find this out for Sybase?
        return 250;
        // per "Programming ODBC for SQLServer" Appendix A
    }


    /**
     *  How many active connections can we have at a time to this database?
     *
     *@return                   max connections
     *@exception  SQLException  if a database-access error occurs.
     */
    public int getMaxConnections() throws SQLException
    {
        // XXX need to find out if this is still true for SYBASE

        // per SQL Server Books Online "Administrator's Companion",
        // Part 1, Chapter 1.
        return 32767;
    }


    /**
     *  What's the maximum cursor name length?
     *
     *@return                   max cursor name length in bytes
     *@exception  SQLException  if a database-access error occurs.
     */
    public int getMaxCursorNameLength() throws SQLException
    {
        // XXX Need to check for Sybase

      return sysnameLength; // per "Programming ODBC for SQLServer" Appendix A
        // per "Programming ODBC for SQLServer" Appendix A
    }


    /**
     *  What's the maximum length of an index (in bytes)?
     *
     *@return                   max index length in bytes
     *@exception  SQLException  if a database-access error occurs.
     */
    public int getMaxIndexLength() throws SQLException
    {
        // XXX Need to check for Sybase

        return 900;
        // per "Programming ODBC for SQLServer" Appendix A
    }


    /**
     *  What's the maximum length of a procedure name?
     *
     *@return                   max name length in bytes
     *@exception  SQLException  if a database-access error occurs.
     */
    public int getMaxProcedureNameLength() throws SQLException
    {
        // XXX Need to check for Sybase

      return sysnameLength; // per "Programming ODBC for SQLServer" Appendix A
        // per "Programming ODBC for SQLServer" Appendix A
    }


    /**
     *  What's the maximum length of a single row?
     *
     *@return                   max row size in bytes
     *@exception  SQLException  if a database-access error occurs.
     */
    public int getMaxRowSize() throws SQLException
    {
        // XXX need to find out if this is still true for SYBASE

        // per SQL Server Books Online "Administrator's Companion",
        // Part 1, Chapter 1.
        return 1962;
    }


    /**
     *  What's the maximum length allowed for a schema name?
     *
     *@return                   max name length in bytes
     *@exception  SQLException  if a database-access error occurs.
     */
    public int getMaxSchemaNameLength() throws SQLException
    {
//      NotImplemented();
        return 0;
    }


    /**
     *  What's the maximum length of a SQL statement?
     *
     *@return                   max length in bytes
     *@exception  SQLException  if a database-access error occurs.
     */
    public int getMaxStatementLength() throws SQLException
    {
        // XXX Need to check for Sybase

        return 131072;
        // per "Programming ODBC for SQLServer" Appendix A
    }


    /**
     *  How many active statements can we have open at one time to this
     *  database?
     *
     *@return                   the maximum
     *@exception  SQLException  if a database-access error occurs.
     */
    public int getMaxStatements() throws SQLException
    {
        return 1; // XXX: as NotImplemented();
        // return 0;
    }


    /**
     *  What's the maximum length of a table name?
     *
     *@return                   max name length in bytes
     *@exception  SQLException  if a database-access error occurs.
     */
    public int getMaxTableNameLength() throws SQLException
    {
        // XXX Need to check for Sybase

      return sysnameLength; // per "Programming ODBC for SQLServer" Appendix A
        // per "Programming ODBC for SQLServer" Appendix A
    }


    /**
     *  What's the maximum number of tables in a SELECT?
     *
     *@return                   the maximum
     *@exception  SQLException  if a database-access error occurs.
     */
    public int getMaxTablesInSelect() throws SQLException
    {
        // XXX Need to check for Sybase

        return 16;
        // per "Programming ODBC for SQLServer" Appendix A
    }


    /**
     *  What's the maximum length of a user name?
     *
     *@return                   max name length in bytes
     *@exception  SQLException  if a database-access error occurs.
     */
    public int getMaxUserNameLength() throws SQLException
    {
        // XXX need to find out if this is still true for SYBASE
      return sysnameLength;
    }


    /**
     *  Get a comma separated list of math functions.
     *
     *@return                   the list
     *@exception  SQLException  if a database-access error occurs.
     */
    public String getNumericFunctions() throws SQLException
    {
        // XXX need to find out if this is still true for SYBASE
        return "ABS,ACOS,ASIN,ATAN,ATN2,CEILING,COS,COT,"
                 + "DEGREES,EXP,FLOOR,LOG,LOG10,PI,POWER,RADIANS,"
                 + "RAND,ROUND,SIGN,SIN,SQRT,TAN";
    }


    /**
     *  Get a description of a table's primary key columns. They are ordered by
     *  COLUMN_NAME. <P>
     *
     *  Each primary key column description has the following columns:
     *  <OL>
     *    <LI> <B>TABLE_CAT</B> String =>table catalog (may be null)
     *    <LI> <B>TABLE_SCHEM</B> String =>table schema (may be null)
     *    <LI> <B>TABLE_NAME</B> String =>table name
     *    <LI> <B>COLUMN_NAME</B> String =>column name
     *    <LI> <B>KEY_SEQ</B> short =>sequence number within primary key
     *    <LI> <B>PK_NAME</B> String =>primary key name (may be null)
     *  </OL>
     *
     *
     *@param  catalog           a catalog name; "" retrieves those without a
     *      catalog; null means drop catalog name from the selection criteria
     *@param  schema            a schema name pattern; "" retrieves those
     *      without a schema
     *@param  table             a table name
     *@return                   ResultSet - each row is a primary key column
     *      description
     *@exception  SQLException  if a database-access error occurs.
     */
    public java.sql.ResultSet getPrimaryKeys(
        String catalog,
        String schema,
        String table)
        throws SQLException
    {
        String sql = null;
        java.sql.Statement statement = connection.createStatement();

        java.sql.ResultSet rs;

        if(catalog == null)
        {
            rs = statement.executeQuery("SELECT DB_NAME()");
            rs.next();

            catalog = rs.getString(1);

            rs.close();
        }

         // XXX Security risk.  It 'might' be possible to create
         // a catalog name that when inserted into this sql statement could
         // do other commands.
        sql =
            "select                                     " +
            "  '" + catalog + "' as TABLE_CAT,          " +
            "  USER_NAME(o.uid) as TABLE_SCHEM,         " +
            "  TABLE_NAME=                              " +
            "    (select name from                      " +
            "    dbo.sysobjects                         " +
            "    where o.parent_obj=id),                " +
            "  c.name as COLUMN_NAME,                   " +
            "  k.keyno as KEY_SEQ,                      " +
            "  o.name as PK_NAME                        " +
            "from                                       " +
            "  " + catalog + ".dbo.sysobjects as o,     " +
            "  " + catalog + ".dbo.sysindexes as i,     " +
            "  " + catalog + ".dbo.sysindexkeys as k,   " +
            "  " + catalog + ".dbo.syscolumns as c      " +
            "where                                      " +
            "  o.xtype = 'PK' and                       " +
            "  i.id = o.parent_obj and                  " +
            "  i.name = o.name and                      " +
            "  k.id = o.parent_obj and                  " +
            "  k.indid = i.indid and                    " +
            "  k.id=c.id and                            " +
            "  k.colid=c.colid and                      " +
            "  OBJECT_NAME(o.parent_obj)='" + table + "'";

        if(schema != null) sql += " and USER_NAME(o.uid) LIKE '" + schema + "'" ;

        sql += " order by KEY_SEQ";

        rs = statement.executeQuery(sql);

        return rs;
    }


    /**
     *  Get a description of a catalog's stored procedure parameters and result
     *  columns. <P>
     *
     *  Only descriptions matching the schema, procedure and parameter name
     *  criteria are returned. They are ordered by PROCEDURE_SCHEM and
     *  PROCEDURE_NAME. Within this, the return value, if any, is first. Next
     *  are the parameter descriptions in call order. The column descriptions
     *  follow in column number order. <P>
     *
     *  Each row in the ResultSet is a parameter description or column
     *  description with the following fields:
     *  <OL>
     *    <LI> <B>PROCEDURE_CAT</B> String =>procedure catalog (may be null)
     *
     *    <LI> <B>PROCEDURE_SCHEM</B> String =>procedure schema (may be null)
     *
     *    <LI> <B>PROCEDURE_NAME</B> String =>procedure name
     *    <LI> <B>COLUMN_NAME</B> String =>column/parameter name
     *    <LI> <B>COLUMN_TYPE</B> Short =>kind of column/parameter:
     *    <UL>
     *      <LI> procedureColumnUnknown - nobody knows
     *      <LI> procedureColumnIn - IN parameter
     *      <LI> procedureColumnInOut - INOUT parameter
     *      <LI> procedureColumnOut - OUT parameter
     *      <LI> procedureColumnReturn - procedure return value
     *      <LI> procedureColumnResult - result column in ResultSet
     *    </UL>
     *
     *    <LI> <B>DATA_TYPE</B> short =>SQL type from java.sql.Types
     *    <LI> <B>TYPE_NAME</B> String =>SQL type name
     *    <LI> <B>PRECISION</B> int =>precision
     *    <LI> <B>LENGTH</B> int =>length in bytes of data
     *    <LI> <B>SCALE</B> short =>scale
     *    <LI> <B>RADIX</B> short =>radix
     *    <LI> <B>NULLABLE</B> short =>can it contain NULL?
     *    <UL>
     *      <LI> procedureNoNulls - does not allow NULL values
     *      <LI> procedureNullable - allows NULL values
     *      <LI> procedureNullableUnknown - nullability unknown
     *    </UL>
     *
     *    <LI> <B>REMARKS</B> String =>comment describing parameter/column
     *  </OL>
     *  <P>
     *
     *  <B>Note:</B> Some databases may not return the column descriptions for a
     *  procedure. Additional columns beyond REMARKS can be defined by the
     *  database.
     *
     *@param  catalog               a catalog name; "" retrieves those without a
     *      catalog; null means drop catalog name from the selection criteria
     *@param  schemaPattern         a schema name pattern; "" retrieves those
     *      without a schema
     *@param  procedureNamePattern  a procedure name pattern
     *@param  columnNamePattern     a column name pattern
     *@return                       ResultSet - each row is a stored procedure
     *      parameter or column description
     *@exception  SQLException      if a database-access error occurs.
     *@see                          #getSearchStringEscape
     */
    public java.sql.ResultSet getProcedureColumns(
            String catalog,
            String schemaPattern,
            String procedureNamePattern,
            String columnNamePattern )
             throws SQLException
    {
      int                  i;

      String              sql = null;
      java.sql.Statement  tmpTableStmt = connection.createStatement();

      // XXX We need to come up with something better than a global temporary
      // table.  It could cause problems if two people try to getColumns().
      // (note- it is _unlikely_, not impossible)
      String              tmpTableName = "##t#" + UniqueId.getUniqueId();
      String              lookup       = makeTypeTable(tmpTableStmt);

      // create a temporary table
      sql =
         "create table " + tmpTableName + " (           " +
         "    PROCEDURE_CAT     sysname null,           " +
         "    PROCEDURE_SCHEM   sysname null,           " +
         "    PROCEDURE_NAME    sysname null,           " +
         "    COLUMN_NAME       sysname null,           " +
         "    COLUMN_TYPE       sysname null,           " +
         "    DATA_TYPE         integer null,           " +
         "    TYPE_NAME         sysname null,           " +
         "    [PRECISION]       integer null,           " +
         "    LENGTH            integer null,           " +
         "    SCALE             integer null,           " +
         "    RADIX             integer null,           " +
         "    NULLABLE          integer null,           " +
         "    REMARKS           char(255) null,         " +
         "    ORDINAL_POSITION  integer null            " +
         ")";
      tmpTableStmt.execute(sql);

      // For each procedure in the system add its columns
      // Note-  We have to do them one at a time in case
      // there are databases we don't have access to.
      java.sql.ResultSet          rs = getCatalogs(catalog);
      while(rs.next())
      {
         String cat = rs.getString(1);

         // XXX Security risk.  It 'might' be possible to create
         // a catalog name that when inserted into this sql statement could
         // do other commands.
         sql =
            "insert into  " + tmpTableName + "                " +
            "select                                           " +
            "   PROCEDURE_CAT='" + cat + "',                  " +
            "   PROCEDURE_SCHEM=USER_NAME(o.uid),             " +
            "   PROCEDURE_NAME=o.name,                        " +
            "   COLUMN_NAME=c.name,                           " +
            "   COLUMN_TYPE=1 + convert(integer,              " +
            "                     convert(bit, c.status&64)), " +
            "   DATA_TYPE=l.jdbc_type,                        " +
            "   TYPE_NAME=t.name,                             " +
            "   [PRECISION]=c.prec,                           " +
            "   LENGTH=0,                                     " +
            "   SCALE=c.scale,                                " +
            "   RADIX=10,                                     " +
            "   NULLABLE=convert(integer,                     " +
            "                    convert(bit, c.status&8)),   " +
            "   REMARKS=null,                                 " +
            "   ORDINAL_POSITION=c.colid                      " +
            "from                                             " +
            "   " + cat + ".dbo.sysobjects o,                 " +
            "   " + cat + ".dbo.syscolumns c,                 " +
            "   " + lookup + " l,                             " +
            "   " + cat + ".dbo.systypes t                    " +
            "where o.type = 'P' and o.id=c.id                 " +
            "      and t.usertype=c.usertype                  " +
            "      and l.native_type=c.type                   " +
            "      and o.name like ? and c.name like ?        " +
            "";
         try
         {
            java.sql.PreparedStatement ps = connection.prepareStatement(sql);
            ps.setString(1, procedureNamePattern);
            ps.setString(2, columnNamePattern);
            ps.executeUpdate();
            ps.close();
         }
         catch (SQLException e)
         {
         }
      }
      rs.close();


      sql =
         "select distinct * from " + tmpTableName +
         " where PROCEDURE_SCHEM like ?    " +
         "order by PROCEDURE_SCHEM, PROCEDURE_NAME, ORDINAL_POSITION " ;

      java.sql.PreparedStatement ps = connection.prepareStatement(sql);

      ps.setString(1, schemaPattern);
      rs = ps.executeQuery();

      // We need to do something about deleting the global temporary table
      tmpTableStmt.close();

      return rs;
    }


    /**
     *  Get a description of stored procedures available in a catalog. <P>
     *
     *  Only procedure descriptions matching the schema and procedure name
     *  criteria are returned. They are ordered by PROCEDURE_SCHEM, and
     *  PROCEDURE_NAME. <P>
     *
     *  Each procedure description has the the following columns:
     *  <OL>
     *    <LI> <B>PROCEDURE_CAT</B> String =>procedure catalog (may be null)
     *
     *    <LI> <B>PROCEDURE_SCHEM</B> String =>procedure schema (may be null)
     *
     *    <LI> <B>PROCEDURE_NAME</B> String =>procedure name
     *    <LI> reserved for future use
     *    <LI> reserved for future use
     *    <LI> reserved for future use
     *    <LI> <B>REMARKS</B> String =>explanatory comment on the procedure
     *    <LI> <B>PROCEDURE_TYPE</B> short =>kind of procedure:
     *    <UL>
     *      <LI> procedureResultUnknown - May return a result
     *      <LI> procedureNoResult - Does not return a result
     *      <LI> procedureReturnsResult - Returns a result
     *    </UL>
     *
     *  </OL>
     *
     *
     *@param  catalog               a catalog name; "" retrieves those without a
     *      catalog; null means drop catalog name from the selection criteria
     *@param  schemaPattern         a schema name pattern; "" retrieves those
     *      without a schema
     *@param  procedureNamePattern  a procedure name pattern
     *@return                       ResultSet - each row is a procedure
     *      description
     *@exception  SQLException      if a database-access error occurs.
     *@see                          #getSearchStringEscape
     */
    public java.sql.ResultSet getProcedures( String catalog,
            String schemaPattern,
            String procedureNamePattern )
             throws SQLException
    {
      /*
        debugPrintln( "Inside of getProcedures" );
        debugPrintln( "  catalog is |" + catalog + "|" );
        debugPrintln( "  schemaPattern is " + schemaPattern );
        debugPrintln( "  procedurePattern is " + procedureNamePattern );
       */

      int                 paramIndex;
      int                 i;

      String              sql = null;
      java.sql.Statement  tmpTableStmt = connection.createStatement();

      // XXX We need to come up with something better than a global temporary
      // table.  It could cause problems if two people try to getProcedures().
      // (note- it is unlikely to cause any problems, but it is possible)
      String              tmpTableName = "##t#" + UniqueId.getUniqueId();
      String              schemaCriteria;
      String              tableCriteria;
      String              typesCriteria;


      // create a temporary table
      sql =
         "create table " + tmpTableName + " ( " +
         "    cat    sysname null,            " +
         "    schem  sysname null,            " +
         "    name   sysname null,            " +
         "    type   smallint null,           " +
         "    rem    char(255) null)          ";
      tmpTableStmt.execute(sql);

      // For each database in the system add its tables
      // Note-  We have to do them one at a time in case
      // there are databases we don't have access to.
      java.sql.ResultSet rs = getCatalogs(catalog);
      while(rs.next())
      {
         String cat = rs.getString(1);

         sql =
            "insert into " + tmpTableName + "                            " +
            "    select '" + cat + "', USER_NAME(uid), name, 0, null     " +
            "        from " + cat + ".dbo.sysobjects                     " +
            "        where type = 'P'                                    ";
         try
         {
            tmpTableStmt.executeUpdate(sql);
         }
         catch (SQLException e)
         {
            // We might not have access to certain tables.  Just ignore the
            // error if this is the case.
         }
      }
      rs.close();

      sql = "select "
         + "  PROCEDURE_CAT=cat,     "
         + "  PROCEDURE_SCHEM=schem, "
         + "  PROCEDURE_NAME=name,   "
         + "  -1 NUM_INPUT_PARAMS,   "
         + "  -1 NUM_OUTPUT_PARAMS,  "
         + "  -1 NUM_RESULT_SETS,    "
         + "  REMARKS=rem,           "
         + "  PROCEDURE_TYPE=type    "
         + " from " + tmpTableName
         + " where schem like ?  and "
         + " name like ?   ";

      java.sql.PreparedStatement ps = connection.prepareStatement(sql);


      ps.setString(1, schemaPattern);
      ps.setString(2, procedureNamePattern);
      rs = ps.executeQuery();

      // We need to do something about deleting the global temporary table
      tmpTableStmt.close();

            return rs;
    }


    /**
     *  What's the database vendor's preferred term for "procedure"?
     *
     *@return                   the vendor term
     *@exception  SQLException  if a database-access error occurs.
     */
    public String getProcedureTerm() throws SQLException
    {
        // XXX Need to check for Sybase

        // per "Programming ODBC for SQLServer" Appendix A
        return "stored procedure";
    }


    /**
     *  Get the schema names available in this database. The results are ordered
     *  by schema name. <P>
     *
     *  The schema column is:
     *  <OL>
     *    <LI> <B>TABLE_SCHEM</B> String =>schema name
     *  </OL>
     *
     *
     *@return                   ResultSet - each row has a single String column
     *      that is a schema name
     *@exception  SQLException  if a database-access error occurs.
     */
    public java.sql.ResultSet getSchemas() throws SQLException
    {
        // XXX We should really clean up all these temporary tables.
        java.sql.Statement statement = connection.createStatement();
        java.sql.ResultSet rs;

        String sql =
            "select                                         " +
            "  u.name as TABLE_SCHEM,                       " +
            "  DB_NAME() as TABLE_CATALOG                   " +
            "from                                           " +
            "  dbo.sysusers as u                            " +
            "where                                          " +
            "  issqlrole=0                                  ";

        return statement.executeQuery(sql);
    }


    /**
     *  What's the database vendor's preferred term for "schema"?
     *
     *@return                   the vendor term
     *@exception  SQLException  if a database-access error occurs.
     */
    public String getSchemaTerm() throws SQLException
    {
        // need to check this for Sybase
        return "owner";
    }


    /**
     *  This is the string that can be used to escape '_' or '%' in the string
     *  pattern style catalog search parameters. <P>
     *
     *  The '_' character represents any single character. <P>
     *
     *  The '%' character represents any sequence of zero or more characters.
     *
     *@return                   the string used to escape wildcard characters
     *@exception  SQLException  if a database-access error occurs.
     */
    public String getSearchStringEscape() throws SQLException
    {
        // XXX Need to check for Sybase

        return "\\";
        // per "Programming ODBC for SQLServer" Appendix A
    }


    /**
     *  Get a comma separated list of all a database's SQL keywords that are NOT
     *  also SQL92 keywords.
     *
     *@return                   the list
     *@exception  SQLException  if a database-access error occurs.
     */
    public String getSQLKeywords() throws SQLException
    {
        return "BREAK,BROWSE,BULK,CHECKPOINT,CLUSTERED,COMMITTED,COMPUTE,"
            +"CONFIRM,CONTROLROW,DATABASE,DBCC,DISK,DISTRIBUTED,DUMMY,DUMP,"
            +"ERRLVL,ERROREXIT,EXIT,FILE,FILLFACTOR,FLOPPY,HOLDLOCK,"
            +"IDENTITY_INSERT,IDENTITYCOL,IF,KILL,LINENO,LOAD,MIRROREXIT,"
            +"NONCLUSTERED,OFF,OFFSETS,ONCE,OVER,PERCENT,PERM,PERMANENT,PLAN,"
            +"PRINT,PROC,PROCESSEXIT,RAISERROR,READ,READTEXT,RECONFIGURE,"
            +"REPEATABLE,RETURN,ROWCOUNT,RULE,SAVE,SERIALIZABLE,SETUSER,"
            +"SHUTDOWN,STATISTICS,TAPE,TEMP,TEXTSIZE,TOP,TRAN,TRIGGER,"
            +"TRUNCATE,TSEQUEL,UNCOMMITTED,UPDATETEXT,USE,WAITFOR,WHILE,"
            +"WRITETEXT";
    }


    /**
     *  Get a comma separated list of string functions.
     *
     *@return                   the list
     *@exception  SQLException  if a database-access error occurs.
     */
    public String getStringFunctions() throws SQLException
    {
        return "LTRIM,SOUNDEX,ASCII,PATINDEX,SPACE,CHAR,REPLICATE,"
                 + "STR,CHARINDEX,REVERSE,STUFF,DIFFERENCE,RIGHT,"
                 + "SUBSTRING,LOWER,RTRIM,UPPER";
    }


    /**
     *  Get a comma separated list of system functions.
     *
     *@return                   the list
     *@exception  SQLException  if a database-access error occurs.
     */
    public String getSystemFunctions() throws SQLException
    {
        return
                "COALESCE," +
                "COL_LENGTH," +
                "COL_NAME," +
                "DATALENGTH," +
                "DB_ID," +
                "DB_NAME," +
                "GETANSINULL," +
                "HOST_ID," +
                "HOST_NAME," +
                "IDENT_INCR," +
                "IDENT_SEED," +
                "INDEX_COL," +
                "ISNULL," +
                "NULLIF," +
                "OBJECT_ID," +
                "OBJECT_NAME," +
                "STATS_DATE," +
                "SUSER_ID," +
                "SUSER_NAME," +
                "USER_ID," +
                "USER_NAME";
    }


    /**
     *  Get a description of the access rights for each table available in a
     *  catalog. Note that a table privilege applies to one or more columns in
     *  the table. It would be wrong to assume that this priviledge applies to
     *  all columns (this may be true for some systems but is not true for all.)
     *  <P>
     *
     *  Only privileges matching the schema and table name criteria are
     *  returned. They are ordered by TABLE_SCHEM, TABLE_NAME, and PRIVILEGE.
     *  <P>
     *
     *  Each privilige description has the following columns:
     *  <OL>
     *    <LI> <B>TABLE_CAT</B> String =>table catalog (may be null)
     *    <LI> <B>TABLE_SCHEM</B> String =>table schema (may be null)
     *    <LI> <B>TABLE_NAME</B> String =>table name
     *    <LI> <B>GRANTOR</B> =>grantor of access (may be null)
     *    <LI> <B>GRANTEE</B> String =>grantee of access
     *    <LI> <B>PRIVILEGE</B> String =>name of access (SELECT, INSERT, UPDATE,
     *    REFRENCES, ...)
     *    <LI> <B>IS_GRANTABLE</B> String =>"YES" if grantee is permitted to
     *    grant to others; "NO" if not; null if unknown
     *  </OL>
     *
     *
     *@param  catalog           a catalog name; "" retrieves those without a
     *      catalog; null means drop catalog name from the selection criteria
     *@param  schemaPattern     a schema name pattern; "" retrieves those
     *      without a schema
     *@param  tableNamePattern  a table name pattern
     *@return                   ResultSet - each row is a table privilege
     *      description
     *@exception  SQLException  if a database-access error occurs.
     *@see                      #getSearchStringEscape
     */
    public java.sql.ResultSet getTablePrivileges(
            String catalog,
            String schemaPattern,
            String tableNamePattern ) throws SQLException
    {
        NotImplemented();
        return null;
    }


    /**
     *  Get a description of tables available in a catalog. <P>
     *
     *  Only table descriptions matching the catalog, schema, table name and
     *  type criteria are returned. They are ordered by TABLE_TYPE, TABLE_SCHEM
     *  and TABLE_NAME. <P>
     *
     *  Each table description has the following columns:
     *  <OL>
     *    <LI> <B>TABLE_CAT</B> String =>table catalog (may be null)
     *    <LI> <B>TABLE_SCHEM</B> String =>table schema (may be null)
     *    <LI> <B>TABLE_NAME</B> String =>table name
     *    <LI> <B>TABLE_TYPE</B> String =>table type. Typical types are "TABLE",
     *    "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY",
     *    "ALIAS", "SYNONYM".
     *    <LI> <B>REMARKS</B> String =>explanatory comment on the table
     *  </OL>
     *  <P>
     *
     *  <B>Note:</B> Some databases may not return information for all tables.
     *
     *@param  catalog           a catalog name; "" retrieves those without a
     *      catalog; null means drop catalog name from the selection criteria
     *@param  schemaPattern     a schema name pattern; "" retrieves those
     *      without a schema
     *@param  tableNamePattern  a table name pattern
     *@param  types             a list of table types to include; null returns
     *      all types
     *@return                   ResultSet - each row is a table description
     *@exception  SQLException  if a database-access error occurs.
     *@see                      #getSearchStringEscape
     */
    public java.sql.ResultSet getTables(
            String catalog,
            String schemaPattern,
            String tableNamePattern,
            String types[] )
            throws SQLException
    {
        java.sql.Statement statement = connection.createStatement();
        java.sql.ResultSet rs;

        if(types != null)
        {
            for(int i=0; i<types.length; i++)
            {
                if(types[i].equalsIgnoreCase("SYSTEM TABLE")) types[i] = "S";
                else if(types[i].equalsIgnoreCase("TABLE")) types[i] = "U";
                else if(types[i].equalsIgnoreCase("VIEW")) types[i] = "V";
                else types[i] = "U";
            }
        }
        else
        {
            types = new String[]{"S", "U", "V"};
        }

        if(catalog == null)
        {
            rs = statement.executeQuery("SELECT DB_NAME()");
            rs.next();

            catalog = rs.getString(1);

            rs.close();
        }

        String sql =
         "select                            " +
         "  '"+catalog+"' as TABLE_CAT,     " +
         "  USER_NAME(uid) as TABLE_SCHEM,  " +
         "  name as TABLE_NAME,             " +
         "  TABLE_TYPE=                     " +
         "    CASE type                     " +
         "      WHEN 'S' THEN 'SYSTEM TABLE'" +
         "      WHEN 'U' THEN 'TABLE'       " +
         "      WHEN 'V' THEN 'VIEW'        " +
         "      END,                        " +
         "  null as REMARKS                 " +
         "from                              " +
         "  " + catalog + ".dbo.sysobjects  " +
         "where                             " +
         "  type in (";

        for(int i=0; i<types.length; i++)
        {
            if(i > 0) sql += ", ";
            sql += "'"+types[i]+"'";
        }

        sql += ") ";

        if(schemaPattern != null) sql += "  and USER_NAME(uid) LIKE '" + schemaPattern +"'";
        if(tableNamePattern != null) sql += "  and name LIKE '" + tableNamePattern +"'";

        sql += "  order by TABLE_CAT, TABLE_SCHEM, TABLE_NAME";

        rs = statement.executeQuery(sql);

        return rs;
    }


    /**
     *  Get the table types available in this database. The results are ordered
     *  by table type. <P>
     *
     *  The table type is:
     *  <OL>
     *    <LI> <B>TABLE_TYPE</B> String =>table type. Typical types are "TABLE",
     *    "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY",
     *    "ALIAS", "SYNONYM".
     *  </OL>
     *
     *
     *@return                   ResultSet - each row has a single String column
     *      that is a table type
     *@exception  SQLException  if a database-access error occurs.
     */
    public java.sql.ResultSet getTableTypes() throws SQLException
    {
        // XXX see if this is still true for Sybase
        String sql =
                "select 'TABLE' TABLE_TYPE              " +
                "union select 'VIEW' TABLE_TYPE         " +
                "union select 'SYSTEM TABLE' TABLE_TYPE ";
        java.sql.Statement stmt = connection.createStatement();
        java.sql.ResultSet rs = stmt.executeQuery( sql );

        return rs;
    }


    /**
     *  Get a comma separated list of time and date functions.
     *
     *@return                   the list
     *@exception  SQLException  if a database-access error occurs.
     */
    public String getTimeDateFunctions() throws SQLException
    {
        return "GETDATE,DATEPART,DATENAME,DATEDIFF,DATEADD";
    }


    /**
     *  Get a description of all the standard SQL types supported by this
     *  database. They are ordered by DATA_TYPE and then by how closely the data
     *  type maps to the corresponding JDBC SQL type. <P>
     *
     *  Each type description has the following columns:
     *  <OL>
     *    <LI> <B>TYPE_NAME</B> String =>Type name
     *    <LI> <B>DATA_TYPE</B> short =>SQL data type from java.sql.Types
     *    <LI> <B>PRECISION</B> int =>maximum precision
     *    <LI> <B>LITERAL_PREFIX</B> String =>prefix used to quote a literal
     *    (may be null)
     *    <LI> <B>LITERAL_SUFFIX</B> String =>suffix used to quote a literal
     *    (may be null)
     *    <LI> <B>CREATE_PARAMS</B> String =>parameters used in creating the
     *    type (may be null)
     *    <LI> <B>NULLABLE</B> short =>can you use NULL for this type?
     *    <UL>
     *      <LI> typeNoNulls - does not allow NULL values
     *      <LI> typeNullable - allows NULL values
     *      <LI> typeNullableUnknown - nullability unknown
     *    </UL>
     *
     *    <LI> <B>CASE_SENSITIVE</B> boolean=>is it case sensitive?
     *    <LI> <B>SEARCHABLE</B> short =>can you use "WHERE" based on this type:
     *
     *    <UL>
     *      <LI> typePredNone - No support
     *      <LI> typePredChar - Only supported with WHERE .. LIKE
     *      <LI> typePredBasic - Supported except for WHERE .. LIKE
     *      <LI> typeSearchable - Supported for all WHERE ..
     *    </UL>
     *
     *    <LI> <B>UNSIGNED_ATTRIBUTE</B> boolean =>is it unsigned?
     *    <LI> <B>FIXED_PREC_SCALE</B> boolean =>can it be a money value?
     *    <LI> <B>AUTO_INCREMENT</B> boolean =>can it be used for an
     *    auto-increment value?
     *    <LI> <B>LOCAL_TYPE_NAME</B> String =>localized version of type name
     *    (may be null)
     *    <LI> <B>MINIMUM_SCALE</B> short =>minimum scale supported
     *    <LI> <B>MAXIMUM_SCALE</B> short =>maximum scale supported
     *    <LI> <B>SQL_DATA_TYPE</B> int =>unused
     *    <LI> <B>SQL_DATETIME_SUB</B> int =>unused
     *    <LI> <B>NUM_PREC_RADIX</B> int =>usually 2 or 10
     *  </OL>
     *
     *
     *@return                   ResultSet - each row is a SQL type description
     *@exception  SQLException  if a database-access error occurs.
     */
    public java.sql.ResultSet getTypeInfo() throws SQLException
    {
        NotImplemented();
        return null;
    }


    /**
     *  What's the url for this database?
     *
     *@return                   the url or null if it can't be generated
     *@exception  SQLException  if a database-access error occurs.
     */
    public String getURL() throws SQLException
    {
        return ( ( ConnectionHelper ) connection ).getUrl();
    }


    /**
     *  What's our user name as known to the database?
     *
     *@return                   our database user name
     *@exception  SQLException  if a database-access error occurs.
     */
    public String getUserName() throws SQLException
    {
        java.sql.Statement s = null;
        java.sql.ResultSet rs = null;
        String result = "";

        try {
            s = connection.createStatement();
            // XXX Need to check this for Sybase
            rs = s.executeQuery( "select USER_NAME()" );

            if ( !rs.next() ) {
                throw new SQLException( "Couldn't determine user name" );
            }
            result = rs.getString( 1 );
        }
        finally {
            if ( rs != null ) {
                rs.close();
            }
            if ( s != null ) {
                s.close();
            }
        }
        return result;
    }


    /**
     *  Get a description of a table's columns that are automatically updated
     *  when any value in a row is updated. They are unordered. <P>
     *
     *  Each column description has the following columns:
     *  <OL>
     *    <LI> <B>SCOPE</B> short =>is not used
     *    <LI> <B>COLUMN_NAME</B> String =>column name
     *    <LI> <B>DATA_TYPE</B> short =>SQL data type from java.sql.Types
     *    <LI> <B>TYPE_NAME</B> String =>Data source dependent type name
     *    <LI> <B>COLUMN_SIZE</B> int =>precision
     *    <LI> <B>BUFFER_LENGTH</B> int =>length of column value in bytes
     *    <LI> <B>DECIMAL_DIGITS</B> short =>scale
     *    <LI> <B>PSEUDO_COLUMN</B> short =>is this a pseudo column like an
     *    Oracle ROWID
     *    <UL>
     *      <LI> versionColumnUnknown - may or may not be pseudo column
     *      <LI> versionColumnNotPseudo - is NOT a pseudo column
     *      <LI> versionColumnPseudo - is a pseudo column
     *    </UL>
     *
     *  </OL>
     *
     *
     *@param  catalog           a catalog name; "" retrieves those without a
     *      catalog; null means drop catalog name from the selection criteria
     *@param  schema            a schema name; "" retrieves those without a
     *      schema
     *@param  table             a table name
     *@return                   ResultSet - each row is a column description
     *@exception  SQLException  if a database-access error occurs.
     */
    public java.sql.ResultSet getVersionColumns( String catalog, String schema,
            String table ) throws SQLException
    {
        NotImplemented();
        return null;
    }


    /**
     *  Does a catalog appear at the start of a qualified table name? (Otherwise
     *  it appears at the end)
     *
     *@return                   true if it appears at the start
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean isCatalogAtStart() throws SQLException
    {
        return true;
    }


    /**
     *  Is the database in read-only mode?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean isReadOnly() throws SQLException
    {
        return false;
    }


    /**
     *  JDBC 2.0 Gets a description of the user-defined types defined in a
     *  particular schema. Schema-specific UDTs may have type JAVA_OBJECT,
     *  STRUCT, or DISTINCT. <P>
     *
     *  Only types matching the catalog, schema, type name and type criteria are
     *  returned. They are ordered by DATA_TYPE, TYPE_SCHEM and TYPE_NAME. The
     *  type name parameter may be a fully-qualified name. In this case, the
     *  catalog and schemaPattern parameters are ignored. <P>
     *
     *  Each type description has the following columns:
     *  <OL>
     *    <LI> <B>TYPE_CAT</B> String =>the type's catalog (may be null)
     *    <LI> <B>TYPE_SCHEM</B> String =>type's schema (may be null)
     *    <LI> <B>TYPE_NAME</B> String =>type name
     *    <LI> <B>CLASS_NAME</B> String =>Java class name
     *    <LI> <B>DATA_TYPE</B> String =>type value defined in java.sql.Types.
     *    One of JAVA_OBJECT, STRUCT, or DISTINCT
     *    <LI> <B>REMARKS</B> String =>explanatory comment on the type
     *  </OL>
     *  <P>
     *
     *  <B>Note:</B> If the driver does not support UDTs, an empty result set is
     *  returned.
     *
     *@param  catalog           a catalog name; "" retrieves those without a
     *      catalog; null means drop catalog name from the selection criteria
     *@param  schemaPattern     a schema name pattern; "" retrieves those
     *      without a schema
     *@param  typeNamePattern   a type name pattern; may be a fully-qualified
     *      name
     *@param  types             a list of user-named types to include
     *      (JAVA_OBJECT, STRUCT, or DISTINCT); null returns all types
     *@return                   ResultSet - each row is a type description
     *@exception  SQLException  if a database access error occurs
     */
    public java.sql.ResultSet getUDTs( String catalog, String schemaPattern,
            String typeNamePattern, int[] types )
             throws SQLException
    {
        NotImplemented();
        return null;
    }


    /**
     *  JDBC 2.0 Retrieves the connection that produced this metadata object.
     *
     *@return                   the connection that produced this metadata
     *      object
     *@exception  SQLException  @todo Description of Exception
     */
    public java.sql.Connection getConnection() throws SQLException
    {
        return connection;
    }



    /**
     *  Are concatenations between NULL and non-NULL values NULL? A
     *  JDBC-Compliant driver always returns true.
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean nullPlusNonNullIsNull() throws SQLException
    {
        // XXX Need to check for Sybase.

        // MS SQLServer seems to break with the SQL standard here.
        // maybe there is an option to make null behavior comply
        //
        // SAfe: Nope, it seems to work fine in SQL Server 7.0
        return true;
    }


    /**
     *  Are NULL values sorted at the end regardless of sort order?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean nullsAreSortedAtEnd() throws SQLException
    {
        // XXX Need to check for Sybase
        return false;
    }


    /**
     *  Are NULL values sorted at the start regardless of sort order?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean nullsAreSortedAtStart() throws SQLException
    {
        // XXX Need to check for Sybase
        return false;
    }


    /**
     *  Are NULL values sorted high?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean nullsAreSortedHigh() throws SQLException
    {
        // XXX Need to check for Sybase
        return false;
    }


    /**
     *  Are NULL values sorted low?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean nullsAreSortedLow() throws SQLException
    {
        // XXX Need to check for Sybase
        return true;
    }


    /**
     *  Does the database treat mixed case unquoted SQL identifiers as case
     *  insensitive and store them in lower case?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean storesLowerCaseIdentifiers() throws SQLException
    {
        // XXX Need to check for Sybase
        return false;
    }


    /**
     *  Does the database treat mixed case quoted SQL identifiers as case
     *  insensitive and store them in lower case?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException
    {
        // XXX Need to check for Sybase
        return false;
    }


    /**
     *  Does the database treat mixed case unquoted SQL identifiers as case
     *  insensitive and store them in mixed case?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean storesMixedCaseIdentifiers() throws SQLException
    {
        // XXX Will have to use [sp_server_info] for this.
        NotImplemented();
        return false;
    }


    /**
     *  Does the database treat mixed case quoted SQL identifiers as case
     *  insensitive and store them in mixed case?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException
    {
        // XXX Will have to use [sp_server_info] for this.
        NotImplemented();
        return false;
    }


    /**
     *  Does the database treat mixed case unquoted SQL identifiers as case
     *  insensitive and store them in upper case?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean storesUpperCaseIdentifiers() throws SQLException
    {
        // XXX Need to check for Sybase
        return false;
    }


    /**
     *  Does the database treat mixed case quoted SQL identifiers as case
     *  insensitive and store them in upper case?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException
    {
        // XXX Need to check for Sybase
        return false;
    }


    //--------------------------------------------------------------------
    // Functions describing which features are supported.

    /**
     *  Is "ALTER TABLE" with add column supported?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsAlterTableWithAddColumn() throws SQLException
    {
        return true;
    }


    /**
     *  Is "ALTER TABLE" with drop column supported?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsAlterTableWithDropColumn() throws SQLException
    {
        return false;
    }


    /**
     *  Is the ANSI92 entry level SQL grammar supported? All JDBC-Compliant
     *  drivers must return true.
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsANSI92EntryLevelSQL() throws SQLException
    {
        // XXX Will have to check for Sybase
        return true;
    }


    /**
     *  Is the ANSI92 full SQL grammar supported?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsANSI92FullSQL() throws SQLException
    {
        // XXX Will have to check for Sybase
        return false;
    }


    /**
     *  Is the ANSI92 intermediate SQL grammar supported?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsANSI92IntermediateSQL() throws SQLException
    {
        // XXX Will have to check for Sybase
        return false;
    }


    /**
     *  Can a catalog name be used in a data manipulation statement?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsCatalogsInDataManipulation() throws SQLException
    {
        // XXX needs to be checked for Sybase
        return true;
    }


    /**
     *  Can a catalog name be used in an index definition statement?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException
    {
        // XXX needs to be checked for Sybase
        return true;
    }


    /**
     *  Can a catalog name be used in a privilege definition statement?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException
    {
        // XXX needs to be checked for Sybase
        return true;
    }


    /**
     *  Can a catalog name be used in a procedure call statement?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsCatalogsInProcedureCalls() throws SQLException
    {
        // XXX needs to be checked for Sybase
        return true;
    }


    /**
     *  Can a catalog name be used in a table definition statement?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsCatalogsInTableDefinitions() throws SQLException
    {
        // XXX needs to be checked for Sybase
        return true;
    }


    /**
     *  Is column aliasing supported? <P>
     *
     *  If so, the SQL AS clause can be used to provide names for computed
     *  columns or to provide alias names for columns as required. A
     *  JDBC-Compliant driver always returns true.
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsColumnAliasing() throws SQLException
    {
        // XXX needs to be checked for Sybase
        return true;
    }


    /**
     *  Is the CONVERT function between SQL types supported?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsConvert() throws SQLException
    {
        return true;
    }


    /**
     *  Is CONVERT between the given SQL types supported?
     *
     *@param  fromType          the type to convert from
     *@param  toType            the type to convert to
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     *@see                      Types
     */
    public boolean supportsConvert( int fromType, int toType ) throws SQLException
    {
        NotImplemented();
        return false;
    }


    /**
     *  Is the ODBC Core SQL grammar supported?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsCoreSQLGrammar() throws SQLException
    {
        // XXX Need to check for Sybase
        return true;
    }


    /**
     *  Are correlated subqueries supported? A JDBC-Compliant driver always
     *  returns true.
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsCorrelatedSubqueries() throws SQLException
    {
        // XXX Need to check for Sybase
        return true;
    }


    /**
     *  Are both data definition and data manipulation statements within a
     *  transaction supported?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsDataDefinitionAndDataManipulationTransactions()
             throws SQLException
    {
        // XXX Need to check for Sybase
        return tds.getTdsVer()>=Tds.TDS70 || (tds.getTdsVer()==Tds.TDS42 && tds.getServerType()==Tds.SQLSERVER);
    }


    /**
     *  Are only data manipulation statements within a transaction supported?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsDataManipulationTransactionsOnly()
             throws SQLException
    {
        // XXX Need to check for Sybase
        return tds.getTdsVer()<Tds.TDS70 && (tds.getTdsVer()!=Tds.TDS42 || tds.getServerType()!=Tds.SQLSERVER);
    }


    /**
     *  If table correlation names are supported, are they restricted to be
     *  different from the names of the tables?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsDifferentTableCorrelationNames() throws SQLException
    {
        // XXX Need to check for Sybase
        return false;
    }


    /**
     *  Are expressions in "ORDER BY" lists supported?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsExpressionsInOrderBy() throws SQLException
    {
        // XXX Need to check for Sybase
        return true;
    }


    /**
     *  Is the ODBC Extended SQL grammar supported?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsExtendedSQLGrammar() throws SQLException
    {
        // XXX Need to check for Sybase
        return false;
    }


    /**
     *  Are full nested outer joins supported?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsFullOuterJoins() throws SQLException
    {
        // XXX Need to check for Sybase

        return true;
        // per "Programming ODBC for SQLServer" Appendix A
    }


    /**
     *  Is some form of "GROUP BY" clause supported?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsGroupBy() throws SQLException
    {
        return true;
    }


    /**
     *  Can a "GROUP BY" clause add columns not in the SELECT provided it
     *  specifies all the columns in the SELECT?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsGroupByBeyondSelect() throws SQLException
    {
        // XXX Need to check for Sybase

        return true;
        // per "Programming ODBC for SQLServer" Appendix A
    }


    /**
     *  Can a "GROUP BY" clause use columns not in the SELECT?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsGroupByUnrelated() throws SQLException
    {
        // XXX need to check this for Sybase
        return true;
    }


    /**
     *  Is the SQL Integrity Enhancement Facility supported?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsIntegrityEnhancementFacility() throws SQLException
    {
        // XXX Need to check for Sybase
        return false;
    }


    /**
     *  Is the escape character in "LIKE" clauses supported? A JDBC-Compliant
     *  driver always returns true.
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsLikeEscapeClause() throws SQLException
    {
        // XXX Need to check for Sybase

        return true;
        // per "Programming ODBC for SQLServer" Appendix A
    }


    /**
     *  Is there limited support for outer joins? (This will be true if
     *  supportFullOuterJoins is true.)
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsLimitedOuterJoins() throws SQLException
    {
        return true;
    }


    /**
     *  Is the ODBC Minimum SQL grammar supported? All JDBC-Compliant drivers
     *  must return true.
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsMinimumSQLGrammar() throws SQLException
    {
        // XXX Need to check for Sybase
        return true;
    }


    /**
     *  Does the database treat mixed case unquoted SQL identifiers as case
     *  sensitive and as a result store them in mixed case? A JDBC-Compliant
     *  driver will always return false.
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsMixedCaseIdentifiers() throws SQLException
    {
        // XXX Will have to use [sp_server_info] to find this out
        NotImplemented();
        return false;
    }


    /**
     *  Does the database treat mixed case quoted SQL identifiers as case
     *  sensitive and as a result store them in mixed case? A JDBC-Compliant
     *  driver will always return true.
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException
    {
        // XXX Will have to use [sp_server_info] to find this out
        NotImplemented();
        return false;
    }


    /**
     *  Are multiple ResultSets from a single execute supported?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsMultipleResultSets() throws SQLException
    {
        return true;
    }


    /**
     *  Can we have multiple transactions open at once (on different
     *  connections)?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsMultipleTransactions() throws SQLException
    {
        return true;
    }


    /**
     *  Can columns be defined as non-nullable? A JDBC-Compliant driver always
     *  returns true.
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsNonNullableColumns() throws SQLException
    {
        return true;
    }


    /**
     *  Can cursors remain open across commits?
     *
     *@return                   true if cursors always remain open; false if
     *      they might not remain open
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException
    {
        // XXX Need to check for Sybase
        return false;
    }


    /**
     *  Can cursors remain open across rollbacks?
     *
     *@return                   true if cursors always remain open; false if
     *      they might not remain open
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException
    {
        // XXX Need to check for Sybase
        return false;
    }


    /**
     *  Can statements remain open across commits?
     *
     *@return                   true if statements always remain open; false if
     *      they might not remain open
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException
    {
        return true;
    }


    /**
     *  Can statements remain open across rollbacks?
     *
     *@return                   true if statements always remain open; false if
     *      they might not remain open
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException
    {
        return true;
    }


    /**
     *  Can an "ORDER BY" clause use columns not in the SELECT?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsOrderByUnrelated() throws SQLException
    {
        // XXX need to verify for Sybase
        return true;
    }


    /**
     *  Is some form of outer join supported?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsOuterJoins() throws SQLException
    {
        return true;
    }


    /**
     *  Is positioned DELETE supported?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsPositionedDelete() throws SQLException
    {
        // XXX Could we support it in the future?
        return false;
    }


    /**
     *  Is positioned UPDATE supported?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsPositionedUpdate() throws SQLException
    {
        // XXX Could we support it in the future?
        return false;
    }


    /**
     *  Can a schema name be used in a data manipulation statement?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsSchemasInDataManipulation() throws SQLException
    {
        // XXX needs to be checked for Sybase
        return true;
    }


    /**
     *  Can a schema name be used in an index definition statement?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsSchemasInIndexDefinitions() throws SQLException
    {
        // XXX needs to be checked for Sybase
        return true;
    }


    /**
     *  Can a schema name be used in a privilege definition statement?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException
    {
        // XXX needs to be checked for Sybase
        return true;
    }


    /**
     *  Can a schema name be used in a procedure call statement?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsSchemasInProcedureCalls() throws SQLException
    {
        // XXX needs to be checked for Sybase
        return true;
    }


    /**
     *  Can a schema name be used in a table definition statement?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsSchemasInTableDefinitions() throws SQLException
    {
        // XXX needs to be checked for Sybase
        return true;
    }


    /**
     *  Is SELECT for UPDATE supported?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsSelectForUpdate() throws SQLException
    {
        // XXX Need to check for Sybase
        return false;
    }


    /**
     *  Are stored procedure calls using the stored procedure escape syntax
     *  supported?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsStoredProcedures() throws SQLException
    {
        return true;
    }


    /**
     *  Are subqueries in comparison expressions supported? A JDBC-Compliant
     *  driver always returns true.
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsSubqueriesInComparisons() throws SQLException
    {
        return true;
    }


    /**
     *  Are subqueries in 'exists' expressions supported? A JDBC-Compliant
     *  driver always returns true.
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsSubqueriesInExists() throws SQLException
    {
        return true;
    }


    /**
     *  Are subqueries in 'in' statements supported? A JDBC-Compliant driver
     *  always returns true.
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsSubqueriesInIns() throws SQLException
    {
        return true;
    }


    /**
     *  Are subqueries in quantified expressions supported? A JDBC-Compliant
     *  driver always returns true.
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsSubqueriesInQuantifieds() throws SQLException
    {
        // XXX Need to check for Sybase
        return true;
    }


    /**
     *  Are table correlation names supported? A JDBC-Compliant driver always
     *  returns true.
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsTableCorrelationNames() throws SQLException
    {
        return true;
    }


    /**
     *  Does the database support the given transaction isolation level?
     *
     *@param  level             the values are defined in java.sql.Connection
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     *@see                      Connection
     */
    public boolean supportsTransactionIsolationLevel( int level )
             throws SQLException
    {
        return true;
    }


    /**
     *  Are transactions supported? If not, commit is a noop and the isolation
     *  level is TRANSACTION_NONE.
     *
     *@return                   true if transactions are supported
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsTransactions() throws SQLException
    {
        return true;
    }


    /**
     *  Is SQL UNION supported?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsUnion() throws SQLException
    {
        return true;
    }


    /**
     *  Is SQL UNION ALL supported?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsUnionAll() throws SQLException
    {
        return true;
    }


    /**
     *  Does the database use a file for each table?
     *
     *@return                   true if the database uses a local file for each
     *      table
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean usesLocalFilePerTable() throws SQLException
    {
        return false;
    }


    /**
     *  Does the database store tables in a local file?
     *
     *@return                   true if so
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean usesLocalFiles() throws SQLException
    {
        return false;
    }


    //--------------------------JDBC 2.0-----------------------------

    /**
     *  JDBC 2.0 Does the database support the given result set type?
     *
     *@param  type              defined in <code>java.sql.ResultSet</code>
     *@return                   <code>true</code> if so; <code>false</code>
     *      otherwise
     *@exception  SQLException  if a database access error occurs
     *@see                      Connection
     */
    public boolean supportsResultSetType( int type ) throws SQLException
    {
        NotImplemented();
        return false;
    }


    /**
     *  JDBC 2.0 Does the database support the concurrency type in combination
     *  with the given result set type?
     *
     *@param  type              defined in <code>java.sql.ResultSet</code>
     *@param  concurrency       type defined in <code>java.sql.ResultSet</code>
     *@return                   <code>true</code> if so; <code>false</code>
     *      otherwise
     *@exception  SQLException  if a database access error occurs
     *@see                      Connection
     */
    public boolean supportsResultSetConcurrency( int type, int concurrency )
             throws SQLException
    {
        NotImplemented();
        return false;
    }


    /**
     *  JDBC 2.0 Indicates whether a result set's own updates are visible.
     *
     *@param  type              @todo Description of Parameter
     *@return                   <code>true</code> if updates are visible for the
     *      result set type; <code>false</code> otherwise
     *@exception  SQLException  if a database access error occurs
     */
    public boolean ownUpdatesAreVisible( int type ) throws SQLException
    {

        NotImplemented();
        return false;
    }


    /**
     *  JDBC 2.0 Indicates whether a result set's own deletes are visible.
     *
     *@param  type              @todo Description of Parameter
     *@return                   <code>true</code> if deletes are visible for the
     *      result set type; <code>false</code> otherwise
     *@exception  SQLException  if a database access error occurs
     */
    public boolean ownDeletesAreVisible( int type ) throws SQLException
    {
        NotImplemented();
        return false;
    }


    /**
     *  JDBC 2.0 Indicates whether a result set's own inserts are visible.
     *
     *@param  type              @todo Description of Parameter
     *@return                   <code>true</code> if inserts are visible for the
     *      result set type; <code>false</code> otherwise
     *@exception  SQLException  if a database access error occurs
     */
    public boolean ownInsertsAreVisible( int type ) throws SQLException
    {
        NotImplemented();
        return false;
    }


    /**
     *  JDBC 2.0 Indicates whether updates made by others are visible.
     *
     *@param  type              @todo Description of Parameter
     *@return                   <code>true</code> if updates made by others are
     *      visible for the result set type; <code>false</code> otherwise
     *@exception  SQLException  if a database access error occurs
     */
    public boolean othersUpdatesAreVisible( int type ) throws SQLException
    {
        NotImplemented();
        return false;
    }


    /**
     *  JDBC 2.0 Indicates whether deletes made by others are visible.
     *
     *@param  type              @todo Description of Parameter
     *@return                   <code>true</code> if deletes made by others are
     *      visible for the result set type; <code>false</code> otherwise
     *@exception  SQLException  if a database access error occurs
     */
    public boolean othersDeletesAreVisible( int type ) throws SQLException
    {
        NotImplemented();
        return false;
    }


    /**
     *  JDBC 2.0 Indicates whether inserts made by others are visible.
     *
     *@param  type              @todo Description of Parameter
     *@return                   true if updates are visible for the result set
     *      type
     *@return                   <code>true</code> if inserts made by others are
     *      visible for the result set type; <code>false</code> otherwise
     *@exception  SQLException  if a database access error occurs
     */
    public boolean othersInsertsAreVisible( int type ) throws SQLException
    {
        NotImplemented();
        return false;
    }


    /**
     *  JDBC 2.0 Indicates whether or not a visible row update can be detected
     *  by calling the method <code>ResultSet.rowUpdated</code> .
     *
     *@param  type              @todo Description of Parameter
     *@return                   <code>true</code> if changes are detected by the
     *      result set type; <code>false</code> otherwise
     *@exception  SQLException  if a database access error occurs
     */
    public boolean updatesAreDetected( int type ) throws SQLException
    {
        switch( type ) {

            case ResultSet.TYPE_SCROLL_SENSITIVE:
                return true;

            case ResultSet.TYPE_SCROLL_INSENSITIVE:
                return false;

            case ResultSet.TYPE_FORWARD_ONLY:
            default:
                return false;
        }
    }


    /**
     *  JDBC 2.0 Indicates whether or not a visible row delete can be detected
     *  by calling ResultSet.rowDeleted(). If deletesAreDetected() returns
     *  false, then deleted rows are removed from the result set.
     *
     *@param  type              @todo Description of Parameter
     *@return                   true if changes are detected by the resultset
     *      type
     *@exception  SQLException  if a database access error occurs
     */
    public boolean deletesAreDetected( int type ) throws SQLException
    {
        NotImplemented();
        return false;
    }


    /**
     *  JDBC 2.0 Indicates whether or not a visible row insert can be detected
     *  by calling ResultSet.rowInserted().
     *
     *@param  type              @todo Description of Parameter
     *@return                   true if changes are detected by the resultset
     *      type
     *@exception  SQLException  if a database access error occurs
     */
    public boolean insertsAreDetected( int type ) throws SQLException
    {
        NotImplemented();
        return false;
    }


    /**
     *  JDBC 2.0 Indicates whether the driver supports batch updates.
     *
     *@return                   true if the driver supports batch updates; false
     *      otherwise
     *@exception  SQLException  @todo Description of Exception
     */
    public boolean supportsBatchUpdates() throws SQLException
    {
        NotImplemented();
        return false;
    }


    /*
    private void debugPrintln( String s )
    {
        if ( verbose ) {
            System.out.println( s );
        }
    }


    private void debugPrint( String s )
    {
        if ( verbose ) {
            System.out.print( s );
        }
    }
     */


    protected void NotImplemented() throws SQLException
    {
        SQLException ex = new SQLException( "Not implemented" );
        ex.printStackTrace();
        throw ex;
    }


    public static void main( String args[] )
             throws java.lang.ClassNotFoundException,
            java.sql.SQLException,
            java.lang.IllegalAccessException,
            java.lang.InstantiationException
    {
        String url = "jdbc:freetds://kap/jdbctest";
        String user = "testuser";
        String password = "password";

        Class.forName( "com.internetcds.jdbc.tds.Driver" ).newInstance();
        java.sql.Connection cx = DriverManager.getConnection( url, user, password );
        java.sql.DatabaseMetaData m = cx.getMetaData();
        java.sql.ResultSet rs;

        System.out.println( "Connected to " + url + " as " + user );

        System.out.println( "url is " + m.getURL() );
        System.out.println( "username is " + m.getUserName() );

        System.out.println( m.getDriverName() );

        System.out.println( "Getting columns" );
//      rs = m.getColumns(null, "%", "%", "%");
rs = m.getColumns("webstats", "%", "%", "%");
        System.out.println( "Got columns" );
        while ( rs.next() ) {
            System.out.println(
                    "TABLE_CAT:         " + rs.getString( "TABLE_CAT" ) + "\n" +
                    "TABLE_SCHEM:       " + rs.getString( "TABLE_SCHEM" ) + "\n" +
                    "TABLE_NAME:        " + rs.getString( "TABLE_NAME" ) + "\n" +
                    "COLUMN_NAME:       " + rs.getString( "COLUMN_NAME" ) + "\n" +
                    "DATA_TYPE:         " + rs.getString( "DATA_TYPE" ) + "\n" +
                    "TYPE_NAME:         " + rs.getString( "TYPE_NAME" ) + "\n" +
                    "COLUMN_SIZE:       " + rs.getString( "COLUMN_SIZE" ) + "\n" +
                    "BUFFER_LENGTH:     " + rs.getString( "BUFFER_LENGTH" ) + "\n" +
                    "DECIMAL_DIGITS:    " + rs.getString( "DECIMAL_DIGITS" ) + "\n" +
                    "NUM_PREC_RADIX:    " + rs.getString( "NUM_PREC_RADIX" ) + "\n" +
                    "NULLABLE:          " + rs.getString( "NULLABLE" ) + "\n" +
                    "REMARKS:           " + rs.getString( "REMARKS" ) + "\n" +
                    "COLUMN_DEF:        " + rs.getString( "COLUMN_DEF" ) + "\n" +
                    "SQL_DATA_TYPE:     " + rs.getString( "SQL_DATA_TYPE" ) + "\n" +
                    "SQL_DATETIME_SUB:  " + rs.getString( "SQL_DATETIME_SUB" ) + "\n" +
                    "CHAR_OCTET_LENGTH: " + rs.getString( "CHAR_OCTET_LENGTH" ) + "\n" +
                    "ORDINAL_POSITION:  " + rs.getString( "ORDINAL_POSITION" ) + "\n" +
                    "IS_NULLABLE:       " + rs.getString( "IS_NULLABLE" ) + "\n" +
                    "\n" );
        }
        System.out.println( "\n" );
        rs.close();

        System.out.println( "Catalog term- " + m.getCatalogTerm() );
        System.out.println( "Catalog separator- " + m.getCatalogSeparator() );
        System.out.println( "Catalog is "
                 + ( m.isCatalogAtStart() ? "" : "not " )
                 + "at start" );
        System.out.println( "Catalogs-" );
        rs = m.getCatalogs();
        while ( rs.next() ) {
            System.out.println( "  " + rs.getString( 1 ) );
        }
        System.out.println( "\n" );
        rs.close();

        System.out.println( "Schema term- " + m.getSchemaTerm() );
        rs = m.getSchemas();
        while ( rs.next() ) {
            System.out.println( "  " + rs.getString( 1 ) );
        }
        System.out.println( "\n" );
        rs.close();

        System.out.println( "Table types-" );
        rs = m.getTableTypes();
        while ( rs.next() ) {
            System.out.println( "  " + rs.getString( 1 ) );
        }
        System.out.println( "\n" );
        rs.close();

        System.out.println( "Tables- " );
        rs = m.getTables( null, "%", "%", null );
        while ( rs.next() ) {
            System.out.println( "  " +
                    rs.getString( 1 ) +
                    "." +
                    rs.getString( 2 ) +
                    "." +
                    rs.getString( 3 ) +
                    "." +
                    rs.getString( 4 ) +
                    "." +
                    rs.getString( 5 ) +
                    "" );
        }
        System.out.println( "\n" );
        rs.close();

        System.out.println( "Tables for pubs- " );
        String tables[] = {"SYSTEM TABLE", "VIEW"};
        rs = m.getTables( "pubs", "%", "%", tables );
        while ( rs.next() ) {
            System.out.println( "  " +
                    rs.getString( 1 ) +
                    "." +
                    rs.getString( 2 ) +
                    "." +
                    rs.getString( 3 ) +
                    "." +
                    rs.getString( 4 ) +
                    "." +
                    rs.getString( 5 ) +
                    "" );
        }
        System.out.println( "\n" );
        rs.close();

        System.out.println( "Columns- " );
        rs = m.getColumns( null, "%", "%", "%" );
        while ( rs.next() ) {
            System.out.println(
                    "TABLE_CAT:         " + rs.getString( "TABLE_CAT" ) + "\n" +
                    "TABLE_SCHEM:       " + rs.getString( "TABLE_SCHEM" ) + "\n" +
                    "TABLE_NAME:        " + rs.getString( "TABLE_NAME" ) + "\n" +
                    "COLUMN_NAME:       " + rs.getString( "COLUMN_NAME" ) + "\n" +
                    "DATA_TYPE:         " + rs.getString( "DATA_TYPE" ) + "\n" +
                    "TYPE_NAME:         " + rs.getString( "TYPE_NAME" ) + "\n" +
                    "COLUMN_SIZE:       " + rs.getString( "COLUMN_SIZE" ) + "\n" +
                    "BUFFER_LENGTH:     " + rs.getString( "BUFFER_LENGTH" ) + "\n" +
                    "DECIMAL_DIGITS:    " + rs.getString( "DECIMAL_DIGITS" ) + "\n" +
                    "NUM_PREC_RADIX:    " + rs.getString( "NUM_PREC_RADIX" ) + "\n" +
                    "NULLABLE:          " + rs.getString( "NULLABLE" ) + "\n" +
                    "REMARKS:           " + rs.getString( "REMARKS" ) + "\n" +
                    "COLUMN_DEF:        " + rs.getString( "COLUMN_DEF" ) + "\n" +
                    "SQL_DATA_TYPE:     " + rs.getString( "SQL_DATA_TYPE" ) + "\n" +
                    "SQL_DATETIME_SUB:  " + rs.getString( "SQL_DATETIME_SUB" ) + "\n" +
                    "CHAR_OCTET_LENGTH: " + rs.getString( "CHAR_OCTET_LENGTH" ) + "\n" +
                    "ORDINAL_POSITION:  " + rs.getString( "ORDINAL_POSITION" ) + "\n" +
                    "IS_NULLABLE:       " + rs.getString( "IS_NULLABLE" ) + "\n" +
                    "\n" );
        }
        System.out.println( "\n" );
        rs.close();

        System.out.println( "Done" );
    }
}
