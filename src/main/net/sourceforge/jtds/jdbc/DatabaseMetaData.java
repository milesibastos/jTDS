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

/**
 * @author   Craig Spannring
 * @author   The FreeTDS project
 * @author   Alin Sinpalean
 * @created  17 March 2001
 * @version  $Id: DatabaseMetaData.java,v 1.24 2004-05-07 04:38:07 bheineman Exp $
 */
public class DatabaseMetaData implements java.sql.DatabaseMetaData
{
    /**
     * CVS version of the file.
     */
    public final static String cvsVersion = "$Id: DatabaseMetaData.java,v 1.24 2004-05-07 04:38:07 bheineman Exp $";

    // internal data needed by this implemention.
    Tds tds;
    TdsConnection connection;

    /**
     * Length of a sysname object (table name, catalog name etc.) -- 128 for
     * TDS 7.0, 30 for earlier versions.
     */
    int sysnameLength = 30;

    /**
     * <code>Boolean.TRUE</code> if identifiers are case sensitive (the server
     * was installed that way). Initially <code>null</code>, set the first time
     * any of the methods that check this are called.
     */
    Boolean caseSensitive = null;

    public DatabaseMetaData(TdsConnection connection_, Tds tds_) throws SQLException
    {
        connection = connection_;
        tds = tds_;
    }

    public static DatabaseMetaData getInstance(TdsConnection connection_, Tds tds_)
        throws SQLException
    {
        // Return a Microsoft7MetaData only if we're using TDS 7.0 or later.
        // (The limitations of the TDS version not the server version apply.)
        if( tds_.getTdsVer() >= Tds.TDS70 )
            return new net.sourceforge.jtds.jdbc.Microsoft7MetaData(connection_, tds_);

        return new net.sourceforge.jtds.jdbc.DatabaseMetaData(connection_, tds_);
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
        return true;
    }

    /**
     * Does a data definition statement within a transaction force the
     * transaction to commit?
     *
     * @return true if so
     * @exception SQLException if a database-access error occurs.
     */
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException
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
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException
    {
        // XXX needs to be checked for Sybase
        return false;
    }

    /**
     * Did getMaxRowSize() include LONGVARCHAR and LONGVARBINARY blobs?
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
        String query = "exec sp_special_columns ?, ?, ?, ?, ?, ?, ?";
        if( catalog != null )
        {
            if( tds.getTdsVer() == Tds.TDS70 )
                query = "exec ["+catalog+"]..sp_special_columns ?, ?, ?, ?, ?, ?, ?";
            else
                query = "exec "+catalog+"..sp_special_columns ?, ?, ?, ?, ?, ?, ?";
        }

        CallableStatement s = connection.prepareCall(query);
        s.setString(1, table);
        s.setString(2, schema);
        s.setString(3, catalog);
        s.setString(4, "R");
        s.setString(5, "T");
        s.setString(6, "U");
        s.setInt(7, 3); // ODBC version 3

        TdsResultSet rs = (TdsResultSet)s.executeQuery();
        Columns col = rs.getContext().getColumnInfo();

        col.setName(5, "COLUMN_SIZE");
        col.setLabel(5, "COLUMN_SIZE");
        col.setName(6, "BUFFER_LENGTH");
        col.setLabel(6, "BUFFER_LENGTH");
        col.setName(7, "DECIMAL_DIGITS");
        col.setLabel(7, "DECIMAL_DIGITS");

        return rs;
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
        String query = "exec sp_tables '', '', '%', NULL";
        Statement s = connection.createStatement();
        TdsResultSet rs = (TdsResultSet)s.executeQuery(query);
        Columns col = rs.getContext().getColumnInfo();

        col.setFakeColumnCount(1);
        col.setName(1, "TABLE_CAT");
        col.setLabel(1, "TABLE_CAT");

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
        String query = "exec sp_column_privileges ?, ?, ?, ?";
        if( catalog != null )
        {
            if( tds.getTdsVer() == Tds.TDS70 )
                query = "exec ["+catalog+"]..sp_column_privileges ?, ?, ?, ?";
            else
                query = "exec "+catalog+"..sp_column_privileges ?, ?, ?, ?";
        }

        CallableStatement s = connection.prepareCall(query);
        s.setString(1, table);
        s.setString(2, schema);
        s.setString(3, catalog);
        s.setString(4, columnNamePattern);

        TdsResultSet rs = (TdsResultSet)s.executeQuery();
        Columns col = rs.getContext().getColumnInfo();

        col.setName(1, "TABLE_CAT");
        col.setLabel(1, "TABLE_CAT");
        col.setName(2, "TABLE_SCHEM");
        col.setLabel(2, "TABLE_SCHEM");

        return rs;
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
    public java.sql.ResultSet getColumns( String catalog, String schemaPattern, String tableNamePattern,
        String columnNamePattern ) throws SQLException
    {
        String query = "exec sp_columns ?, ?, ?, ?, ?";
        if( catalog != null )
        {
            if( tds.getTdsVer() == Tds.TDS70 )
                query = "exec ["+catalog+"]..sp_columns ?, ?, ?, ?, ?";
            else
                query = "exec "+catalog+"..sp_columns ?, ?, ?, ?, ?";
        }

        CallableStatement s = connection.prepareCall(query);
        s.setString(1, tableNamePattern);
        s.setString(2, schemaPattern);
        s.setString(3, catalog);
        s.setString(4, columnNamePattern);
        s.setInt(5, 3); // ODBC version 3

        TdsResultSet rs = (TdsResultSet)s.executeQuery();
        Columns col = rs.getContext().getColumnInfo();

        col.setName(1, "TABLE_CAT");
        col.setLabel(1, "TABLE_CAT");
        col.setName(2, "TABLE_SCHEM");
        col.setLabel(2, "TABLE_SCHEM");
        col.setName(7, "COLUMN_SIZE");
        col.setLabel(7, "COLUMN_SIZE");
        col.setName(8, "BUFFER_LENGTH");
        col.setLabel(8, "BUFFER_LENGTH");
        col.setName(9, "DECIMAL_DIGITS");
        col.setLabel(9, "DECIMAL_DIGITS");
        col.setName(10, "NUM_PREC_RADIX");
        col.setLabel(10, "NUM_PREC_RADIX");
        col.setFakeColumnCount(18);

        return rs;
    }

    /**
     *  Get a description of the foreign key columns in the foreign key table
     *  that reference the primary key columns of the primary key table
     *  (describe how one table imports another's key). This should normally
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
        String query = "exec sp_fkeys ?, ?, ?, ?, ?, ?";
        if( primaryCatalog != null )
        {
            if( tds.getTdsVer() == Tds.TDS70 )
                query = "exec ["+primaryCatalog+"]..sp_fkeys ?, ?, ?, ?, ?, ?";
            else
                query = "exec "+primaryCatalog+"..sp_fkeys ?, ?, ?, ?, ?, ?";
        }
        else if( foreignCatalog != null )
        {
            if( tds.getTdsVer() == Tds.TDS70 )
                query = "exec ["+foreignCatalog+"]..sp_fkeys ?, ?, ?, ?, ?, ?";
            else
                query = "exec "+foreignCatalog+"..sp_fkeys ?, ?, ?, ?, ?, ?";
        }

        CallableStatement s = connection.prepareCall(query);
        s.setString(1, primaryTable);
        s.setString(2, primarySchema);
        s.setString(3, primaryCatalog);
        s.setString(4, foreignTable);
        s.setString(5, foreignSchema);
        s.setString(6, foreignCatalog);

        TdsResultSet rs = (TdsResultSet)s.executeQuery();
        Columns col = rs.getContext().getColumnInfo();

        col.setName(1, "PKTABLE_CAT");
        col.setLabel(1, "PKTABLE_CAT");
        col.setName(2, "PKTABLE_SCHEM");
        col.setLabel(2, "PKTABLE_SCHEM");
        col.setName(5, "FKTABLE_CAT");
        col.setLabel(5, "FKTABLE_CAT");
        col.setName(6, "FKTABLE_SCHEM");
        col.setLabel(6, "FKTABLE_SCHEM");

        return rs;
    }

    /**
     * Returns the name of this database product.
     *
     * @return                   database product name
     * @exception  SQLException  if a database-access error occurs.
     */
    public String getDatabaseProductName() throws SQLException
    {
        return connection.getDatabaseProductName();
    }

    /**
     * Returns the version of this database product.
     *
     * @return                   database version
     * @exception  SQLException  if a database-access error occurs.
     */
    public String getDatabaseProductVersion() throws SQLException
    {
        return connection.getDatabaseProductVersion();
    }

    //----------------------------------------------------------------------

    /**
     * Returns the database's default transaction isolation level. The values
     * are defined in java.sql.Connection.
     *
     * @return                   the default isolation level
     * @exception  SQLException  if a database-access error occurs.
     * @see                      Connection
     */
    public int getDefaultTransactionIsolation() throws SQLException {
        // XXX need to check this for Sybase
        return Connection.TRANSACTION_READ_COMMITTED;
    }

    /**
     * Returns this JDBC driver's major version number.
     *
     * @return    JDBC driver major version
     */
    public int getDriverMajorVersion() {
        return Driver.MAJOR_VERSION;
    }

    /**
     * Returns this JDBC driver's minor version number.
     *
     * @return    JDBC driver minor version number
     */
    public int getDriverMinorVersion() {
        return Driver.MINOR_VERSION;
    }

    /**
     * Returns the name of this JDBC driver.
     *
     * @return                   JDBC driver name
     * @exception  SQLException  if a database-access error occurs.
     */
    public String getDriverName() throws SQLException {
        return "jTDS Type 4 JDBC Driver for MS SQL Server";
    }

    /**
     * Returns the version of this JDBC driver.
     *
     * @return                   JDBC driver version
     * @exception  SQLException  if a database-access error occurs.
     */
    public String getDriverVersion() throws SQLException {
        return getDriverMajorVersion() + "." + getDriverMinorVersion();
    }

    /**
     * Get a description of the foreign key columns that reference a table's
     * primary key columns (the foreign keys exported by a table). They are
     * ordered by FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME, and KEY_SEQ.
     * <p>
     * Each foreign key column description has the following columns:
     * <OL>
     *   <LI> <B>PKTABLE_CAT</B> String =>primary key table catalog (may be
     *   null)
     *   <LI> <B>PKTABLE_SCHEM</B> String =>primary key table schema (may be
     *   null)
     *   <LI> <B>PKTABLE_NAME</B> String =>primary key table name
     *   <LI> <B>PKCOLUMN_NAME</B> String =>primary key column name
     *   <LI> <B>FKTABLE_CAT</B> String =>foreign key table catalog (may be
     *   null) being exported (may be null)
     *   <LI> <B>FKTABLE_SCHEM</B> String =>foreign key table schema (may be
     *   null) being exported (may be null)
     *   <LI> <B>FKTABLE_NAME</B> String =>foreign key table name being
     *   exported
     *   <LI> <B>FKCOLUMN_NAME</B> String =>foreign key column name being
     *   exported
     *   <LI> <B>KEY_SEQ</B> short =>sequence number within foreign key
     *   <LI> <B>UPDATE_RULE</B> short =>What happens to foreign key when
     *   primary is updated:
     *   <UL>
     *     <LI> importedNoAction - do not allow update of primary key if it has
     *     been imported
     *     <LI> importedKeyCascade - change imported key to agree with primary
     *     key update
     *     <LI> importedKeySetNull - change imported key to NULL if its primary
     *     key has been updated
     *     <LI> importedKeySetDefault - change imported key to default values
     *     if its primary key has been updated
     *     <LI> importedKeyRestrict - same as importedKeyNoAction (for ODBC 2.x
     *     compatibility)
     *   </UL>
     *
     *   <LI> <B>DELETE_RULE</B> short =>What happens to the foreign key when
     *   primary is deleted.
     *   <UL>
     *     <LI> importedKeyNoAction - do not allow delete of primary key if it
     *     has been imported
     *     <LI> importedKeyCascade - delete rows that import a deleted key
     *     <LI> importedKeySetNull - change imported key to NULL if its primary
     *     key has been deleted
     *     <LI> importedKeyRestrict - same as importedKeyNoAction (for ODBC 2.x
     *     compatibility)
     *     <LI> importedKeySetDefault - change imported key to default if its
     *     primary key has been deleted
     *   </UL>
     *
     *   <LI> <B>FK_NAME</B> String =>foreign key name (may be null)
     *   <LI> <B>PK_NAME</B> String =>primary key name (may be null)
     *   <LI> <B>DEFERRABILITY</B> short =>can the evaluation of foreign key
     *   constraints be deferred until commit
     *   <UL>
     *     <LI> importedKeyInitiallyDeferred - see SQL92 for definition
     *     <LI> importedKeyInitiallyImmediate - see SQL92 for definition
     *     <LI> importedKeyNotDeferrable - see SQL92 for definition
     *   </UL>
     *
     * </OL>
     *
     *
     * @param  catalog           a catalog name; "" retrieves those without a
     *      catalog; null means drop catalog name from the selection criteria
     * @param  schema            a schema name pattern; "" retrieves those
     *      without a schema
     * @param  table             a table name
     * @return                   ResultSet - each row is a foreign key column
     *      description
     * @exception  SQLException  if a database-access error occurs.
     * @see                      #getImportedKeys
     */
    public java.sql.ResultSet getExportedKeys(
        String catalog,
        String schema,
        String table)
        throws SQLException
    {
        return getCrossReference(catalog, schema, table, null, null, null);
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
        // @todo Maybe add the extended set characters, too, to this list
        return "#$";
    }

    /**
     * Returns the string used to quote SQL identifiers. This returns a space "
     * " if identifier quoting isn't supported. A JDBC-Compliant driver always
     * uses a double quote character.
     *
     *@return                   the quoting string
     *@exception  SQLException  if a database-access error occurs.
     */
    public String getIdentifierQuoteString() throws SQLException
    {
        return "\"";
    }

    /**
     * Get a description of the primary key columns that are referenced by a
     * table's foreign key columns (the primary keys imported by a table). They
     * are ordered by PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, and KEY_SEQ.
     * <p>
     * Each primary key column description has the following columns:
     * <OL>
     *   <LI> <B>PKTABLE_CAT</B> String =>primary key table catalog being
     *   imported (may be null)
     *   <LI> <B>PKTABLE_SCHEM</B> String =>primary key table schema being
     *   imported (may be null)
     *   <LI> <B>PKTABLE_NAME</B> String =>primary key table name being
     *   imported
     *   <LI> <B>PKCOLUMN_NAME</B> String =>primary key column name being
     *   imported
     *   <LI> <B>FKTABLE_CAT</B> String =>foreign key table catalog (may be
     *   null)
     *   <LI> <B>FKTABLE_SCHEM</B> String =>foreign key table schema (may be
     *   null)
     *   <LI> <B>FKTABLE_NAME</B> String =>foreign key table name
     *   <LI> <B>FKCOLUMN_NAME</B> String =>foreign key column name
     *   <LI> <B>KEY_SEQ</B> short =>sequence number within foreign key
     *   <LI> <B>UPDATE_RULE</B> short =>What happens to foreign key when
     *   primary is updated:
     *   <UL>
     *     <LI> importedNoAction - do not allow update of primary key if it has
     *     been imported
     *     <LI> importedKeyCascade - change imported key to agree with primary
     *     key update
     *     <LI> importedKeySetNull - change imported key to NULL if its primary
     *     key has been updated
     *     <LI> importedKeySetDefault - change imported key to default values
     *     if its primary key has been updated
     *     <LI> importedKeyRestrict - same as importedKeyNoAction (for ODBC 2.x
     *     compatibility)
     *   </UL>
     *
     *   <LI> <B>DELETE_RULE</B> short =>What happens to the foreign key when
     *   primary is deleted.
     *   <UL>
     *     <LI> importedKeyNoAction - do not allow delete of primary key if it
     *     has been imported
     *     <LI> importedKeyCascade - delete rows that import a deleted key
     *     <LI> importedKeySetNull - change imported key to NULL if its primary
     *     key has been deleted
     *     <LI> importedKeyRestrict - same as importedKeyNoAction (for ODBC 2.x
     *     compatibility)
     *     <LI> importedKeySetDefault - change imported key to default if its
     *     primary key has been deleted
     *   </UL>
     *
     *   <LI> <B>FK_NAME</B> String =>foreign key name (may be null)
     *   <LI> <B>PK_NAME</B> String =>primary key name (may be null)
     *   <LI> <B>DEFERRABILITY</B> short =>can the evaluation of foreign key
     *   constraints be deferred until commit
     *   <UL>
     *     <LI> importedKeyInitiallyDeferred - see SQL92 for definition
     *     <LI> importedKeyInitiallyImmediate - see SQL92 for definition
     *     <LI> importedKeyNotDeferrable - see SQL92 for definition
     *   </UL>
     *
     * </OL>
     *
     *
     * @param  catalog           a catalog name; "" retrieves those without a
     *      catalog; null means drop catalog name from the selection criteria
     * @param  schema            a schema name pattern; "" retrieves those
     *      without a schema
     * @param  table             a table name
     * @return                   ResultSet - each row is a primary key column
     *      description
     * @exception  SQLException  if a database-access error occurs.
     * @see                      #getExportedKeys
     */
    public java.sql.ResultSet getImportedKeys(
        String catalog,
        String schema,
        String table)
        throws SQLException
    {
        return getCrossReference(null, null, null, catalog, schema, table);
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
        String query = "exec sp_statistics ?, ?, ?, ?, ?, ?";
        if( catalog != null )
        {
            if( tds.getTdsVer() == Tds.TDS70 )
                query = "exec ["+catalog+"]..sp_statistics ?, ?, ?, ?, ?, ?";
            else
                query = "exec "+catalog+"..sp_statistics ?, ?, ?, ?, ?, ?";
        }

        CallableStatement s = connection.prepareCall(query);
        s.setString(1, table);
        s.setString(2, schema);
        s.setString(3, catalog);
        s.setString(4, "%");
        s.setString(5, unique ? "Y" : "N");
        s.setString(6, approximate ? "Q" : "E");

        TdsResultSet rs = (TdsResultSet)s.executeQuery();
        Columns col = rs.getContext().getColumnInfo();

        col.setName(1, "TABLE_CAT");
        col.setLabel(1, "TABLE_CAT");
        col.setName(2, "TABLE_SCHEM");
        col.setLabel(2, "TABLE_SCHEM");
        col.setName(8, "ORDINAL_POSITION");
        col.setLabel(8, "ORDINAL_POSITION");
        col.setName(10, "ASC_OR_DESC");
        col.setLabel(10, "ASC_OR_DESC");

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
        return sysnameLength;
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
        return sysnameLength;
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
        return 0;
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
        // @todo Implement (a)%(b) for MOD(a,b)
        // XXX need to find out if this is still true for SYBASE
        return "ABS,ACOS,ASIN,ATAN,ATAN2,CEILING,COS,COT,DEGREES,EXP,FLOOR,LOG,LOG10,PI,POWER,RADIANS,RAND,ROUND,SIGN,SIN,SQRT,TAN";
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
        String query = "exec sp_pkeys ?, ?, ?";
        if( catalog != null )
        {
            if( tds.getTdsVer() == Tds.TDS70 )
                query = "exec ["+catalog+"]..sp_pkeys ?, ?, ?";
            else
                query = "exec "+catalog+"..sp_pkeys ?, ?, ?";
        }

        CallableStatement s = connection.prepareCall(query);
        s.setString(1, table);
        s.setString(2, schema);
        s.setString(3, catalog);

        TdsResultSet rs = (TdsResultSet)s.executeQuery();
        Columns col = rs.getContext().getColumnInfo();

        col.setName(1, "TABLE_CAT");
        col.setLabel(1, "TABLE_CAT");
        col.setName(2, "TABLE_SCHEM");
        col.setLabel(2, "TABLE_SCHEM");

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
        String query = "exec sp_sproc_columns ?, ?, ?, ?, ?";
        if( catalog != null )
        {
            if( tds.getTdsVer() == Tds.TDS70 )
                query = "exec ["+catalog+"]..sp_sproc_columns ?, ?, ?, ?, ?";
            else
                query = "exec "+catalog+"..sp_sproc_columns ?, ?, ?, ?, ?";
        }

        CallableStatement s = connection.prepareCall(query);
        s.setString(1, procedureNamePattern);
        s.setString(2, schemaPattern);
        s.setString(3, catalog);
        s.setString(4, columnNamePattern);
        s.setInt(5, 3); // ODBC version 3

        TdsResultSet rs = (TdsResultSet)s.executeQuery();
        Columns col = rs.getContext().getColumnInfo();

        col.setName(1, "PROCEDURE_CAT");
        col.setLabel(1, "PROCEDURE_CAT");
        col.setName(2, "PROCEDURE_SCHEM");
        col.setLabel(2, "PROCEDURE_SCHEM");

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
        String query = "exec sp_stored_procedures ?, ?, ?";
        if( catalog != null )
        {
            if( tds.getTdsVer() == Tds.TDS70 )
                query = "exec ["+catalog+"]..sp_stored_procedures ?, ?, ?";
            else
                query = "exec "+catalog+"..sp_stored_procedures ?, ?, ?";
        }

        CallableStatement s = connection.prepareCall(query);
        s.setString(1, procedureNamePattern);
        s.setString(2, schemaPattern);
        s.setString(3, catalog);

        TdsResultSet rs = (TdsResultSet)s.executeQuery();
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
        java.sql.Statement statement = connection.createStatement();

        String sql;
        //
        // MJH - isLogin column only in MSSQL >= 7.0
        //
        if( this.tds.getTdsVer() >= Tds.TDS70 )
            sql = "SELECT name AS TABLE_SCHEM FROM dbo.sysusers WHERE islogin=1";
        else
            sql = "SELECT name AS TABLE_SCHEM FROM dbo.sysusers WHERE uid>0";

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
     * Get a comma separated list of all a database's SQL keywords that are NOT
     * also SQL92 keywords.
     *
     * @return the list
     * @exception SQLException  if a database-access error occurs.
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
     * Get a comma separated list of string functions.
     *
     * @return the list
     * @exception SQLException  if a database-access error occurs.
     */
    public String getStringFunctions() throws SQLException
    {
        return "ASCII,CHAR,CONCAT,DIFFERENCE,INSERT,LCASE,LEFT,LENGTH,LOCATE,LTRIM,REPEAT,REPLACE,RIGHT,RTRIM,SOUNDEX,SPACE,SUBSTRING,UCASE";
    }

    /**
     * Get a comma separated list of system functions.
     *
     * @return the list
     * @exception  SQLException  if a database-access error occurs.
     */
    public String getSystemFunctions() throws SQLException
    {
        return "DATABASE,IFNULL,USER";
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
        String query = "exec sp_table_privileges ?, ?, ?";
        if( catalog != null )
        {
            if( tds.getTdsVer() == Tds.TDS70 )
                query = "exec ["+catalog+"]..sp_table_privileges ?, ?, ?";
            else
                query = "exec "+catalog+"..sp_table_privileges ?, ?, ?";
        }

        CallableStatement s = connection.prepareCall(query);
        s.setString(1, tableNamePattern);
        s.setString(2, schemaPattern);
        s.setString(3, catalog);

        TdsResultSet rs = (TdsResultSet)s.executeQuery();
        Columns col = rs.getContext().getColumnInfo();

        col.setName(1, "TABLE_CAT");
        col.setLabel(1, "TABLE_CAT");
        col.setName(2, "TABLE_SCHEM");
        col.setLabel(2, "TABLE_SCHEM");

        return rs;
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
        String query = "exec sp_tables ?, ?, ?, ?";
        if( catalog != null )
        {
            if( tds.getTdsVer() == Tds.TDS70 )
                query = "exec ["+catalog+"]..sp_tables ?, ?, ?, ?";
            else
                query = "exec "+catalog+"..sp_tables ?, ?, ?, ?";
        }

        CallableStatement s = connection.prepareCall(query);
        s.setString(1, tableNamePattern);
        s.setString(2, schemaPattern);
        s.setString(3, catalog);
        if( types == null )
            s.setString(4, null);
        else
        {
            StringBuffer buf = new StringBuffer(64);
            buf.append('"');
            for( int i=0; i<types.length; i++ )
                buf.append('\'').append(types[i]).append("',");
            if( buf.length() > 1 )
                buf.setLength(buf.length()-1);
            buf.append('"');
            s.setString(4, buf.toString());
        }

        TdsResultSet rs = (TdsResultSet)s.executeQuery();
        Columns col = rs.getContext().getColumnInfo();

        col.setName(1, "TABLE_CAT");
        col.setLabel(1, "TABLE_CAT");
        col.setName(2, "TABLE_SCHEM");
        col.setLabel(2, "TABLE_SCHEM");

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
        // @todo Implement this method correctly (and add dependencies to EscapeProcessor)!
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
        Statement s = connection.createStatement();
        TdsResultSet rs = (TdsResultSet)s.executeQuery(
            "exec sp_datatype_info @ODBCVer=3");

        Columns col = rs.getContext().getColumnInfo();
        col.setFakeColumnCount(18);
        col.setName(11, "FIXED_PREC_SCALE");
        col.setLabel(11, "FIXED_PREC_SCALE");

        return rs;
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
        String colNames[] = {"TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "CLASS_NAME", "DATA_TYPE", "REMARKS"};
        Columns cols = new Columns(6);
        final Context ctx = new Context(cols, null);

        for( int i=0; i<6; i++ )
        {
            cols.setNativeType(i+1, Tds.SYBVARCHAR);
            cols.setLabel(i+1, colNames[i]);
            cols.setName(i+1, colNames[i]);
        }

        return new AbstractResultSet()
        {
            public SQLWarning getWarnings() throws SQLException{ return null;}
            public String getCursorName() throws SQLException{ return null;}
            public boolean isBeforeFirst() throws SQLException{ return false; }
            public boolean isFirst() throws SQLException{ return false; }
            public boolean first() throws SQLException{ return false; }
            public boolean last() throws SQLException{ return false; }
            public boolean absolute(int row) throws SQLException{ return false; }
            public boolean relative(int rows) throws SQLException{ return false; }
            public boolean previous() throws SQLException{ return false; }
            public void setFetchDirection(int direction) throws SQLException {}
            public int getFetchDirection() throws SQLException{ return FETCH_FORWARD; }
            public int getFetchSize() throws SQLException{ return 0; }
            public int getType() throws SQLException{ return TYPE_FORWARD_ONLY; }
            public boolean rowUpdated() throws SQLException{ return false; }
            public boolean rowInserted() throws SQLException{ return false; }
            public boolean rowDeleted() throws SQLException{ return false; }
            public void insertRow() throws SQLException {}
            public void updateRow() throws SQLException {}
            public void deleteRow() throws SQLException {}
            public void refreshRow() throws SQLException {}
            public void cancelRowUpdates() throws SQLException {}
            public void moveToInsertRow() throws SQLException {}
            public void moveToCurrentRow() throws SQLException {}
            public Statement getStatement() throws SQLException{ return null; }
            public boolean isLast() throws SQLException{ return false; }
            public boolean isAfterLast() throws SQLException{ return false; }
            public void setFetchSize(int rows) throws SQLException {}
            public void clearWarnings() throws SQLException {}
            public void beforeFirst() throws SQLException {}
            public int getRow() throws SQLException{ return 0; }
            public void close() throws SQLException {}
            public void afterLast() {}
            public boolean next() throws SQLException{ return false; }
            public int getConcurrency(){ return TYPE_FORWARD_ONLY; }
            public Context getContext(){ return ctx; }
            public PacketRowResult currentRow() throws SQLException{ return null; }
        };
    }

    /**
     *  What's the url for this database?
     *
     *@return                   the url or null if it can't be generated
     *@exception  SQLException  if a database-access error occurs.
     */
    public String getURL() throws SQLException
    {
        return connection.getUrl();
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

            // MJH Sybase does not support system_user
            if( tds.getServerType() == Tds.SYBASE )
                rs = s.executeQuery("select suser_name()");
            else
                rs = s.executeQuery("select system_user");

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
        String query = "exec sp_special_columns ?, ?, ?, ?, ?, ?, ?";
        if( catalog != null )
        {
            if( tds.getTdsVer() == Tds.TDS70 )
                query = "exec ["+catalog+"]..sp_special_columns ?, ?, ?, ?, ?, ?, ?";
            else
                query = "exec "+catalog+"..sp_special_columns ?, ?, ?, ?, ?, ?, ?";
        }

        CallableStatement s = connection.prepareCall(query);
        s.setString(1, table);
        s.setString(2, schema);
        s.setString(3, catalog);
        s.setString(4, "V");
        s.setString(5, "C");
        s.setString(6, "O");
        s.setInt(7, 3); // ODBC version 3

        TdsResultSet rs = (TdsResultSet)s.executeQuery();
        Columns col = rs.getContext().getColumnInfo();

        col.setName(5, "COLUMN_SIZE");
        col.setLabel(5, "COLUMN_SIZE");
        col.setName(6, "BUFFER_LENGTH");
        col.setLabel(6, "BUFFER_LENGTH");
        col.setName(7, "DECIMAL_DIGITS");
        col.setLabel(7, "DECIMAL_DIGITS");

        return rs;
    }

    /**
     * Retrieves whether a catalog appears at the start of a fully qualified
     * table name.  If not, the catalog appears at the end.
     *
     * @return                   true if it appears at the start
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean isCatalogAtStart() throws SQLException
    {
        return true;
    }

    /**
     * Is the database in read-only mode?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean isReadOnly() throws SQLException
    {
        return false;
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
     * Retrieves whether this database supports concatenations between
     * <code>NULL</code> and non-<code>NULL</code> values being
     * <code>NULL</code>.
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
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
     * Are NULL values sorted at the end regardless of sort order?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean nullsAreSortedAtEnd() throws SQLException
    {
        // XXX Need to check for Sybase
        return false;
    }

    /**
     * Are NULL values sorted at the start regardless of sort order?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean nullsAreSortedAtStart() throws SQLException
    {
        // XXX Need to check for Sybase
        return false;
    }

    /**
     * Are NULL values sorted high?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean nullsAreSortedHigh() throws SQLException
    {
        // XXX Need to check for Sybase
        return false;
    }

    /**
     * Are NULL values sorted low?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean nullsAreSortedLow() throws SQLException
    {
        // XXX Need to check for Sybase
        return true;
    }

    /**
     * Does the database treat mixed case unquoted SQL identifiers as case
     * insensitive and store them in lower case?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean storesLowerCaseIdentifiers() throws SQLException
    {
        // XXX Need to check for Sybase
        return false;
    }

    /**
     * Does the database treat mixed case quoted SQL identifiers as case
     * insensitive and store them in lower case?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException
    {
        // XXX Need to check for Sybase
        return false;
    }

    /**
     * Does the database treat mixed case unquoted SQL identifiers as case
     * insensitive and store them in mixed case?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean storesMixedCaseIdentifiers() throws SQLException
    {
        setCaseSensitiveFlag();
        return !caseSensitive.booleanValue();
    }

    /**
     * Does the database treat mixed case quoted SQL identifiers as case
     * insensitive and store them in mixed case?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException
    {
        setCaseSensitiveFlag();
        return !caseSensitive.booleanValue();
    }

    /**
     * Does the database treat mixed case unquoted SQL identifiers as case
     * insensitive and store them in upper case?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean storesUpperCaseIdentifiers() throws SQLException
    {
        // XXX Need to check for Sybase
        return false;
    }

    /**
     * Does the database treat mixed case quoted SQL identifiers as case
     * insensitive and store them in upper case?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException
    {
        // XXX Need to check for Sybase
        return false;
    }

    //--------------------------------------------------------------------
    // Functions describing which features are supported.

    /**
     * Is "ALTER TABLE" with add column supported?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsAlterTableWithAddColumn() throws SQLException
    {
        return true;
    }

    /**
     * Is "ALTER TABLE" with drop column supported?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsAlterTableWithDropColumn() throws SQLException
    {
        return true;
    }

    /**
     * Retrieves whether this database supports the ANSI92 entry level SQL
     * grammar.
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsANSI92EntryLevelSQL() throws SQLException
    {
        // XXX Will have to check for Sybase
        return true;
    }

    /**
     * Is the ANSI92 full SQL grammar supported?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsANSI92FullSQL() throws SQLException
    {
        // XXX Will have to check for Sybase
        return false;
    }

    /**
     * Is the ANSI92 intermediate SQL grammar supported?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsANSI92IntermediateSQL() throws SQLException
    {
        // XXX Will have to check for Sybase
        return false;
    }

    /**
     * Can a catalog name be used in a data manipulation statement?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsCatalogsInDataManipulation() throws SQLException
    {
        // XXX needs to be checked for Sybase
        return true;
    }

    /**
     * Can a catalog name be used in an index definition statement?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException
    {
        // XXX needs to be checked for Sybase
        return true;
    }

    /**
     * Can a catalog name be used in a privilege definition statement?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException
    {
        // XXX needs to be checked for Sybase
        return true;
    }

    /**
     * Can a catalog name be used in a procedure call statement?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsCatalogsInProcedureCalls() throws SQLException
    {
        // XXX needs to be checked for Sybase
        return true;
    }

    /**
     * Can a catalog name be used in a table definition statement?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsCatalogsInTableDefinitions() throws SQLException
    {
        // XXX needs to be checked for Sybase
        return true;
    }

    /**
     * Retrieves whether this database supports column aliasing.
     * <p>
     * If so, the SQL AS clause can be used to provide names for computed
     * columns or to provide alias names for columns as required. A
     * JDBC-Compliant driver always returns true.
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsColumnAliasing() throws SQLException
    {
        // XXX needs to be checked for Sybase
        return true;
    }

    /**
     * Is the CONVERT function between SQL types supported?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsConvert() throws SQLException
    {
        return true;
    }

    /**
     * Is CONVERT between the given SQL types supported?
     *
     * @param  fromType          the type to convert from
     * @param  toType            the type to convert to
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsConvert(int fromType, int toType)
        throws SQLException
    {
        if( fromType == toType )
            return true;

        switch( fromType )
        {
            // SAfe Most types will convert to anything but IMAGE and
            //      TEXT/NTEXT (and UNIQUEIDENTIFIER, but that's not a standard
            //      type).
            case Types.BIT:
            case ParameterUtils.BOOLEAN:
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
            case Types.NUMERIC:
            case Types.DECIMAL:
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                return toType!=Types.LONGVARCHAR && toType!=Types.LONGVARBINARY;

            case Types.BINARY:
            case Types.VARBINARY:
                return toType!=Types.FLOAT && toType!=Types.REAL &&
                       toType!=Types.DOUBLE && toType!=Types.LONGVARBINARY;

            // IMAGE
            case Types.LONGVARBINARY:
                return toType==Types.BINARY || toType==Types.VARBINARY;

            // TEXT and NTEXT
            case Types.LONGVARCHAR:
                return toType==Types.CHAR || toType==Types.VARCHAR;

            // These types can be converted to anything
            case Types.NULL:
            case Types.CHAR:
            case Types.VARCHAR:
                return true;

            // We can't tell for sure what will happen with other types, so...
            case Types.OTHER:
            default:
                return false;
        }
    }

    /**
     * Is the ODBC Core SQL grammar supported?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsCoreSQLGrammar() throws SQLException
    {
        // XXX Need to check for Sybase
        return true;
    }

    /**
     * Retrieves whether this database supports correlated subqueries.
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsCorrelatedSubqueries() throws SQLException
    {
        // XXX Need to check for Sybase
        return true;
    }

    /**
     * Are both data definition and data manipulation statements within a
     * transaction supported?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsDataDefinitionAndDataManipulationTransactions()
             throws SQLException
    {
        // XXX Need to check for Sybase
        return tds.getServerType()==Tds.SQLSERVER;
    }

    /**
     * Are only data manipulation statements within a transaction supported?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsDataManipulationTransactionsOnly()
             throws SQLException
    {
        // XXX Need to check for Sybase
        return tds.getServerType()!=Tds.SQLSERVER;
    }

    /**
     * If table correlation names are supported, are they restricted to be
     * different from the names of the tables?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsDifferentTableCorrelationNames() throws SQLException
    {
        // XXX Need to check for Sybase
        return false;
    }

    /**
     * Are expressions in "ORDER BY" lists supported?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsExpressionsInOrderBy() throws SQLException
    {
        // XXX Need to check for Sybase
        return true;
    }

    /**
     * Is the ODBC Extended SQL grammar supported?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsExtendedSQLGrammar() throws SQLException
    {
        // XXX Need to check for Sybase
        return false;
    }

    /**
     * Are full nested outer joins supported?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsFullOuterJoins() throws SQLException
    {
        // XXX Need to check for Sybase

        return true;
        // per "Programming ODBC for SQLServer" Appendix A
    }

    /**
     * Is some form of "GROUP BY" clause supported?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsGroupBy() throws SQLException
    {
        return true;
    }

    /**
     * Can a "GROUP BY" clause add columns not in the SELECT provided it
     * specifies all the columns in the SELECT?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsGroupByBeyondSelect() throws SQLException
    {
        // XXX Need to check for Sybase

        return true;
        // per "Programming ODBC for SQLServer" Appendix A
    }

    /**
     * Can a "GROUP BY" clause use columns not in the SELECT?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsGroupByUnrelated() throws SQLException
    {
        // XXX need to check this for Sybase
        return true;
    }

    /**
     * Is the SQL Integrity Enhancement Facility supported?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsIntegrityEnhancementFacility() throws SQLException
    {
        // XXX Need to check for Sybase
        return false;
    }

    /**
     * Retrieves whether this database supports specifying a <code>LIKE</code>
     * escape clause.
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsLikeEscapeClause() throws SQLException
    {
        // XXX Need to check for Sybase

        return true;
        // per "Programming ODBC for SQLServer" Appendix A
    }

    /**
     * Retrieves whether this database provides limited support for outer
     * joins.  (This will be <code>true</code> if the method
     * <code>supportsFullOuterJoins</code> returns <code>true</code>).
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsLimitedOuterJoins() throws SQLException
    {
        return true;
    }

    /**
     * Retrieves whether this database supports the ODBC Minimum SQL grammar.
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsMinimumSQLGrammar() throws SQLException
    {
        // XXX Need to check for Sybase
        return true;
    }

    /**
     * Retrieves whether this database treats mixed case unquoted SQL identifiers as
     * case sensitive and as a result stores them in mixed case.
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsMixedCaseIdentifiers() throws SQLException
    {
        setCaseSensitiveFlag();
        return caseSensitive.booleanValue();
    }

    /**
     * Retrieves whether this database treats mixed case quoted SQL identifiers as
     * case sensitive and as a result stores them in mixed case.
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException
    {
        setCaseSensitiveFlag();
        return caseSensitive.booleanValue();
    }

    /**
     * Are multiple ResultSets from a single execute supported?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsMultipleResultSets() throws SQLException
    {
        return true;
    }

    /**
     * Can we have multiple transactions open at once (on different
     * connections)?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsMultipleTransactions() throws SQLException
    {
        return true;
    }

    /**
     * Retrieves whether columns in this database may be defined as non-nullable.
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsNonNullableColumns() throws SQLException
    {
        return true;
    }

    /**
     * Can cursors remain open across commits?
     *
     * @return                   true if cursors always remain open; false if
     *      they might not remain open
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException
    {
        // XXX Need to check for Sybase
        return true;
    }

    /**
     * Can cursors remain open across rollbacks?
     *
     * @return                   true if cursors always remain open; false if
     *      they might not remain open
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException
    {
        // XXX Need to check for Sybase
        return false;
    }

    /**
     * Can statements remain open across commits?
     *
     * @return                   true if statements always remain open; false if
     *      they might not remain open
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException
    {
        return true;
    }

    /**
     * Can statements remain open across rollbacks?
     *
     * @return                   true if statements always remain open; false if
     *      they might not remain open
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException
    {
        return true;
    }

    /**
     * Can an "ORDER BY" clause use columns not in the SELECT?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsOrderByUnrelated() throws SQLException
    {
        // XXX need to verify for Sybase
        return true;
    }

    /**
     * Is some form of outer join supported?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsOuterJoins() throws SQLException
    {
        return true;
    }

    /**
     * Is positioned DELETE supported?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsPositionedDelete() throws SQLException
    {
        // XXX Could we support it in the future?
        return false;
    }

    /**
     * Is positioned UPDATE supported?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsPositionedUpdate() throws SQLException
    {
        // XXX Could we support it in the future?
        return false;
    }

    /**
     * Can a schema name be used in a data manipulation statement?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsSchemasInDataManipulation() throws SQLException
    {
        // XXX needs to be checked for Sybase
        return true;
    }

    /**
     * Can a schema name be used in an index definition statement?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsSchemasInIndexDefinitions() throws SQLException
    {
        // XXX needs to be checked for Sybase
        return true;
    }

    /**
     * Can a schema name be used in a privilege definition statement?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException
    {
        // XXX needs to be checked for Sybase
        return true;
    }

    /**
     * Can a schema name be used in a procedure call statement?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsSchemasInProcedureCalls() throws SQLException
    {
        // XXX needs to be checked for Sybase
        return true;
    }

    /**
     * Can a schema name be used in a table definition statement?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsSchemasInTableDefinitions() throws SQLException
    {
        // XXX needs to be checked for Sybase
        return true;
    }

    /**
     * Is SELECT for UPDATE supported?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsSelectForUpdate() throws SQLException
    {
        // XXX Need to check for Sybase
        return false;
    }

    /**
     * Are stored procedure calls using the stored procedure escape syntax
     * supported?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsStoredProcedures() throws SQLException
    {
        return true;
    }

    /**
     * Retrieves whether this database supports subqueries in comparison
     * expressions.
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsSubqueriesInComparisons() throws SQLException
    {
        return true;
    }

    /**
     * Retrieves whether this database supports subqueries in
     * <code>EXISTS</code> expressions.
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsSubqueriesInExists() throws SQLException
    {
        return true;
    }

    /**
     * Retrieves whether this database supports subqueries in
     * <code>IN</code> statements.
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsSubqueriesInIns() throws SQLException
    {
        return true;
    }

    /**
     * Retrieves whether this database supports subqueries in quantified
     * expressions.
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsSubqueriesInQuantifieds() throws SQLException
    {
        // XXX Need to check for Sybase
        return true;
    }

    /**
     * Retrieves whether this database supports table correlation names.
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsTableCorrelationNames() throws SQLException
    {
        return true;
    }

    /**
     * Does the database support the given transaction isolation level?
     *
     * @param  level             the values are defined in java.sql.Connection
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     * @see                      Connection
     */
    public boolean supportsTransactionIsolationLevel( int level )
             throws SQLException
    {
        return true;
    }

    /**
     * Retrieves whether this database supports transactions. If not, invoking the
     * method <code>commit</code> is a noop, and the isolation level is
     * <code>TRANSACTION_NONE</code>.
     *
     * @return                   true if transactions are supported
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsTransactions() throws SQLException
    {
        return true;
    }

    /**
     * Is SQL UNION supported?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsUnion() throws SQLException
    {
        return true;
    }

    /**
     * Is SQL UNION ALL supported?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean supportsUnionAll() throws SQLException
    {
        return true;
    }

    /**
     * Does the database use a file for each table?
     *
     * @return                   true if the database uses a local file for each
     *      table
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean usesLocalFilePerTable() throws SQLException
    {
        return false;
    }

    /**
     * Does the database store tables in a local file?
     *
     * @return                   true if so
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean usesLocalFiles() throws SQLException
    {
        return false;
    }

    //--------------------------JDBC 2.0-----------------------------

    /**
     * JDBC 2.0 Does the database support the given result set type?
     *
     * @param  type              defined in <code>java.sql.ResultSet</code>
     * @return                   <code>true</code> if so; <code>false</code>
     *      otherwise
     * @exception  SQLException  if a database access error occurs
     * @see                      Connection
     */
    public boolean supportsResultSetType( int type ) throws SQLException
    {
        // jTDS supports all ResultSet types (more or less)
        return true;
    }

    /**
     * JDBC 2.0 Does the database support the concurrency type in combination
     * with the given result set type?
     *
     * @param  type              defined in <code>java.sql.ResultSet</code>
     * @param  concurrency       type defined in <code>java.sql.ResultSet</code>
     * @return                   <code>true</code> if so; <code>false</code>
     *      otherwise
     * @exception  SQLException  if a database access error occurs
     * @see                      Connection
     */
    public boolean supportsResultSetConcurrency( int type, int concurrency )
             throws SQLException
    {
        // jTDS supports both read-only and updatable ResultSets (more or less)
        return true;
    }

    /**
     * JDBC 2.0 Indicates whether a result set's own updates are visible.
     *
     * @param  type              </code>ResultSet<code> type
     * @return                   <code>true</code> if updates are visible for the
     *      result set type; <code>false</code> otherwise
     * @exception  SQLException  if a database access error occurs
     */
    public boolean ownUpdatesAreVisible( int type ) throws SQLException
    {
        // No support in SQL Server for this
        return false;
    }

    /**
     * JDBC 2.0 Indicates whether a result set's own deletes are visible.
     *
     * @param  type              </code>ResultSet<code> type
     * @return                   <code>true</code> if deletes are visible for the
     *      result set type; <code>false</code> otherwise
     * @exception  SQLException  if a database access error occurs
     */
    public boolean ownDeletesAreVisible( int type ) throws SQLException
    {
        // Yes, own deletes are visible
        return true;
    }

    /**
     * JDBC 2.0 Indicates whether a result set's own inserts are visible.
     *
     * @param  type              </code>ResultSet<code> type
     * @return                   <code>true</code> if inserts are visible for the
     *      result set type; <code>false</code> otherwise
     * @exception  SQLException  if a database access error occurs
     */
    public boolean ownInsertsAreVisible( int type ) throws SQLException
    {
        // No support in SQL Server for this
        return false;
    }

    /**
     * JDBC 2.0 Indicates whether updates made by others are visible.
     *
     * @param  type              </code>ResultSet<code> type
     * @return                   <code>true</code> if updates made by others are
     *      visible for the result set type; <code>false</code> otherwise
     * @exception  SQLException  if a database access error occurs
     */
    public boolean othersUpdatesAreVisible( int type ) throws SQLException
    {
        // Updates not supported, so just return false
        return false;
    }

    /**
     * JDBC 2.0 Indicates whether deletes made by others are visible.
     *
     * @param  type              </code>ResultSet<code> type
     * @return                   <code>true</code> if deletes made by others are
     *      visible for the result set type; <code>false</code> otherwise
     * @exception  SQLException  if a database access error occurs
     */
    public boolean othersDeletesAreVisible( int type ) throws SQLException
    {
        // @todo Make sure this is indeed true
        return type == ResultSet.TYPE_SCROLL_SENSITIVE;
    }

    /**
     * JDBC 2.0 Indicates whether inserts made by others are visible.
     *
     * @param  type              </code>ResultSet<code> type
     * @return                   true if updates are visible for the result set
     *      type
     * @return                   <code>true</code> if inserts made by others are
     *      visible for the result set type; <code>false</code> otherwise
     * @exception  SQLException  if a database access error occurs
     */
    public boolean othersInsertsAreVisible( int type ) throws SQLException
    {
        // Inserts not supported, so just return false
        return false;
    }

    /**
     * JDBC 2.0 Indicates whether or not a visible row update can be detected
     * by calling the method <code>ResultSet.rowUpdated</code> .
     *
     * @param  type              </code>ResultSet<code> type
     * @return                   <code>true</code> if changes are detected by the
     *      result set type; <code>false</code> otherwise
     * @exception  SQLException  if a database access error occurs
     */
    public boolean updatesAreDetected( int type ) throws SQLException
    {
        return false;
    }

    /**
     * JDBC 2.0 Indicates whether or not a visible row delete can be detected
     * by calling ResultSet.rowDeleted(). If deletesAreDetected() returns
     * false, then deleted rows are removed from the result set.
     *
     * @param  type              </code>ResultSet<code> type
     * @return                   true if changes are detected by the resultset
     *      type
     * @exception  SQLException  if a database access error occurs
     */
    public boolean deletesAreDetected( int type ) throws SQLException
    {
        return type == ResultSet.TYPE_SCROLL_SENSITIVE;
    }

    /**
     * JDBC 2.0 Indicates whether or not a visible row insert can be detected
     * by calling ResultSet.rowInserted().
     *
     * @param  type              </code>ResultSet<code> type
     * @return                   true if changes are detected by the resultset
     *      type
     * @exception  SQLException  if a database access error occurs
     */
    public boolean insertsAreDetected( int type ) throws SQLException
    {
        return false;
    }

    /**
     * JDBC 2.0 Indicates whether the driver supports batch updates.
     *
     * @return                   true if the driver supports batch updates; false
     *      otherwise
     * @exception  SQLException  never
     */
    public boolean supportsBatchUpdates() throws SQLException {
        return true;
    }

    private void setCaseSensitiveFlag() throws SQLException
    {
        if( caseSensitive == null )
        {
            Statement s = connection.createStatement();
            ResultSet rs = s.executeQuery("sp_server_info 16");
            rs.next();
            caseSensitive = rs.getString(3).equalsIgnoreCase("MIXED") ?
                Boolean.FALSE : Boolean.TRUE;
            s.close();
        }
    }

    public java.sql.ResultSet getAttributes(String str, String str1, String str2, String str3) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the database major version.
     */
    public int getDatabaseMajorVersion() throws SQLException {
        return connection.getDatabaseMajorVersion();
    }

    /**
     * Returns the database minor version.
     */
    public int getDatabaseMinorVersion() throws SQLException {
        return connection.getDatabaseMinorVersion();
    }

    /**
     * Returns the JDBC major version.
     */
    public int getJDBCMajorVersion() throws SQLException {
        return 2;
    }

    /**
     * Returns the JDBC minor version.
     */
    public int getJDBCMinorVersion() throws SQLException {
        return 1;
    }

    public int getResultSetHoldability() throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public int getSQLStateType() throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public java.sql.ResultSet getSuperTables(String str, String str1, String str2) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public java.sql.ResultSet getSuperTypes(String str, String str1, String str2) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns <code>true</code> if updates are made to a copy of the LOB; returns
     * <code>false</code> if LOB updates are made directly to the database.
     * <p>
     * NOTE: Since SQL Server / Sybase do not support LOB locators as Oracle does (AFAIK);
     * this method always returns <code>true</code>.
     */
    public boolean locatorsUpdateCopy() throws SQLException {
        return true;
    }

    /**
     * Returns <code>true</code> if getting auto-generated keys is supported after a
     * statment is executed; returns <code>false</code> otherwise
     */
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return true;
    }

    /**
     * Returns <code>true</code> if Callable statements can return multiple result sets;
     * returns <code>false</code> if they can only return one result set.
     */
    public boolean supportsMultipleOpenResults() throws SQLException {
        return true;
    }

    /**
     * Returns <code>true</code> if the database supports named parameters;
     * returns <code>false</code> if the database does not support named parameters.
     */
    public boolean supportsNamedParameters() throws SQLException {
        return true;
    }

    public boolean supportsResultSetHoldability(int param) throws SQLException {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns <code>true</code> if savepoints are supported; returns
     * <code>false</code> otherwise
     */
    public boolean supportsSavepoints() throws SQLException {
        return true;
    }

    /**
     * Returns <code>true</code> if the database supports statement pooling;
     * returns <code>false</code> otherwise.
     */
    public boolean supportsStatementPooling() throws SQLException {
        return true;
    }
}
