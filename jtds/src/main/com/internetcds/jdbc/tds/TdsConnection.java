//
// Copyright 1998, 1999 CDS Networks, Inc., Medford Oregon
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
import java.util.Properties;
import java.util.Vector;

import freetds.*;

/**
 *  Description of the Class
 *
 *@author     skizz
 *@created    March 16, 2001
 */
class TdsInstance {

    /**
     *  Description of the Field
     */
    public boolean inUse = false;
    /**
     *  Description of the Field
     */
    public Tds tds = null;
    /**
     *  Description of the Field
     */
    public final static String cvsVersion = "$Id: TdsConnection.java,v 1.1.1.1 2001-08-10 01:44:34 skizz Exp $";


    public TdsInstance(Tds tds_)
    {
        tds = tds_;
        inUse = false;
    }
}


/**
 *  <P>
 *
 *  A Connection represents a session with a specific database. Within the
 *  context of a Connection, SQL statements are executed and results are
 *  returned. <P>
 *
 *  A Connection's database is able to provide information describing its
 *  tables, its supported SQL grammar, its stored procedures, the capabilities
 *  of this connection, etc. This information is obtained with the getMetaData
 *  method. <P>
 *
 *  <B>Note:</B> By default the Connection automatically commits changes after
 *  executing each statement. If auto commit has been disabled, an explicit
 *  commit must be done or database changes will not be saved.
 *
 *@author     Craig Spannring
 *@author     Igor Petrovski
 *@author     The FreeTDS project
 *@created    March 16, 2001
 *@version    $Id: TdsConnection.java,v 1.1.1.1 2001-08-10 01:44:34 skizz Exp $
 *@see        DriverManager#getConnection
 *@see        Statement
 *@see        ResultSet
 *@see        DatabaseMetaData
 */
public class TdsConnection implements ConnectionHelper, Connection {


    String host = null;
    int serverType = -1;
    // Can be either Driver.SYBASE or Driver.SQLSERVER
    int port = -1;
    // Port numbers are _unsigned_ 16 bit, short is too small
    String database = null;
    Properties initialProps = null;

    Vector tdsPool = null;
    DatabaseMetaData databaseMetaData = null;
    Vector allStatements = null;

    boolean autoCommit = true;
    int transactionIsolationLevel = java.sql.Connection.TRANSACTION_READ_COMMITTED;
    boolean isClosed = false;

    private SQLWarningChain warningChain;
    /**
     *  Description of the Field
     */
    public final static String cvsVersion = "$Id: TdsConnection.java,v 1.1.1.1 2001-08-10 01:44:34 skizz Exp $";




    /**
     *  Connect via TDS to a database server.
     *
     *@param  props_                                     Description of
     *      Parameter
     *@exception  SQLException                           if a database access
     *      error occurs
     *@exception  com.internetcds.jdbc.tds.TdsException  Description of
     *      Exception
     */
    public TdsConnection(
            Properties props_)
             throws SQLException, com.internetcds.jdbc.tds.TdsException
    {
        host = props_.getProperty("HOST");
        serverType = Integer.parseInt(props_.getProperty("SERVERTYPE"));
        port = Integer.parseInt(props_.getProperty("PORT"));
        database = props_.getProperty("DBNAME");
        String user = props_.getProperty("user");
        String password = props_.getProperty("password");
        initialProps = props_;

        warningChain = new SQLWarningChain();

        if (user == null) {
            user = props_.getProperty("USER");
            if (user == null) {
                throw new SQLException("Need a username.");
            }
            props_.put("user", user);
        }

        if (password == null) {
            password = props_.getProperty("PASSWORD");
            if (password == null) {
                throw new SQLException("Need a password.");
            }
            props_.put("password", password);
        }

        if (tdsPool == null) {
            tdsPool = new Vector(20);
        }

        if (allStatements == null) {
            allStatements = new Vector(2);
        }

        Tds tmpTds = this.allocateTds();
        freeTds(tmpTds);
    }


    /**
     *  If a connection is in auto-commit mode, then all its SQL statements will
     *  be executed and committed as individual transactions. Otherwise, its SQL
     *  statements are grouped into transactions that are terminated by either
     *  commit() or rollback(). By default, new connections are in auto-commit
     *  mode. The commit occurs when the statement completes or the next execute
     *  occurs, whichever comes first. In the case of statements returning a
     *  ResultSet, the statement completes when the last row of the ResultSet
     *  has been retrieved or the ResultSet has been closed. In advanced cases,
     *  a single statement may return multiple results as well as output
     *  parameter values. Here the commit occurs when all results and output
     *  param values have been retrieved.
     *
     *@param  value             true enables auto-commit; false disables
     *      auto-commit.
     *@exception  SQLException  if a database-access error occurs.
     */
    public void setAutoCommit(boolean value) throws SQLException
    {
        int i;
        String sql = null;

        autoCommit = value;

        sql = sqlStatementToSetCommit();

        for (i = 0; i < allStatements.size(); i++) {
            Statement stmt = (Statement) allStatements.elementAt(i);
             {
                // Note-  stmt.execute implicitly eats the END_TOKEN
                // that will come back from the commit command
                stmt.execute(sql);
            }
        }
    }


    /**
     *  You can put a connection in read-only mode as a hint to enable database
     *  optimizations <B>Note:</B> setReadOnly cannot be called while in the
     *  middle of a transaction
     *
     *@param  readOnly          - true enables read-only mode; false disables it
     *@exception  SQLException  if a database access error occurs
     */
    public void setReadOnly(boolean readOnly) throws SQLException
    {
        throw new SQLException("Not implemented (setReadOnly)");
    }



    /**
     *  A sub-space of this Connection's database may be selected by setting a
     *  catalog name. If the driver does not support catalogs it will silently
     *  ignore this request.
     *
     *@param  catalog           The new Catalog value
     *@exception  SQLException  if a database-access error occurs.
     */
    public void setCatalog(String catalog) throws SQLException
    {
        throw new SQLException("Not implemented (setCatalog)");
    }


    /**
     *  You can call this method to try to change the transaction isolation
     *  level using one of the TRANSACTION_* values. <P>
     *
     *  <B>Note:</B> setTransactionIsolation cannot be called while in the
     *  middle of a transaction.
     *
     *@param  level             one of the TRANSACTION_* isolation values with
     *      the exception of TRANSACTION_NONE; some databases may not support
     *      other values
     *@exception  SQLException  if a database-access error occurs.
     *@see                      DatabaseMetaData#supportsTransactionIsolationLevel
     */
    public void setTransactionIsolation(int level)
             throws SQLException
    {
        int i;
        String sql;

        transactionIsolationLevel = level;

        sql = sqlStatementToSetTransactionIsolationLevel();

        for (i = 0; i < allStatements.size(); i++) {
            Statement stmt = (Statement) allStatements.elementAt(i);
             {
                // Note-  stmt.execute implicitly eats the END_TOKEN
                // that will come back from the commit command
                stmt.execute(sql);
            }
        }
    }


    /**
     *  JDBC 2.0 Installs the given type map as the type map for this
     *  connection. The type map will be used for the custom mapping of SQL
     *  structured types and distinct types.
     *
     *@param  map               The new TypeMap value
     *@exception  SQLException  Description of Exception
     */
    public void setTypeMap(java.util.Map map) throws SQLException
    {
        NotImplemented();
    }


    public String getUrl()
    {
        // XXX Is it legal to return something that might not be
        // exactly the URL used to connect?
        return
                ("jdbc:freetds:"
                 + (serverType == Tds.SYBASE ? "sybase" : "sqlserver")
                 + "://" + host + ":" + port + "/" + database);
    }


    /**
     *  Get the current auto-commit state.
     *
     *@return                   Current state of auto-commit mode.
     *@exception  SQLException  if a database-access error occurs.
     *@see                      #setAutoCommit
     */
    public boolean getAutoCommit() throws SQLException
    {
        return autoCommit;
    }


    /**
     *  Tests to see if a Connection is closed.
     *
     *@return                   true if the connection is closed; false if it's
     *      still open
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean isClosed() throws SQLException
    {
        return isClosed;
    }


    /**
     *  A connection's database is able to provide information describing its
     *  tables, its supported SQL grammar, its stored procedures, the
     *  capabilities of this connection, etc. This information is made available
     *  through a DatabaseMetaData object.
     *
     *@return                   a DatabaseMetaData object for this connection
     *@exception  SQLException  if a database access error occurs
     */
    public java.sql.DatabaseMetaData getMetaData() throws SQLException
    {
        if (databaseMetaData == null) {
            // The DatabaseMetaData may need the tds connection
            // at some later time.  Therefore we shouldn't relinquish the
            // tds.
            Tds tds = this.allocateTds();
            databaseMetaData = new com.internetcds.jdbc.tds.DatabaseMetaData(this, tds);
        }
        return databaseMetaData;
//       catch(java.net.UnknownHostException e)
//       {
//          throw new SQLException(e.getMessage());
//       }
    }


    /**
     *  Tests to see if the connection is in read-only mode.
     *
     *@return                   true if connection is read-only
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean isReadOnly() throws SQLException
    {
        throw new SQLException("Not implemented (isReadOnly)");
    }


    /**
     *  Return the Connection's current catalog name.
     *
     *@return                   the current catalog name or null
     *@exception  SQLException  if a database-access error occurs.
     */
    public String getCatalog() throws SQLException
    {
        throw new SQLException("Not implemented (getCatalog)");
    }



    /**
     *  Get this Connection's current transaction isolation mode.
     *
     *@return                   the current TRANSACTION_* mode value
     *@exception  SQLException  if a database-access error occurs.
     */
    public int getTransactionIsolation() throws SQLException
    {
        return transactionIsolationLevel;
    }


    /**
     *  The first warning reported by calls on this Connection is returned. <P>
     *
     *  <B>Note:</B> Subsequent warnings will be chained to this SQLWarning.
     *
     *@return                   the first SQLWarning or null
     *@exception  SQLException  if a database-access error occurs.
     */
    public SQLWarning getWarnings() throws SQLException
    {
        return warningChain.getWarnings();
    }


    /**
     *  JDBC 2.0 Gets the type map object associated with this connection.
     *  Unless the application has added an entry to the type map, the map
     *  returned will be empty.
     *
     *@return                   the <code>java.util.Map</code> object associated
     *      with this <code>Connection</code> object
     *@exception  SQLException  Description of Exception
     */
    public java.util.Map getTypeMap() throws SQLException
    {
        NotImplemented();
        return null;
    }



    public void markAsClosed(java.sql.Statement stmt) throws TdsException
    {
        if (!allStatements.removeElement(stmt)) {
            throw new TdsException("Statement was not known by the connection");
        }
    }


    /**
     *  return a tds instance back to the tds pool for reuse. A thread that is
     *  using a tds instance should return the instance back to the tds pool
     *  when it is finished using it.
     *
     *@param  tds               Description of Parameter
     *@exception  TdsException  Description of Exception
     *@see                      allocateTds
     */
    public void relinquish(Tds tds)
             throws TdsException
    {
        freeTds(tds);
    }


    /**
     *  SQL statements without parameters are normally executed using Statement
     *  objects. If the same SQL statement is executed many times, it is more
     *  efficient to use a PreparedStatement JDBC 2.0 Result sets created using
     *  the returned Statement will have forward-only type, and read-only
     *  concurrency, by default.
     *
     *@return                   a new Statement object
     *@exception  SQLException  passed through from the constructor
     */
    public java.sql.Statement createStatement() throws SQLException
    {
        Statement result = new TdsStatement( this, allocateTds() );
        allStatements.addElement(result);
        return result;
    }



    /**
     *  A SQL statement with or without IN parameters can be pre-compiled and
     *  stored in a PreparedStatement object. This object can then be used to
     *  efficiently execute this statement multiple times. <P>
     *
     *  <B>Note:</B> This method is optimized for handling parametric SQL
     *  statements that benefit from precompilation. If the driver supports
     *  precompilation, prepareStatement will send the statement to the database
     *  for precompilation. Some drivers may not support precompilation. In this
     *  case, the statement may not be sent to the database until the
     *  PreparedStatement is executed. This has no direct affect on users;
     *  however, it does affect which method throws certain SQLExceptions. JDBC
     *  2.0 Result sets created using the returned PreparedStatement will have
     *  forward-only type and read-only concurrency, by default.
     *
     *@param  sql               a SQL statement that may contain one or more '?'
     *      IN parameter placeholders
     *@return                   a new PreparedStatement object containing the
     *      pre-compiled statement
     *@exception  SQLException  if a database-access error occurs.
     */
    public java.sql.PreparedStatement prepareStatement(String sql)
             throws SQLException
    {
        java.sql.PreparedStatement result;

        Tds tmpTds = this.allocateTds();

        result = Constructors.newPreparedStatement(this, tmpTds, sql);

        allStatements.addElement(result);

        return result;
    }


    /**
     *  A SQL stored procedure call statement is handled by creating a
     *  CallableStatement for it. The CallableStatement provides methods for
     *  setting up its IN and OUT parameters and methods for executing it. <B>
     *  Note:</B> This method is optimised for handling stored procedure call
     *  statements. Some drivers may send the call statement to the database
     *  when the prepareCall is done; others may wait until the
     *  CallableStatement is executed. This has no direct effect on users;
     *  however, it does affect which method throws certain SQLExceptions JDBC
     *  2.0 Result sets created using the returned CallableStatement will have
     *  forward-only type and read-only concurrency, by default.
     *
     *@param  sql               a SQL statement that may contain one or more '?'
     *      parameter placeholders. Typically this statement is a JDBC function
     *      call escape string.
     *@return                   a new CallableStatement object containing the
     *      pre-compiled SQL statement
     *@exception  SQLException  if a database access error occurs
     */
    public java.sql.CallableStatement prepareCall(String sql) throws SQLException
    {
        java.sql.CallableStatement result;

        Tds tmpTds = this.allocateTds();

        result = Constructors.newCallableStatement(this, tmpTds, sql);
        allStatements.addElement(result);

        return result;
    }


    /**
     *  A driver may convert the JDBC sql grammar into its system's native SQL
     *  grammar prior to sending it; nativeSQL returns the native form of the
     *  statement that the driver would have sent.
     *
     *@param  sql               a SQL statement that may contain one or more '?'
     *      parameter placeholders
     *@return                   the native form of this statement
     *@exception  SQLException  if a database access error occurs
     */

    public String nativeSQL(String sql) throws SQLException
    {
        return Tds.toNativeSql(sql, serverType);
    }


    /**
     *  Commit makes all changes made since the previous commit/rollback
     *  permanent and releases any database locks currently held by the
     *  Connection. This method should only be used when auto commit has been
     *  disabled.
     *
     *@exception  SQLException  if a database-access error occurs.
     *@see                      #setAutoCommit
     */
    public void commit() throws SQLException
    {
        commitOrRollback(true);
    }


    /**
     *  Rollback drops all changes made since the previous commit/rollback and
     *  releases any database locks currently held by the Connection. This
     *  method should only be used when auto commit has been disabled.
     *
     *@exception  SQLException  if a database-access error occurs.
     *@see                      #setAutoCommit
     */
    public void rollback() throws SQLException
    {
        commitOrRollback(false);
    }


    /**
     *  In some cases, it is desirable to immediately release a Connection's
     *  database and JDBC resources instead of waiting for them to be
     *  automatically released; the close method provides this immediate
     *  release. <P>
     *
     *  <B>Note:</B> A Connection is automatically closed when it is garbage
     *  collected. Certain fatal errors also result in a closed Connection.
     *
     *@exception  SQLException  if a database-access error occurs.
     */
    public void close() throws SQLException
    {
        int i;

        for (i = 0; i < allStatements.size(); i++) {
            Statement stmt = (Statement) allStatements.elementAt(i);
             {
                stmt.close();
            }
        }

        for (i = 0; i < tdsPool.size(); i++) {
            ((TdsInstance) tdsPool.elementAt(i)).tds.close();
        }

        clearWarnings();
        isClosed = true;
    }


    /**
     *  After this call, getWarnings returns null until a new warning is
     *  reported for this connection.
     *
     *@exception  SQLException  if a database access error occurs
     */
    public void clearWarnings() throws SQLException
    {
        warningChain.clearWarnings();
    }


    //--------------------------JDBC 2.0-----------------------------

    /**
     *  JDBC 2.0 Creates a <code>Statement</code> object that will generate
     *  <code>ResultSet</code> objects with the given type and concurrency. This
     *  method is the same as the <code>createStatement</code> method above, but
     *  it allows the default result set type and result set concurrency type to
     *  be overridden.
     *
     *@param  resultSetType         a result set type; see ResultSet.TYPE_XXX
     *@param  resultSetConcurrency  a concurrency type; see ResultSet.CONCUR_XXX
     *@return                       a new Statement object
     *@exception  SQLException      if a database access error occurs
     */
    public java.sql.Statement createStatement( int type, int concurrency )
             throws SQLException
    {
        Statement result = new CursorStatement( this, allocateTds(), type, concurrency );
        allStatements.addElement(result);
        return result;
    }


    /**
     *  JDBC 2.0 Creates a <code>PreparedStatement</code> object that will
     *  generate <code>ResultSet</code> objects with the given type and
     *  concurrency. This method is the same as the <code>prepareStatement
     *  </code>method above, but it allows the default result set type and
     *  result set concurrency type to be overridden.
     *
     *@param  resultSetType         a result set type; see ResultSet.TYPE_XXX
     *@param  resultSetConcurrency  a concurrency type; see ResultSet.CONCUR_XXX
     *@param  sql                   Description of Parameter
     *@return                       a new PreparedStatement object containing
     *      the pre-compiled SQL statement
     *@exception  SQLException      if a database access error occurs
     */
    public java.sql.PreparedStatement prepareStatement(
            String sql,
            int resultSetType,
            int resultSetConcurrency)
             throws SQLException
    {
        NotImplemented();
        return null;
    }


    /**
     *  JDBC 2.0 Creates a <code>CallableStatement</code> object that will
     *  generate <code>ResultSet</code> objects with the given type and
     *  concurrency. This method is the same as the <code>prepareCall</code>
     *  method above, but it allows the default result set type and result set
     *  concurrency type to be overridden.
     *
     *@param  resultSetType         a result set type; see ResultSet.TYPE_XXX
     *@param  resultSetConcurrency  a concurrency type; see ResultSet.CONCUR_XXX
     *@param  sql                   Description of Parameter
     *@return                       a new CallableStatement object containing
     *      the pre-compiled SQL statement
     *@exception  SQLException      if a database access error occurs
     */
    public java.sql.CallableStatement prepareCall(
            String sql,
            int resultSetType,
            int resultSetConcurrency) throws SQLException
    {
        NotImplemented();
        return null;
    }


    protected void NotImplemented() throws java.sql.SQLException
    {
        throw new java.sql.SQLException("Not Implemented");
    }


    protected String sqlStatementToInitialize()
    {
        return serverType == Tds.SYBASE ? "set quoted_identifier on set textsize 50000" : "";
    }


    protected String sqlStatementToSetTransactionIsolationLevel()
             throws SQLException
    {
        String sql = "set transaction isolation level ";

        if (serverType == Tds.SYBASE) {
            switch (transactionIsolationLevel) {
                case java.sql.Connection.TRANSACTION_READ_UNCOMMITTED:
                {
                    throw new SQLException("Bad transaction level");
                }
                case java.sql.Connection.TRANSACTION_READ_COMMITTED:
                {
                    sql = sql + "1";
                    break;
                }
                case java.sql.Connection.TRANSACTION_REPEATABLE_READ:
                {
                    throw new SQLException("Bad transaction level");
                }
                case java.sql.Connection.TRANSACTION_SERIALIZABLE:
                {
                    sql = sql + "3";
                    break;
                }
                case java.sql.Connection.TRANSACTION_NONE:
                default:
                {
                    throw new SQLException("Bad transaction level");
                }
            }
        }
        else {
            switch (transactionIsolationLevel) {
                case java.sql.Connection.TRANSACTION_READ_UNCOMMITTED:
                {
                    sql = sql + " read uncommitted ";
                    break;
                }
                case java.sql.Connection.TRANSACTION_READ_COMMITTED:
                {
                    sql = sql + " read committed ";
                    break;
                }
                case java.sql.Connection.TRANSACTION_REPEATABLE_READ:
                {
                    sql = sql + " repeatable read ";
                    break;
                }
                case java.sql.Connection.TRANSACTION_SERIALIZABLE:
                {
                    throw new SQLException("SQLServer does not support " +
                            "TRANSACTION_SERIALIZABLE");
                }
                case java.sql.Connection.TRANSACTION_NONE:
                default:
                {
                    throw new SQLException("Bad transaction level");
                }
            }
        }
        return sql;
    }


    protected String sqlStatementToSetCommit()
    {
        String result;

        if (serverType == Tds.SYBASE) {
            if (autoCommit) {
                result = "set CHAINED off ";
            }
            else {
                result = "set CHAINED on ";
            }
        }
        else {
            if (autoCommit) {
                result = "set implicit_transactions off ";
            }
            else {
                result = "set implicit_transactions on ";
            }
        }
        return result;
    }



    protected String sqlStatementForSettings()
             throws SQLException
    {
        return
                sqlStatementToInitialize() + " " +
                sqlStatementToSetTransactionIsolationLevel() + " " +
                sqlStatementToSetCommit();
    }


    /**
     *  allocate a tds instance to the calling thread. <br>
     *  The routine tries to reuse an available tds instance. If there are no
     *  tds instances that aren't in use it will create a new instance.
     *
     *@return                                            A tds instance to use
     *      for database communications.
     *@exception  java.sql.SQLException
     */
    private synchronized Tds allocateTds()
             throws java.sql.SQLException
    {
        Tds result;
        int i;

        try {
            i = findAnAvailableTds();
            if (i == -1) {
                Tds tmpTds = null;
                try {
                    tmpTds = new Tds((java.sql.Connection) this,
                            initialProps, sqlStatementForSettings());
                }
                catch (SQLException e) {
                    throw new SQLException(e.getMessage() + "\n" + tdsPool.size()
                             + " connection are in use by this program");
                }
                TdsInstance tmp = new TdsInstance(tmpTds);
                tdsPool.addElement(tmp);
                i = findAnAvailableTds();
            }
            if (i == -1) {
                throw new TdsException("Internal Error.  Couldn't get tds instance");
            }
            if (((TdsInstance) tdsPool.elementAt(i)).inUse) {
                throw new TdsException("Internal Error.  tds " + i
                         + " is already allocated");
            }
            ((TdsInstance) tdsPool.elementAt(i)).inUse = true;
            result = ((TdsInstance) (tdsPool.elementAt(i))).tds;

            result.changeSettings(null, sqlStatementForSettings());
        }
        catch (com.internetcds.jdbc.tds.TdsException e) {
            throw new SQLException(e.getMessage());
        }
        catch (java.io.IOException e) {
            throw new SQLException(e.getMessage());
        }
        return result;
    }


    /**
     *  Find a tds in the TdsPool that is not in use.
     *
     *@return    -1 if none were found, otherwise return the index a tds
     */
    private int findAnAvailableTds()
    {
        int i;

        for (i = tdsPool.size() - 1;
                i >= 0 && ((TdsInstance) tdsPool.elementAt(i)).inUse;
                i--) {
            // nop
        }
        return i;
    }


    /**
     *  return a tds instance back to the tds pool for reuse.
     *
     *@param  tds               Description of Parameter
     *@exception  TdsException  Description of Exception
     *@see                      allocateTds
     */
    private void freeTds(Tds tds)
             throws TdsException
    {
        int i;

        i = -1;
        do {
            i++;
        } while (i < tdsPool.size()
                 && tds != ((TdsInstance) tdsPool.elementAt(i)).tds);

        if (i < tdsPool.size()) {
            // System.out.println("dealloacting a tds instance");
            ((TdsInstance) tdsPool.elementAt(i)).inUse = false;

            // XXX Should also send a cancel to the server and throw out any
            // data that has already been sent.
        }
        else {
            throw new TdsException("Tried to free a tds that wasn't in use");
        }
    }


    private void commitOrRollback(boolean commit)
             throws SQLException
    {
        int i;
        SQLException exception = null;

        if (autoCommit) {
            throw new SQLException("This method should only be " +
                    " used when auto commit has been disabled.");
        }

        // XXX race condition here.  It is possible that a statement could
        // close while running this for loop.
        for (i = 0; i < allStatements.size(); i++) {
            TdsStatement stmt = (TdsStatement) allStatements.elementAt(i);

            try {
                if (commit) {
                    stmt.commit();
                }
                else {
                    stmt.rollback();
                }
            }
            // XXX need to put all of these into the warning chain.
            //
            // Don't think so, the warnings would belong to Statement anyway -- SB
            catch (java.sql.SQLException e) {
                exception = e;
            }
            catch (java.io.IOException e) {
                exception = new SQLException(e.getMessage());
            }
            catch (com.internetcds.jdbc.tds.TdsException e) {
                exception = new SQLException(e.getMessage());
            }

            if (stmt instanceof CallableStatement) {
                ((PreparedStatementHelper) stmt).dropAllProcedures();
                throw new SQLException("Not implemented");
            }
            else if (stmt instanceof PreparedStatement) {
                ((PreparedStatementHelper) stmt).dropAllProcedures();
            }
        }
        if (exception != null) {
            throw exception;
        }
    }
}

