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
 * Wrapper for a <code>Tds</code> instance and a flag marking if the
 * <code>Tds</code> instance is in use.
 *
 * @author  skizz
 * @created March 16, 2001
 */
class TdsInstance
{
    /**
     * Set if the <code>Tds</code> instance is in use.
     */
    public boolean inUse = false;
    /**
     * <code>Tds</code> instance wrapped by this object.
     */
    public Tds tds = null;
    /**
     * CVS revision of the file.
     */
    public final static String cvsVersion = "$Id: TdsConnection.java,v 1.21 2002-09-23 14:17:08 alin_sinpalean Exp $";

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
 * @author     Craig Spannring
 * @author     Igor Petrovski
 * @author     Alin Sinpalean
 * @author     The FreeTDS project
 * @created    March 16, 2001
 * @version    $Id: TdsConnection.java,v 1.21 2002-09-23 14:17:08 alin_sinpalean Exp $
 * @see        Statement
 * @see        ResultSet
 * @see        DatabaseMetaData
 */
public class TdsConnection implements ConnectionHelper, Connection
{
    private String host = null;
    // Can be either Driver.SYBASE or Driver.SQLSERVER
    private int serverType = -1;
    // Port numbers are _unsigned_ 16 bit, short is too small
    private int port = -1;
    private int tdsVer = -1;
    private String database = null;
    private Properties initialProps = null;

    private Vector tdsPool = null;
    private DatabaseMetaData databaseMetaData = null;
    private Vector allStatements = null;

    private boolean autoCommit = true;
    private int transactionIsolationLevel = java.sql.Connection.TRANSACTION_READ_COMMITTED;
    private boolean isClosed = false;

    private SQLWarningChain warningChain;
    /**
     * CVS revision of the file.
     */
    public final static String cvsVersion = "$Id: TdsConnection.java,v 1.21 2002-09-23 14:17:08 alin_sinpalean Exp $";

    /**
     * Create a <code>Connection</code> to a database server.
     *
     * @param  props_            <code>Properties</code> of the new
     *     <code>Connection</code> instance
     * @exception  SQLException  if a database access error occurs
     * @exception  TdsException  if a network protocol error occurs
     */
    public TdsConnection(Properties props_)
        throws SQLException, TdsException
    {
        host = props_.getProperty(Tds.PROP_HOST);
        serverType = Integer.parseInt(props_.getProperty(Tds.PROP_SERVERTYPE));
        port = Integer.parseInt(props_.getProperty(Tds.PROP_PORT));
        database = props_.getProperty(Tds.PROP_DBNAME);
        String user = props_.getProperty(Tds.PROP_USER);
        String password = props_.getProperty(Tds.PROP_PASSWORD);
        initialProps = props_;

        warningChain = new SQLWarningChain();

        if (user == null) {
            user = props_.getProperty(Tds.PROP_USER.toLowerCase());
            if (user == null) {
                throw new SQLException("Need a username.");
            }
            props_.put(Tds.PROP_USER, user);
        }

        if (password == null) {
            password = props_.getProperty(Tds.PROP_PASSWORD.toLowerCase());
            if (password == null) {
                throw new SQLException("Need a password.");
            }
            props_.put(Tds.PROP_PASSWORD, password);
        }

        if (tdsPool == null) {
            tdsPool = new Vector(20);
        }

        if (allStatements == null) {
            allStatements = new Vector(2);
        }

        Tds tmpTds = this.allocateTds();
        tdsVer = tmpTds.getTdsVer();
        database = tmpTds.getDatabase();
        freeTds(tmpTds);
    }

    /**
     * Returns the version of TDS used (one of the TdsDefinitions.TDS<i>XX</i>
     * constants).
     */
    protected int getTdsVer() throws SQLException
    {
        checkClosed();
        return tdsVer;
    }

    /**
     * If a connection is in auto-commit mode, then all its SQL statements will
     * be executed and committed as individual transactions. Otherwise, its SQL
     * statements are grouped into transactions that are terminated by either
     * commit() or rollback(). By default, new connections are in auto-commit
     * mode. The commit occurs when the statement completes or the next execute
     * occurs, whichever comes first. In the case of statements returning a
     * ResultSet, the statement completes when the last row of the ResultSet
     * has been retrieved or the ResultSet has been closed. In advanced cases,
     * a single statement may return multiple results as well as output
     * parameter values. Here the commit occurs when all results and output
     * param values have been retrieved.
     *
     * @param value             true enables auto-commit; false disables
     *     auto-commit.
     * @exception SQLException  if a database-access error occurs.
     */
    public synchronized void setAutoCommit(boolean value) throws SQLException
    {
        checkClosed();
        autoCommit = value;
        changeSettings();
    }

    /**
     * Change the auto-commit and transaction isolation level values for all
     * the <code>Statement</code>s belonging to this <code>Connection</code>.
     * <p>
     * Note: This is not synchronized because it's only supposed to be called
     *       by synchronized methods.
     */
    private void changeSettings() throws SQLException
    {
        for( int i=0; i<allStatements.size(); i++ )
            ((TdsStatement)allStatements.elementAt(i))
                .changeSettings(autoCommit, transactionIsolationLevel);
    }

    /**
     * You can put a connection in read-only mode as a hint to enable database
     * optimizations <B>Note:</B> setReadOnly cannot be called while in the
     * middle of a transaction
     *
     * @param readOnly          true enables read-only mode; false disables it
     * @exception SQLException  if a database access error occurs
     */
    public void setReadOnly(boolean readOnly) throws SQLException
    {
    }

    /**
     * A sub-space of this Connection's database may be selected by setting a
     * catalog name. If the driver does not support catalogs it will silently
     * ignore this request.
     *
     * @param  catalog           The new Catalog value
     * @exception  SQLException  if a database-access error occurs.
     */
    public synchronized void setCatalog(String catalog) throws SQLException
    {
        if( database.equals(catalog) )
            return;

        /** @todo: Maybe find a smarter implementation for this */
        for( int i=0; i<tdsPool.size(); i++ )
        {
            Tds tds = ((TdsInstance)tdsPool.get(i)).tds;

            // SAfe We have to synchronize this so that the Statement doesn't
            //      begin sending data while we're changing the database. Not
            //      really safe though, since the Tds could be deallocated just
            //      before entering the monitor. Or not? Not too clear to me...
            synchronized( tds )
            {
                TdsStatement s = tds.statement;

                // SAfe A bit paranoic thinking here, but better to be sure it
                //      works. :o) Will synchronize again on the Tds if the
                //      Statement is null (otherwise it crashes with NPE).
                Object o = s==null ? (Object)tds : s;
                synchronized( o )
                {
                    if( s != null )
                        s.skipToEnd();
                    tds.changeDB(catalog);
                }
            }
        }

        database = catalog;
    }

    /**
     * You can call this method to try to change the transaction isolation
     * level using one of the TRANSACTION_* values. <P>
     *
     * <B>Note:</B> setTransactionIsolation cannot be called while in the
     * middle of a transaction.
     *
     * @param level             one of the TRANSACTION_* isolation values with
     *     the exception of TRANSACTION_NONE; some databases may not support
     *     other values
     * @exception SQLException  if a database-access error occurs.
     * @see                     DatabaseMetaData#supportsTransactionIsolationLevel
     */
    public synchronized void setTransactionIsolation(int level)
             throws SQLException
    {
        checkClosed();
        transactionIsolationLevel = level;
        changeSettings();
    }

    /**
     * JDBC 2.0 Installs the given type map as the type map for this
     * connection. The type map will be used for the custom mapping of SQL
     * structured types and distinct types.
     *
     * @param  map               The new TypeMap value
     * @exception  SQLException  Description of Exception
     */
    public void setTypeMap(java.util.Map map) throws SQLException
    {
        NotImplemented();
    }

    public String getUrl() throws SQLException
    {
        checkClosed();
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
        checkClosed();
        return autoCommit;
    }

    /**
     * Tests to see if a Connection is closed.
     *
     * @return                  true if the connection is closed; false if it's
     *     still open
     * @exception SQLException  if a database-access error occurs.
     */
    public boolean isClosed() throws SQLException
    {
        return isClosed;
    }

    /**
     * A connection's database is able to provide information describing its
     * tables, its supported SQL grammar, its stored procedures, the
     * capabilities of this connection, etc. This information is made available
     * through a DatabaseMetaData object.
     *
     * @return                   a DatabaseMetaData object for this connection
     * @exception  SQLException  if a database access error occurs
     */
    public synchronized java.sql.DatabaseMetaData getMetaData() throws SQLException
    {
        checkClosed();
        if( databaseMetaData == null )
            // SAfe: There is always one Tds in the connection pool and we
            // don't need exclusive access to it.
            databaseMetaData = DatabaseMetaData.getInstance(this,
                ((TdsInstance)tdsPool.get(0)).tds);
        return databaseMetaData;
    }

    /**
     * Tests to see if the connection is in read-only mode.
     *
     * @return                   true if connection is read-only
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean isReadOnly() throws SQLException
    {
        checkClosed();
        return false;
    }

    /**
     * Return the Connection's current catalog name.
     *
     * @return                   the current catalog name or null
     * @exception  SQLException  if a database-access error occurs.
     */
    public String getCatalog() throws SQLException
    {
        checkClosed();
        return database;
    }

    /**
     * Get this Connection's current transaction isolation mode.
     *
     * @return                   the current TRANSACTION_* mode value
     * @exception  SQLException  if a database-access error occurs.
     */
    public int getTransactionIsolation() throws SQLException
    {
        checkClosed();
        return transactionIsolationLevel;
    }

    /**
     * The first warning reported by calls on this Connection is returned. <P>
     *
     * <B>Note:</B> Subsequent warnings will be chained to this SQLWarning.
     *
     * @return                   the first SQLWarning or null
     * @exception  SQLException  if a database-access error occurs.
     */
    public synchronized SQLWarning getWarnings() throws SQLException
    {
        checkClosed();
        return warningChain.getWarnings();
    }

    /**
     * JDBC 2.0 Gets the type map object associated with this connection.
     * Unless the application has added an entry to the type map, the map
     * returned will be empty.
     *
     * @return                   the <code>java.util.Map</code> object associated
     *     with this <code>Connection</code> object
     * @exception  SQLException  Description of Exception
     */
    public java.util.Map getTypeMap() throws SQLException
    {
        return new java.util.HashMap();
    }

    public synchronized void markAsClosed(java.sql.Statement stmt)
    {
        allStatements.removeElement(stmt);
    }

    /**
     * SQL statements without parameters are normally executed using Statement
     * objects. If the same SQL statement is executed many times, it is more
     * efficient to use a PreparedStatement JDBC 2.0 Result sets created using
     * the returned Statement will have forward-only type, and read-only
     * concurrency, by default.
     *
     * @return                   a new Statement object
     * @exception  SQLException  passed through from the constructor
     */
    public synchronized java.sql.Statement createStatement() throws SQLException
    {
        checkClosed();
        Statement result = new TdsStatement(this);
        allStatements.addElement(result);
        return result;
    }

    /**
     * A SQL statement with or without IN parameters can be pre-compiled and
     * stored in a PreparedStatement object. This object can then be used to
     * efficiently execute this statement multiple times. <P>
     *
     * <B>Note:</B> This method is optimized for handling parametric SQL
     * statements that benefit from precompilation. If the driver supports
     * precompilation, prepareStatement will send the statement to the database
     * for precompilation. Some drivers may not support precompilation. In this
     * case, the statement may not be sent to the database until the
     * PreparedStatement is executed. This has no direct affect on users;
     * however, it does affect which method throws certain SQLExceptions. JDBC
     * 2.0 Result sets created using the returned PreparedStatement will have
     * forward-only type and read-only concurrency, by default.
     *
     * @param sql              a SQL statement that may contain one or more '?'
     *      IN parameter placeholders
     * @return                 a new PreparedStatement object containing the
     *      pre-compiled statement
     * @exception SQLException if a database-access error occurs.
     */
    public java.sql.PreparedStatement prepareStatement(String sql)
        throws SQLException
    {
        // No need for synchronized here, prepareStatement(String, int, int) is
        // synchronized
        return prepareStatement(sql,
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY);
    }

    /**
     * A SQL stored procedure call statement is handled by creating a
     * CallableStatement for it. The CallableStatement provides methods for
     * setting up its IN and OUT parameters and methods for executing it.
     * <P>
     * <B>Note:</B> This method is optimised for handling stored procedure call
     * statements. Some drivers may send the call statement to the database
     * when the prepareCall is done; others may wait until the
     * CallableStatement is executed. This has no direct effect on users;
     * however, it does affect which method throws certain SQLExceptions JDBC
     * 2.0 Result sets created using the returned CallableStatement will have
     * forward-only type and read-only concurrency, by default.
     *
     * @param sql              a SQL statement that may contain one or more '?'
     *     parameter placeholders. Typically this statement is a JDBC function
     *     call escape string.
     * @return                 a new CallableStatement object containing the
     *     pre-compiled SQL statement
     * @exception SQLException if a database access error occurs
     */
    public java.sql.CallableStatement prepareCall(String sql) throws SQLException
    {
        // No need for synchronized here, prepareCall(String, int, int) is
        // synchronized
        return prepareCall(sql,
            ResultSet.TYPE_FORWARD_ONLY,
            ResultSet.CONCUR_READ_ONLY);
    }

    /**
     * A driver may convert the JDBC sql grammar into its system's native SQL
     * grammar prior to sending it; nativeSQL returns the native form of the
     * statement that the driver would have sent.
     *
     * @param sql              a SQL statement that may contain one or more '?'
     *     parameter placeholders
     * @return                 the native form of this statement
     * @exception SQLException if a database access error occurs
     */
    public String nativeSQL(String sql) throws SQLException
    {
        return Tds.toNativeSql(sql, serverType);
    }

    /**
     * Commit makes all changes made since the previous commit/rollback
     * permanent and releases any database locks currently held by the
     * Connection. This method should only be used when auto commit has been
     * disabled.
     *
     * @exception  SQLException  if a database-access error occurs.
     * @see                      #setAutoCommit
     */
    public synchronized void commit() throws SQLException
    {
        commitOrRollback(true);
    }

    /**
     * Rollback drops all changes made since the previous commit/rollback and
     * releases any database locks currently held by the Connection. This
     * method should only be used when auto commit has been disabled.
     *
     * @exception  SQLException  if a database-access error occurs.
     * @see                      #setAutoCommit
     */
    public synchronized void rollback() throws SQLException
    {
        commitOrRollback(false);
    }

    /**
     * In some cases, it is desirable to immediately release a Connection's
     * database and JDBC resources instead of waiting for them to be
     * automatically released; the close method provides this immediate
     * release. <P>
     *
     * <B>Note:</B> A Connection is automatically closed when it is garbage
     * collected. Certain fatal errors also result in a closed Connection.
     *
     * @exception  SQLException  if a database-access error occurs.
     */
    public synchronized void close() throws SQLException
    {
        int i;
        SQLException exception = null;

        // SAfe First close all Statements, to ensure nothing is left behind
        //      This is needed to ensure rollback below doesn't crash.
        // SAfe Need to do it backwards because when closing, Statements remove
        //      themselves from the list.
        for( i=allStatements.size()-1; i>=0; i-- )
            try
            {
                ((Statement)allStatements.elementAt(i)).close();
            }
            catch( SQLException ex )
            {
                // SAfe Add the old exceptions to the chain
                ex.setNextException(exception);
                exception = ex;
            }
        allStatements.clear();

        /*
        ** MJH Need to do roll back for manual commit connections.
        */
        for( i=0; i<tdsPool.size(); i++ )
            try
            {
                if( !autoCommit )
                    ((TdsInstance)tdsPool.elementAt(i)).tds.rollback(); // MJH
                ((TdsInstance)tdsPool.elementAt(i)).tds.close();
            }
            catch( SQLException ex )
            {
                // SAfe Add the old exceptions to the chain
                ex.setNextException(exception);
                exception = ex;
            }
        tdsPool.clear();

        clearWarnings();
        isClosed = true;

        if( exception != null )
            throw exception;
    }

    /**
     * After this call, getWarnings returns null until a new warning is
     * reported for this connection.
     *
     * @exception  SQLException  if a database access error occurs
     */
    public synchronized void clearWarnings() throws SQLException
    {
        checkClosed();
        warningChain.clearWarnings();
    }

    //--------------------------JDBC 2.0-----------------------------

    /**
     * JDBC 2.0 Creates a <code>Statement</code> object that will generate
     * <code>ResultSet</code> objects with the given type and concurrency. This
     * method is the same as the <code>createStatement</code> method above, but
     * it allows the default result set type and result set concurrency type to
     * be overridden.
     *
     * @param resultSetType        a result set type; see ResultSet.TYPE_XXX
     * @param resultSetConcurrency a concurrency type; see ResultSet.CONCUR_XXX
     * @return                     a new Statement object
     * @exception SQLException     if a database access error occurs
     */
    public synchronized java.sql.Statement createStatement(int type, int concurrency)
        throws SQLException
    {
        checkClosed();
        Statement result = new TdsStatement(this, type, concurrency);
        allStatements.addElement(result);
        return result;
    }

    /**
     * JDBC 2.0 Creates a <code>PreparedStatement</code> object that will
     * generate <code>ResultSet</code> objects with the given type and
     * concurrency. This method is the same as the <code>prepareStatement
     * </code>method above, but it allows the default result set type and
     * result set concurrency type to be overridden.
     *
     * @param resultSetType        a result set type; see ResultSet.TYPE_XXX
     * @param resultSetConcurrency a concurrency type; see ResultSet.CONCUR_XXX
     * @param sql                  Description of Parameter
     * @return                     a new PreparedStatement object containing
     *     the pre-compiled SQL statement
     * @exception SQLException     if a database access error occurs
     */
    public synchronized java.sql.PreparedStatement prepareStatement(
        String sql,
        int resultSetType,
        int resultSetConcurrency)
        throws SQLException
    {
        checkClosed();
        java.sql.PreparedStatement result = new PreparedStatement_base(
            this, sql, resultSetType, resultSetConcurrency);
        allStatements.addElement(result);
        return result;
    }

    /**
     * JDBC 2.0 Creates a <code>CallableStatement</code> object that will
     * generate <code>ResultSet</code> objects with the given type and
     * concurrency. This method is the same as the <code>prepareCall</code>
     * method above, but it allows the default result set type and result set
     * concurrency type to be overridden.
     *
     * @param resultSetType        a result set type; see ResultSet.TYPE_XXX
     * @param resultSetConcurrency a concurrency type; see ResultSet.CONCUR_XXX
     * @param sql                  Description of Parameter
     * @return                     a new CallableStatement object containing
     *     the pre-compiled SQL statement
     * @exception SQLException     if a database access error occurs
     */
    public synchronized java.sql.CallableStatement prepareCall(
        String sql,
        int resultSetType,
        int resultSetConcurrency) throws SQLException
    {
        checkClosed();
        java.sql.CallableStatement result = new CallableStatement_base(
            this, sql, resultSetType, resultSetConcurrency);
        allStatements.addElement(result);
        return result;
    }

    private void NotImplemented() throws java.sql.SQLException
    {
        throw new java.sql.SQLException("Not Implemented");
    }

    private void checkClosed() throws SQLException
    {
        if( isClosed )
            throw new java.sql.SQLException("Connection closed");
    }

    /**
     * Allocate a tds instance to the calling thread. <br>
     * The routine tries to reuse an available tds instance. If there are no
     * tds instances that aren't in use it will create a new instance.
     *
     * @return                 A Tds instance to use for database
     *     communications.
     * @exception SQLException
     */
    synchronized Tds allocateTds() throws java.sql.SQLException
    {
        Tds result;
        int i;

        try {
            i = findAnAvailableTds();
            if (i == -1) {
                Tds tmpTds = null;
                try {
                    tmpTds = new Tds(this, initialProps);
                }
                catch (SQLException e) {
                    throw new SQLException(e.getMessage() + "\n" + tdsPool.size()
                             + " connections are in use by the driver");
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

            result.changeSettings(autoCommit, transactionIsolationLevel);
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
     * Find a <code>Tds</code> in the <code>TdsPool</code> that is not in use.
     * <p>
     * Note: This is not synchronized because it's only supposed to be called
     *       by synchronized methods.
     *
     * @return -1 if none was found, otherwise return the index of a free tds
     */
    private int findAnAvailableTds()
    {
        int i;

        for( i=tdsPool.size()-1;
            i>=0 && ((TdsInstance)tdsPool.elementAt(i)).inUse; i-- );

        return i;
    }


    /**
     * Return a <code>Tds</code> instance back to the <code>Tds</code> pool for
     * reuse.
     * <p>
     * Note: This is not synchronized because it's only supposed to be called
     *       by synchronized methods.
     *
     * @param  tds               Description of Parameter
     * @exception  TdsException  Description of Exception
     * @see                      #allocateTds
     */
    void freeTds(Tds tds) throws TdsException
    {
        int i;

        i = -1;
        do {
            i++;
        } while( i<tdsPool.size()
                 && tds!=((TdsInstance)tdsPool.elementAt(i)).tds );

        if( i < tdsPool.size() )
        {
            ((TdsInstance)tdsPool.elementAt(i)).inUse = false;
            tds.setStatement(null);

            // XXX Should also send a cancel to the server and throw out any
            // data that has already been sent.
        }
        else
            throw new TdsException("Tried to free a tds that wasn't in use");
    }

    /**
     * Implementation for both <code>commit()</code> and
     * <code>rollback()</code> methods.
     * <p>
     * Note: This is not synchronized because it's only supposed to be called
     *       by synchronized methods.
     *
     * @param commit if <code>true</code> commit, else rollback all executing
     *     <code>Statement</code>s
     */
    private void commitOrRollback(boolean commit) throws SQLException
    {
        int i;
        SQLException exception = null;

        // SAfe Completely wrong! If in manual commit mode, the transaction is
        //      not commited when going into auto commit. It has to be commited
        //      manually (even after the commit mode switch), otherwise it will
        //      be rolled back when the connection is closed.
//        if( autoCommit )
//            throw new SQLException("This method should only be " +
//                    " used when auto commit has been disabled.");

        // XXX race condition here.  It is possible that a statement could
        // close while running this for loop.
        // SAfe Release all Tds instances first.
        for( i=0; i<allStatements.size(); i++ )
        {
            TdsStatement stmt = (TdsStatement) allStatements.elementAt(i);
            try
            {
                // SAfe Need to consume all outstanding input...
                stmt.skipToEnd();
                // SAfe ...then release the Tds
                stmt.releaseTds();
            }
            catch( SQLException ex )
            {
                ex.setNextException(exception);
                exception = ex;
            }
        }

        // MJH Commit or Rollback Tds connections directly rather
        //     than mess with TdsStatement
        for( i=0; i<tdsPool.size(); i++ )
        {
            try
            {
                if( commit )
                    ((TdsInstance)tdsPool.elementAt(i)).tds.commit();
                else
                    ((TdsInstance)tdsPool.elementAt(i)).tds.rollback();
            }
            // XXX need to put all of these into the warning chain.
            //
            // Don't think so, the warnings would belong to Statement anyway -- SB
            catch( java.sql.SQLException e )
            {
                exception = e;
            }
        }

        if( exception != null )
            throw exception;
    }

    public java.sql.Statement createStatement(int param, int param1, int param2) throws java.sql.SQLException
    {
        throw new UnsupportedOperationException();
    }

    public int getHoldability() throws java.sql.SQLException
    {
        throw new UnsupportedOperationException();
    }

    public java.sql.CallableStatement prepareCall(String str, int param, int param2, int param3) throws java.sql.SQLException
    {
        throw new UnsupportedOperationException();
    }

    public java.sql.PreparedStatement prepareStatement(String str, int param) throws java.sql.SQLException
    {
        throw new UnsupportedOperationException();
    }

    public java.sql.PreparedStatement prepareStatement(String str, int[] values) throws java.sql.SQLException
    {
        throw new UnsupportedOperationException();
    }

    public java.sql.PreparedStatement prepareStatement(String str, String[] str1) throws java.sql.SQLException
    {
        throw new UnsupportedOperationException();
    }

    public java.sql.PreparedStatement prepareStatement(String str, int param, int param2, int param3) throws java.sql.SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void releaseSavepoint(java.sql.Savepoint savepoint) throws java.sql.SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void rollback(java.sql.Savepoint savepoint) throws java.sql.SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void setHoldability(int param) throws java.sql.SQLException
    {
        throw new UnsupportedOperationException();
    }

    public java.sql.Savepoint setSavepoint() throws java.sql.SQLException
    {
        throw new UnsupportedOperationException();
    }

    public java.sql.Savepoint setSavepoint(String str) throws java.sql.SQLException
    {
        throw new UnsupportedOperationException();
    }

}