// jTDS JDBC Driver for Microsoft SQL Server
// Copyright (C) 2004 The jTDS Project
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either
// version 2.1 of the License, or (at your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
//

package net.sourceforge.jtds.jdbc;

import java.sql.*;
import java.util.*;
import java.io.IOException;
import java.net.UnknownHostException;

import net.sourceforge.jtds.util.Logger;

/**
 * A Connection represents a session with a specific database. Within the
 * context of a Connection, SQL statements are executed and results are
 * returned. <P>
 *
 * A Connection's database is able to provide information describing its
 * tables, its supported SQL grammar, its stored procedures, the capabilities
 * of this connection, etc. This information is obtained with the getMetaData
 * method. <P>
 *
 * <B>Note:</B> By default the Connection automatically commits changes after
 * executing each statement. If auto commit has been disabled, an explicit
 * commit must be done or database changes will not be saved.
 *
 * @author     Craig Spannring
 * @author     Igor Petrovski
 * @author     Alin Sinpalean
 * @author     The FreeTDS project
 * @created    March 16, 2001
 * @version    $Id: TdsConnection.java,v 1.28 2004-05-02 22:45:21 bheineman Exp $
 * @see        Statement
 * @see        ResultSet
 * @see        DatabaseMetaData
 */
public class TdsConnection implements Connection
{
    private String host = null;
    // Can be either TdsDefinitions.SYBASE or TdsDefinitions.SQLSERVER
    private int serverType = -1;
    // Port numbers are _unsigned_ 16 bit, short is too small
    private int port = -1;
    private int tdsVer = -1;
    private String database = null;
    private boolean useUnicode;
    private byte maxPrecision = 38; // Sybase + MS SQL 2000 default

    private final Vector tdsPool = new Vector();
    private DatabaseMetaData databaseMetaData = null;

    private boolean autoCommit = true;
    private int transactionIsolationLevel = Connection.TRANSACTION_READ_COMMITTED;
    private boolean isClosed = false;
    /**
     * If set, only return the last update count. For example, if set update
     * counts from triggers will no longer be returned to the application.
     */
    private boolean lastUpdateCount;

    private TdsSocket tdsSocket = null;
    /**
     * The network packet size to use for output packets
     */
    private int networkPacketSize;

    private SQLWarningChain warningChain;

    // SAfe Access to both of these fields is synchronized on procedureCache
    private HashMap procedureCache = new HashMap();
    private ArrayList proceduresOfTra = new ArrayList();

    private EncodingHelper encoder = null;
    private String charset = null;
    /**
     * This is set if the user specifies an explicit charset, so that we'll ignore the server
     * charset.
     */
    private boolean charsetSpecified = false;

    private String databaseProductName;
    private String databaseProductVersion;
    private int databaseMajorVersion;
    private int databaseMinorVersion;

    /**
     * Object used to sync access to the main <code>Tds</code> instance.
     */
    final Object mainTdsMonitor = new Object();

    /**
     * CVS revision of the file.
     */
    public final static String cvsVersion = "$Id: TdsConnection.java,v 1.28 2004-05-02 22:45:21 bheineman Exp $";

    /**
     * Create a <code>Connection</code> to a database server.
     *
     * @param  props            <code>Properties</code> of the new
     *     <code>Connection</code> instance
     * @exception  SQLException  if a database access error occurs
     * @exception  TdsException  if a network protocol error occurs
     */
    public TdsConnection(Properties props)
            throws SQLException, TdsException {

        host = props.getProperty(Tds.PROP_SERVERNAME);
        serverType = Integer.parseInt(props.getProperty(Tds.PROP_SERVERTYPE));
        port = Integer.parseInt(props.getProperty(Tds.PROP_PORT));
        String user = props.getProperty(Tds.PROP_USER);
        String password = props.getProperty(Tds.PROP_PASSWORD);
        useUnicode = "true".equalsIgnoreCase(
                props.getProperty(Tds.PROP_USEUNICODE, "true"));
        // SAfe We don't know what database we'll get (probably master)
        database = null;

        String verString = props.getProperty(Tds.PROP_TDS, "7.0");
        if (verString.equals("5.0")) {
            // XXX This driver doesn't properly support TDS 5.0, AFAIK.
            // Added 2000-06-07.
            tdsVer = Tds.TDS50;
        } else if (verString.equals("4.2")) {
            tdsVer = Tds.TDS42;
        } else {
            tdsVer = Tds.TDS70;
        }

        warningChain = new SQLWarningChain();

        if (user == null) {
            user = props.getProperty(Tds.PROP_USER.toLowerCase());
            if (user == null) {
                throw new SQLException("Need a username.", "HY000");
            }
            props.put(Tds.PROP_USER, user);
        }

        if (password == null) {
            password = props.getProperty(Tds.PROP_PASSWORD.toLowerCase());
            if (password == null) {
                throw new SQLException("Need a password.", "HY000");
            }
            props.put(Tds.PROP_PASSWORD, password);
        }

        //mdb: get the instance port, if it is specified...
        final String instanceName = props.getProperty(Tds.PROP_INSTANCE, "");
        if (instanceName.length() > 0) {
            final MSSqlServerInfo info = new MSSqlServerInfo(host);
            port = info.getPortForInstance(instanceName);
            if (port == -1) {
                throw new SQLException(
                        "Server " + host + " has no instance named " + instanceName,
                        "08003");
            }
        }
        //
        // Create and attach shared network socket
        //
        TdsSocket.setMemoryBudget(100000); // Max memory for driver
        TdsSocket.setMinMemPkts(8); // Min packets to cache in RAM
        try {
            tdsSocket = new TdsSocket(host, port);
        } catch (UnknownHostException e) {
            throw new SQLException("Uknown server host name " + host, "08003");
        } catch (IOException e) {
            throw TdsUtil.getSQLException(null, "08004", e);
        }

        // Adellera
        setCharset(props.getProperty(Tds.PROP_CHARSET));

        networkPacketSize = (tdsVer >= Tds.TDS70) ? 4096 : 512;
        Tds tmpTds = new Tds(this, tdsSocket, tdsVer, serverType, useUnicode);
        tdsPool.addElement(tmpTds);
        try {
            try {
                tmpTds.logon(host,
                             props.getProperty(Tds.PROP_DBNAME),
                             user,
                             password,
                             props.getProperty(Tds.PROP_DOMAIN, ""),
                             charset,
                             props.getProperty(Tds.PROP_APPNAME, "jTDS"),
                             props.getProperty(Tds.PROP_PROGNAME, "jTDS"),
                             props.getProperty(Tds.PROP_MAC_ADDR));
            } catch (IOException e) {
                throw TdsUtil.getSQLException(null, "08S01", e);
            }
            final SQLWarningChain wChain = new SQLWarningChain();
            tmpTds.initSettings(props.getProperty(Tds.PROP_DBNAME), wChain);
            wChain.checkForExceptions();
        } catch (SQLException e) {
            throw TdsUtil.getSQLException("Logon failed", e.getSQLState(), e.getErrorCode(), e);
        }

        lastUpdateCount = "true".equalsIgnoreCase(
                props.getProperty(Tds.PROP_LAST_UPDATE_COUNT, "false"));

        if (serverType == Tds.SQLSERVER) {
            TdsStatement stmt = null;
            try {
                stmt = new TdsStatement(this);
                ResultSet rs = stmt.executeQuery("SELECT @@MAX_PRECISION");
                rs.next();
                maxPrecision = rs.getByte(1);
            } finally {
                if (stmt != null) {
                    stmt.close();
                }
            }
        }
    }

    public Procedure findCompatibleStoredProcedure(final String signature) {
        synchronized (procedureCache) {
            return (Procedure) procedureCache.get(signature);
        }
    }

    public void addStoredProcedure(final Procedure procedure)
            throws SQLException {
        synchronized (procedureCache) {
            // store the procedure in the procedureCache
            // MJH Use the signature (includes parameters)
            // rather than rawQueryString
            procedureCache.put(procedure.getSignature(), procedure);

            // MJH Only record the proc name in proceduresOfTra if in manual commit mode
            if (!this.autoCommit) {
                proceduresOfTra.add(procedure);
            }
        }
    }

    /**
     * Returns the version of TDS used (one of the TdsDefinitions.TDS<i>XX</i>
     * constants).
     */
    protected int getTdsVer() throws SQLException {
        checkClosed();
        return tdsVer;
    }

    /**
     * Returns the major version of the server software (e.g. 7 for SQL Server
     * 7.0).
     */
    protected int getServerVer() throws SQLException {
        checkClosed();
        return tdsVer;
    }

    /**
     * Returns the maximum precision of numeric and decimal values for this
     * connection.
     */
    protected byte getMaxPrecision() {
        return maxPrecision;
    }

    /**
     * Get the network packet size for request packets.
     *
     * @return The packet size as an <code>int</code>.
     */
    protected int getNetworkPacketSize()
    {
        return networkPacketSize;
    }

    /**
     * Set the network packet size for request packets.
     * @param networkPacketSize The packet size eg 4096.
     */
    protected void setNetworkPacketSize(int networkPacketSize)
    {
        this.networkPacketSize = networkPacketSize;
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
    public synchronized void setAutoCommit(boolean value) throws SQLException {
        checkClosed();
        changeSettings(value, this.transactionIsolationLevel);
        this.autoCommit = value;
    }

    /**
     * Change the auto-commit and transaction isolation level values for all
     * the <code>Tds</code> instances of this <code>Connection</code>. It's
     * important to do this on all <code>Tds</code>s, because the javadoc for
     * <code>setAutoCommit</code> specifies that <i>&quot;if [the] method is
     * called during a transaction, the transaction is committed&quot;</i>.
     * <p>
     * Note: This is not synchronized because it's only supposed to be called
     *       by synchronized methods.
     */
    private void changeSettings(boolean autoCommit, int transactionIsolationLevel)
            throws SQLException
    {
        synchronized (mainTdsMonitor) {
            Tds tds = allocateTds(true);
            try {
                tds.changeSettings(autoCommit, transactionIsolationLevel);
            } finally {
                try {
                    freeTds(tds);
                } catch (TdsException e) {
                    throw TdsUtil.getSQLException(null, null, e);
                }
            }
        }
    }

    /**
     * You can put a connection in read-only mode as a hint to enable database
     * optimizations <B>Note:</B> setReadOnly cannot be called while in the
     * middle of a transaction
     *
     * @param readOnly          true enables read-only mode; false disables it
     * @exception SQLException  if a database access error occurs
     */
    public void setReadOnly(boolean readOnly) throws SQLException {
    }

    /**
     * A sub-space of this Connection's database may be selected by setting a
     * catalog name. If the driver does not support catalogs it will silently
     * ignore this request.
     *
     * @param  catalog           The new Catalog value
     * @exception  SQLException  if a database-access error occurs.
     */
    public synchronized void setCatalog(String catalog) throws SQLException {
        if (database.equals(catalog))
            return;

        synchronized (mainTdsMonitor) {
            Tds tds = allocateTds(true);
            try {
                tds.skipToEnd();
                tds.changeDB(catalog, warningChain);
            } catch (SQLException e) {
                warningChain.addException(e);
            } finally {
                try {
                    freeTds(tds);
                } catch (TdsException e) {
                    warningChain.addException(TdsUtil.getSQLException(null, null, e));
                }
            }
        }

        warningChain.checkForExceptions();
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
            throws SQLException {
        checkClosed();
        changeSettings(this.autoCommit, level);
        this.transactionIsolationLevel = level;
    }

    /**
     * JDBC 2.0 Installs the given type map as the type map for this
     * connection. The type map will be used for the custom mapping of SQL
     * structured types and distinct types.
     *
     * @param  map               The new TypeMap value
     * @exception  SQLException  Description of Exception
     */
    public void setTypeMap(java.util.Map map) throws SQLException {
        NotImplemented("setTypeMap");
    }

    public String getUrl() throws SQLException {
        checkClosed();
        // XXX Is it legal to return something that might not be
        // exactly the URL used to connect?
        return
                ("jdbc:jtds:"
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
    public boolean getAutoCommit() throws SQLException {
        checkClosed();
        return autoCommit;
    }

    /**
     * Tests to see if a Connection is closed.
     *
     * @return    true if the connection is closed; false if it's still open
     * @exception SQLException  if a database-access error occurs.
     */
    public boolean isClosed() throws SQLException {
        return isClosed || !tdsSocket.isConnected();
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
    public synchronized java.sql.DatabaseMetaData getMetaData()
            throws SQLException {
        checkClosed();
        if (databaseMetaData == null)
        // SAfe: There is always one Tds in the connection pool and we
        // don't need exclusive access to it.
            databaseMetaData = DatabaseMetaData.getInstance(
                    this, ((Tds) tdsPool.get(0)));
        return databaseMetaData;
    }

    /**
     * Tests to see if the connection is in read-only mode.
     *
     * @return                   true if connection is read-only
     * @exception  SQLException  if a database-access error occurs.
     */
    public boolean isReadOnly() throws SQLException {
        checkClosed();
        return false;
    }

    /**
     * Return the Connection's current catalog name.
     *
     * @return                   the current catalog name or null
     * @exception  SQLException  if a database-access error occurs.
     */
    public String getCatalog() throws SQLException {
        checkClosed();
        return database;
    }

    /**
     * Get this Connection's current transaction isolation mode.
     *
     * @return                   the current TRANSACTION_* mode value
     * @exception  SQLException  if a database-access error occurs.
     */
    public int getTransactionIsolation() throws SQLException {
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
    public synchronized SQLWarning getWarnings() throws SQLException {
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
    public java.util.Map getTypeMap() throws SQLException {
        return new java.util.HashMap();
    }

    /**
     * Retrieves whether <code>executeUpdate()</code> should only return the
     * last update count. This is useful in the case of triggers, which can
     * generate additional (unexpected) update counts.
     *
     * @return the value of <code>lastUpdateCount</code>
     */
    public boolean returnLastUpdateCount() {
        return lastUpdateCount;
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
    public synchronized Statement createStatement() throws SQLException {
        checkClosed();
        return new TdsStatement(this);
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
    public PreparedStatement prepareStatement(String sql)
            throws SQLException {
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
    public CallableStatement prepareCall(String sql) throws SQLException {
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
     * @param sql a SQL statement that may contain one or more '?' parameter
     *            placeholders
     * @return    the native form of this statement
     * @throws SQLException if a database access error occurs
     */
    public String nativeSQL(String sql) throws SQLException {
        return EscapeProcessor.nativeSQL(sql);
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
    public synchronized void commit() throws SQLException {
        commitOrRollback(true);
        synchronized (procedureCache) {
            proceduresOfTra.clear();
        }
    }

    /**
     * Rollback drops all changes made since the previous commit/rollback and
     * releases any database locks currently held by the Connection. This
     * method should only be used when auto commit has been disabled.
     *
     * @exception  SQLException  if a database-access error occurs.
     * @see                      #setAutoCommit
     */
    public synchronized void rollback() throws SQLException {
        commitOrRollback(false);
        synchronized (procedureCache) {
            // SAfe No need to reinstate procedures. This ONLY leads to performance
            //      benefits if the statement is reused. The overhead is the same
            //      (plus some memory that's freed and reallocated).
            final Iterator it = proceduresOfTra.iterator();
            while (it.hasNext()) {
                final Procedure p = (Procedure) it.next();
                // MJH Use signature (includes parameters)
                // rather than rawSqlQueryString
                procedureCache.remove(p.getSignature());
            }
            proceduresOfTra.clear();
        }
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
    public synchronized void close() throws SQLException {
        if (isClosed()) {
            return;
        }

        // MJH Need to rollback if in manual commit mode
        synchronized (mainTdsMonitor) {
            Tds tds = allocateTds(true);
            try {
                tds.skipToEnd();
                if (!autoCommit)
                    tds.rollback(); // MJH
                tds.close();
            } catch (SQLException e) {
                warningChain.addException(e);
            } finally {
                try {
                    freeTds(tds);
                } catch (TdsException e) {
                    warningChain.addException(TdsUtil.getSQLException(null, null, e));
                }
            }
        }

        // Close the other Tds instances
        for (int i = 1; i < tdsPool.size(); i++ ) {
            ((Tds) tdsPool.get(i)).close();
        }

        tdsPool.clear();

        warningChain.clearWarnings();
        isClosed = true;
        // Close the physical socket
        //
        // SAfe Seems to me like it's already closed by Tds.close()
        try {
            tdsSocket.close();
        } catch (IOException e) {
            warningChain.addException(TdsUtil.getSQLException(null, "01002", e));
        }

        warningChain.checkForExceptions();
    }

    /**
     * After this call, getWarnings returns null until a new warning is
     * reported for this connection.
     *
     * @exception  SQLException  if a database access error occurs
     */
    public synchronized void clearWarnings() throws SQLException {
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
     * @param type        a result set type; see ResultSet.TYPE_XXX
     * @param concurrency a concurrency type; see ResultSet.CONCUR_XXX
     * @return            a new Statement object
     * @exception SQLException if a database access error occurs
     */
    public synchronized Statement createStatement(int type, int concurrency)
            throws SQLException {
        checkClosed();
        return new TdsStatement(this, type, concurrency);
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
    public synchronized PreparedStatement prepareStatement(
            String sql,
            int resultSetType,
            int resultSetConcurrency)
            throws SQLException {
        checkClosed();
        return new PreparedStatement_base(
                this, sql, resultSetType, resultSetConcurrency);
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
    public synchronized CallableStatement prepareCall(
            String sql,
            int resultSetType,
            int resultSetConcurrency) throws SQLException {
        checkClosed();
        return new CallableStatement_base(
                this, sql, resultSetType, resultSetConcurrency);
    }

    private void NotImplemented(String method) throws SQLException {
        throw new SQLException(
                "Method not Implemented: Connection." + method, "HY000");
    }

    /*package*/ void checkClosed() throws SQLException {
        if (isClosed()) {
            throw new SQLException("Connection closed", "HY000");
        }
    }

    /**
     * Allocate a tds instance to the calling thread. <br>
     * The routine tries to reuse an available tds instance. If there are no
     * tds instances that aren't in use it will create a new instance.
     *
     * @return    A Tds instance to use for database communications.
     * @exception SQLException
     */
    synchronized Tds allocateTds(boolean mainTds) throws SQLException {
        Tds result;
        int i;

        if (mainTds) {
            i = 0;
        } else {
            i = findAnAvailableTds();
            if (i == -1) {
                Tds tmpTds = new Tds(this, tdsSocket, tdsVer, serverType,
                                     useUnicode);
                tdsPool.addElement(tmpTds);

                i = findAnAvailableTds();
            }

            if (i == -1)
                throw new SQLException("Internal Error. Couldn't get Tds instance.", "HY000");
        }

        result = (Tds) tdsPool.elementAt(i);

        // This also means that i==0 and haveMainTds==true
        if (mainTds) {
            synchronized (result) {
                // SAfe Do nothing, just wait for it to be released (if it's in use).
            }
        }
        if (result.isInUse()) {
            throw new SQLException("Internal Error. Tds " + i + " is already allocated.", "HY000");
        }

        result.setInUse(true);

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
    private int findAnAvailableTds() {
        int i;

        for (i = tdsPool.size() - 1;
             i >= 1 && ((Tds) tdsPool.elementAt(i)).isInUse();
             i--);

        return (i == 0) ? -1 : i;
    }

    /**
     * Return a <code>Tds</code> instance back to the <code>Tds</code> pool for reuse.
     *
     * @param  tds               Description of Parameter
     * @exception  TdsException  Description of Exception
     * @see                      #allocateTds
     */
    synchronized void freeTds(Tds tds) throws TdsException, SQLException {
        int i;

        for (i = tdsPool.size() - 1; i >= 0; i-- ) {
            if (tds == (Tds)tdsPool.elementAt(i)) {
                break;
            }
        }

        if (i >= 0 && tds.isInUse()) {
            tds.setInUse(false);
        } else {
            // Only throw the exception if the connection is not closed
            if (!isClosed()) {
                throw new TdsException(
                        "Tried to free a tds that wasn't in use.");
            }
        }
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
    private void commitOrRollback(boolean commit) throws SQLException {
        checkClosed();

        // @todo SAfe Check when must the warnings actually be cleared
        warningChain.clearWarnings();

        // MJH  Commit or Rollback Tds connections directly rather than mess
        //      with TdsStatement
        synchronized (mainTdsMonitor) {
            Tds tds = allocateTds(true);

            try {
                if (commit) {
                    tds.commit();
                } else {
                    tds.rollback();
                }
            } catch (SQLException e) {
                warningChain.addException(e);
            } finally {
                try {
                    freeTds(tds);
                } catch (TdsException e) {
                    warningChain.addException(TdsUtil.getSQLException(null, null, e));
                }

                clearSavepoints();
            }
        }

        warningChain.checkForExceptions();
    }

    public Statement createStatement(int param, int param1, int param2)
            throws SQLException {
        NotImplemented("createStatement");
        return null;
    }

    public int getHoldability() throws SQLException {
        NotImplemented("getHoldability");
        return 0;
    }

    public CallableStatement prepareCall(
            String str, int param, int param2, int param3)
            throws SQLException {
        NotImplemented("prepareCall(String, int, int, int)");
        return null;
    }

    //
    // MJH - Add option to request return of generated keys.
    //
    public PreparedStatement prepareStatement(String sql, int param) throws SQLException {
        checkClosed();

        boolean returnKeys = false;

        if (param == TdsStatement.RETURN_GENERATED_KEYS
                && sql.trim().substring(0, 6).equalsIgnoreCase("INSERT")) {
            StringBuffer tmpSQL = new StringBuffer(sql);

            if (serverType == Tds.SQLSERVER && databaseMajorVersion >= 8) {
                tmpSQL.append("\r\nSELECT SCOPE_IDENTITY() AS ID");
            } else {
                tmpSQL.append("\r\nSELECT @@IDENTITY AS ID");
            }

            sql = tmpSQL.toString();
            returnKeys = true;
        }

        PreparedStatement_base stmt = new PreparedStatement_base(this, sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        stmt.statementReturnsKeys(returnKeys);

        return stmt;
    }

    //
    // MJH - Add option to request return of generated keys.
    // NB. SQL Server only allows one IDENTITY column per table so
    // cheat here and don't process the column parameter in detail.
    //
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
            throws SQLException {
        if (columnIndexes == null) {
            throw new SQLException("columnIndexes must not be null.", "HY024");
        } else  if (columnIndexes.length != 1) {
            throw new SQLException("One valid column index must be supplied.", "HY024");
        }

        return prepareStatement(sql, TdsStatement.RETURN_GENERATED_KEYS);
    }

    //
    // MJH - Add option to request return of generated keys.
    // NB. SQL Server only allows one IDENTITY column per table so
    // cheat here and don't process the column parameter in detail.
    //
    public PreparedStatement prepareStatement(String sql, String[] columnNames)
            throws SQLException {
        if (columnNames == null) {
            throw new SQLException("columnNames must not be null.", "HY024");
        } else if (columnNames.length != 1) {
            throw new SQLException("One valid column name must be supplied.", "HY024");
        }

        return prepareStatement(sql, TdsStatement.RETURN_GENERATED_KEYS);
    }

    public PreparedStatement prepareStatement(
            String str, int param, int param2, int param3)
            throws SQLException {
        NotImplemented("prepareStatement(String, int, int, int)");
        return null;
    }

    public void setHoldability(int param) throws SQLException {
        NotImplemented("setHoldability");
    }

    public void releaseSavepoint(java.sql.Savepoint savepoint)
            throws java.sql.SQLException {
        NotImplemented("releaseSavepoint");
    }

    public void rollback(Savepoint savepoint) throws SQLException {
        NotImplemented("rollback(java.sql.Savepoint)");
    }

    public Savepoint setSavepoint() throws SQLException {
        NotImplemented("setSavepoint");
        return null;
    }

    public Savepoint setSavepoint(String str) throws SQLException {
        NotImplemented("setSavepoint(String)");
        return null;
    }

    /**
     * Overridden by {@link TdsConnectionJDBC3} to implement release of all
     * savepoints on commit or rollback.
     */
    /*package*/ void clearSavepoints() {
    }

    protected void setCharset(String charset) {
        // If the user specified a charset, ignore environment changes
        if (encoder != null && charsetSpecified)
        {
            Logger.println("Server charset " + charset + ". Ignoring.");
            return;
        }

        // Empty string is equivalent to null (because of DataSource)
        charset = (charset != null && charset.length() == 0) ? null : charset;

        // If true at the end of the method, the specified charset was used.
        // Otherwise, iso_1 was.
        charsetSpecified = charset != null;

        if (charset == null || charset.length() > 30) {
            charset = "iso_1";
        }

        if (charset.toLowerCase().startsWith("cp")) {
            charset = "Cp" + charset.substring(2);
        }

        if (!charset.equals(this.charset)) {
            // SAfe Use an internal variable to avoid NPEs elsewhere
            EncodingHelper encoder = EncodingHelper.getHelper(charset);

            if (encoder == null) {
                if (Logger.isActive()) {
                    Logger.println("Invalid charset: " + charset + ". Trying iso_1 instead.");
                }

                charset = "iso_1";
                encoder = EncodingHelper.getHelper(charset);
                charsetSpecified = false;
            }

            this.encoder = encoder;
            this.charset = charset;
        }

        if (Logger.isActive()) {
            Logger.println("Set charset to " + charset + '/' + encoder.getName());
        }
    }

    public EncodingHelper getEncoder() {
        return encoder;
    }

    protected void setDBServerInfo(String databaseProductName,
                                   int databaseMajorVersion,
                                   int databaseMinorVersion,
                                   int buildNumber) {
        this.databaseProductName = databaseProductName;
        this.databaseMajorVersion = databaseMajorVersion;
        this.databaseMinorVersion = databaseMinorVersion;

        if (tdsVer == Tds.TDS70) {
            StringBuffer buf = new StringBuffer(10);
            if (databaseMajorVersion < 10) {
                buf.append('0');
            }
            buf.append(databaseMajorVersion).append('.');
            if (databaseMinorVersion < 10) {
                buf.append('0');
            }
            buf.append(databaseMinorVersion).append('.');
            buf.append(buildNumber);
            while (buf.length() < 10) {
                buf.insert(6, '0');
            }
            this.databaseProductVersion = buf.toString();
        } else {
            databaseProductVersion =
                    databaseMajorVersion + "." + databaseMinorVersion;
        }
    }

    /**
     * Return the name that this database server program calls itself.
     *
     * @return    The DatabaseProductName value
     */
    String getDatabaseProductName() {
        return databaseProductName;
    }

    /**
     * Return the version that this database server program identifies itself with.
     *
     * @return    The DatabaseProductVersion value
     */
    String getDatabaseProductVersion() {
        return databaseProductVersion;
    }

    /**
     * Return the major version that this database server program identifies itself with.
     *
     * @return    the <code>databaseMajorVersion</code> value
     */
    int getDatabaseMajorVersion() {
        return databaseMajorVersion;
    }

    /**
     * Return the minor version that this database server program identifies itself with.
     *
     * @return    the <code>databaseMinorVersion</code> value
     */
    int getDatabaseMinorVersion() {
        return databaseMinorVersion;
    }

    protected void databaseChanged(final String newDb, final String oldDb)
            throws TdsException {
        if (database != null && !oldDb.equalsIgnoreCase(database)) {
            throw new TdsException("Old database mismatch.");
        }

        database = newDb;
        if (Logger.isActive()) {
            Logger.println("Changed database to " + newDb);
        }
    }
}
