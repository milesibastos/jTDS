// jTDS JDBC Driver for Microsoft SQL Server and Sybase
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

import java.lang.ref.WeakReference;
import java.sql.CallableStatement;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Types;
import java.sql.ResultSet;
import java.net.UnknownHostException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import net.sourceforge.jtds.jdbc.cache.*;
import net.sourceforge.jtds.util.*;

/**
 * jTDS implementation of the java.sql.Connection interface.
 * <p>
 * Implementation notes:
 * <ol>
 * <li>Environment setting code carried over from old jTDS otherwise
 *     generally a new implementation of Connection.
 * <li>Connection properties and SQLException text messages are loaded from
 *     a properties file.
 * <li>Character set choices are also loaded from a resource file and the original
 *     Encoder class has gone.
 * <li>Prepared SQL statements are converted to procedures in the prepareSQL method.
 * <li>Use of Stored procedures is optional and controlled via connection property.
 * <li>This Connection object maintains a table of weak references to associated
 *     statements. This allows the connection object to control the statements (for
 *     example to close them) but without preventing them being garbage collected in
 *     a pooled environment.
 * </ol>
 *
 * @author Mike Hutchinson
 * @author Alin Sinpalean
 * @version $Id: ConnectionJDBC2.java,v 1.76 2005-03-18 11:46:45 alin_sinpalean Exp $
 */
public class ConnectionJDBC2 implements java.sql.Connection {
    /**
     * Class used to describe a cached stored procedure for prepared statements.
     */
    protected static class ProcEntry {
        /** The stored procedure name. */
        private String name;
        /** The column meta data (Sybase only). */
        private ColInfo[] colMetaData;
        /** The parameter meta data (Sybase only). */
        private ParamInfo[] paramMetaData;

        public final String toString() {
        	return name;
        }
    }

    /**
     * SQL query to determine the server charset on Sybase.
     */
    private static final String SYBASE_SERVER_CHARSET_QUERY
            = "select name from master.dbo.syscharsets where id ="
            + " (select value from master.dbo.sysconfigures where config=131)";

    /**
     * SQL query to determine the server charset on MS SQL Server 6.5.
     */
    private static final String SQL_SERVER_65_CHARSET_QUERY
            = "select name from master.dbo.syscharsets where id ="
            + " (select csid from master.dbo.syscharsets, master.dbo.sysconfigures"
            + " where config=1123 and id = value)";

    /** Sybase initial connection string. */
    private String SYBASE_INITIAL_SQL     = "SET TRANSACTION ISOLATION LEVEL 1\r\n" +
                                            "SET CHAINED OFF\r\n" +
                                            "SET QUOTED_IDENTIFIER ON\r\n"+
                                            "SET TEXTSIZE 2147483647";
    /**
     * SQL Server initial connection string. Also contains a <code>SELECT
     * @@MAX_PRECISION</code> query to retrieve the maximum precision for
     * DECIMAL/NUMERIC data. */
    private String SQL_SERVER_INITIAL_SQL = "SELECT @@MAX_PRECISION\r\n" +
                                            "SET TRANSACTION ISOLATION LEVEL READ COMMITTED\r\n" +
                                            "SET IMPLICIT_TRANSACTIONS OFF\r\n" +
                                            "SET QUOTED_IDENTIFIER ON\r\n"+
                                            "SET TEXTSIZE 2147483647";
    /*
     * Conection attributes
     */

    /** The orginal connection URL. */
    private String url;
    /** The server host name. */
    private String serverName;
    /** The server port number. */
    private int portNumber;
    /** The make of SQL Server (sybase/microsoft). */
    private int serverType;
    /** The SQL Server instance. */
    private String instanceName;
    /** The requested database name. */
    private String databaseName;
    /** The current database name. */
    private String currentDatabase;
    /** The Windows Domain name. */
    private String domainName;
    /** The database user ID. */
    private String user;
    /** The user password. */
    private String password;
    /** The server character set. */
    private String serverCharset;
    /** The application name. */
    private String appName;
    /** The program name. */
    private String progName;
    /** Workstation ID. */
    private String wsid;
    /** The server message language. */
    private String language;
    /** The client MAC Address. */
    private String macAddress;
    /** The server protocol version. */
    private int tdsVersion;
    /** The network TCP/IP socket. */
    private SharedSocket socket;
    /** The cored TDS protocol object. */
    private TdsCore baseTds;
    /** The initial network packet size. */
    private int netPacketSize = TdsCore.MIN_PKT_SIZE;
    /** User requested packet size. */
    private int packetSize;
    /** SQL Server 2000 collation. */
    private byte collation[];
    /** True if user specifies an explicit charset. */
    private boolean charsetSpecified = false;
    /** The database product name eg SQL SERVER. */
    private String databaseProductName;
    /** The product version eg 11.92. */
    private String databaseProductVersion;
    /** The major version number eg 11. */
    private int databaseMajorVersion;
    /** The minor version number eg 92. */
    private int databaseMinorVersion;
    /** True if this connection is closed. */
    private boolean closed = false;
    /** True if this connection is read only. */
    private boolean readOnly = false;
    /** List of statements associated with this connection. */
    private ArrayList statements;
    /** Default transaction isolation level. */
    private int transactionIsolation = java.sql.Connection.TRANSACTION_READ_COMMITTED;
    /** Default auto commit state. */
    private boolean autoCommit = true;
    /** Diagnostc messages for this connection. */
    private SQLDiagnostic messages;
    /** Connection's current rowcount limit. */
    private int rowCount = 0;
    /** Connection's current maximum field size limit. */
    private int textSize = 0;
    /** Maximum decimal precision. */
    private int maxPrecision = 38; // Sybase default
    /** Stored procedure unique ID number. */
    private int spSequenceNo = 1;
    /** Procedures in this transaction. */
    private ArrayList procInTran = new ArrayList();
    /** Java charset for encoding. */
    private CharsetInfo charsetInfo;
    /** Method for preparing SQL used in Prepared Statements. */
    private int prepareSql;
    /** The amount of LOB data to buffer in memory. */
    private long lobBuffer;
    /** The maximum number of statements to keep open. */
    private int maxStatements;
    /** Statement cache.*/
    private StatementCache statementCache;
    /** Send parameters as unicode. */
    private boolean useUnicode = true;
    /** Use named pipe IPC instead of TCP/IP sockets. */
    private boolean namedPipe = false;
    /** Only return the last update count. */
    private boolean lastUpdateCount = false;
    /** TCP_NODELAY */
    private boolean tcpNoDelay = true;
    /** Login timeout value in seconds or 0. */
    private int loginTimeout = 0;
    /** Sybase capability mask.*/
    private int sybaseInfo = 0;
    /** True if running distributed transaction. */
    private boolean xaTransaction = false;
    /** Current emulated XA State eg start/end/prepare etc. */
    private int xaState = 0;
    /** Current XA Transaction ID. */
    private Object xid;
    /** True if driver should emulate distributed transactions. */
    private boolean xaEmulation = true;
    /** Mutual exclusion lock to control access to connection. */
    private Semaphore mutex = new Semaphore(1);
    /** SSL setting. */
    private String ssl;
    /** The maximum size of a batch. */
    private int batchSize;

    /**
     * Default constructor.
     * <p/>
     * Used for testing.
     */
    private ConnectionJDBC2() {
    }

    /**
     * Create a new database connection.
     *
     * @param url The connection URL starting jdbc:jtds:.
     * @param info The additional connection properties.
     * @throws SQLException
     */
    ConnectionJDBC2(String url, Properties info)
            throws SQLException {
        this.statements = new ArrayList();
        this.url = url;
        //
        // Extract properties into instance variables
        //
        unpackProperties(info);
        this.messages = new SQLDiagnostic(serverType);
        //
        // Get the instance port, if it is specified.
        // Named pipes use instance names differently.
        //
        if (instanceName.length() > 0 && !namedPipe) {
            final MSSqlServerInfo msInfo = new MSSqlServerInfo(serverName);

            portNumber = msInfo.getPortForInstance(instanceName);

            if (portNumber == -1) {
                throw new SQLException(
                                      Messages.get("error.msinfo.badinst", serverName, instanceName),
                                      "08003");
            }
        }
        //
        // TODO These parameters should be set from the connection properties
        //
        SharedSocket.setMemoryBudget(100000);
        SharedSocket.setMinMemPkts(8);

        try {
            Object timer = null;
            if (loginTimeout > 0) {
                // Start a login timer
                timer = TimerThread.getInstance().setTimer(loginTimeout * 1000,
                        new TimerThread.TimerListener() {
                            public void timerExpired() {
                                if (socket != null) {
                                    socket.forceClose();
                                }
                            }
                        });
            }

            if (namedPipe == true) {
                // TODO Use namedPipe parameter to select implementation type
                if(System.getProperty("os.name").toLowerCase().startsWith("windows")) {
                    // If the OS is Windows, use a local named pipe
                    socket = new SharedLocalNamedPipe(serverName, tdsVersion, serverType,
                            instanceName);
                } else {
                    // Otherwise use a named pipe over TCP/IP
                    socket = SharedNamedPipe.instance(serverName, tdsVersion, serverType,
                            packetSize, instanceName, domainName, user, password);
                }
            } else {
                // Use plain TCP/IP socket
                socket = new SharedSocket(serverName, portNumber, tdsVersion,
                        serverType, tcpNoDelay, loginTimeout);
            }

            if (timer != null && TimerThread.getInstance().hasExpired(timer)) {
                // If the timer has expired during the connection phase, close
                // the socket and throw an exception
                socket.forceClose();
                throw new IOException("Login timed out");
            }

            if ( charsetSpecified ) {
                loadCharset(serverCharset);
            } else {
                // Need a default charset to process login packets for TDS 4.2/5.0
                // Will discover the actual serverCharset later
                loadCharset("iso_1");
                serverCharset = ""; // But don't send charset name to server!
            }

            //
            // Create TDS protocol object
            //
            baseTds = new TdsCore(this, messages);

            //
            // Negotiate SSL connection if required
            //
            if (tdsVersion >= Driver.TDS80 && !namedPipe) {
                baseTds.negotiateSSL(instanceName, ssl);
            }

            //
            // Now try and login
            //
            baseTds.login(serverName,
                          databaseName,
                          user,
                          password,
                          domainName,
                          serverCharset,
                          appName,
                          progName,
                          wsid,
                          language,
                          macAddress,
                          packetSize);

            if (timer != null) {
                // Cancel loginTimer
                TimerThread.getInstance().cancelTimer(timer);
            }

            // Update the tdsVersion with the value in baseTds. baseTds sets
            // the TDS version for the socket and there are no other objects
            // with cached TDS versions at this point.
            tdsVersion = baseTds.getTdsVersion();

            if (tdsVersion < Driver.TDS70 && databaseName.length() > 0) {
                // Need to select the default database
                setCatalog(databaseName);
            }
        } catch (UnknownHostException e) {
            throw Support.linkException(
                    new SQLException(Messages.get("error.connection.badhost",
                            e.getMessage()), "08S03"), e);
        } catch (IOException e) {
            if (loginTimeout > 0 && e.getMessage().indexOf("timed out") >= 0) {
                throw new SQLException(
                        Messages.get("error.connection.timeout"), "HYT01");
            }
            throw Support.linkException(
                    new SQLException(Messages.get("error.connection.ioerror",
                            e.getMessage()), "08S01"), e);
        } catch (SQLException e) {
            if (loginTimeout > 0 && e.getMessage().indexOf("socket closed") >= 0) {
                throw new SQLException(
                        Messages.get("error.connection.timeout"), "HYT01");
            }

            throw e;
        }

        // If charset is still unknown and the collation is not set either,
        // determine the charset by querying (we're using Sybase or SQL Server
        // 6.5)
        if ((serverCharset == null || serverCharset.length() == 0)
                && collation == null) {
            loadCharset(determineServerCharset());
        }

        // Initial database settings.
        // Sets: auto commit mode  = true
        //       transaction isolation = read committed.
        if (serverType == Driver.SYBASE) {
            baseTds.submitSQL(SYBASE_INITIAL_SQL);
        } else {
            // Also discover the maximum decimal precision (28 for MS SQL pre
            // 2000, configurable to 28/38 for 2000 and later)
            Statement stmt = this.createStatement();
            ResultSet rs = stmt.executeQuery(SQL_SERVER_INITIAL_SQL);

            if (rs.next()) {
                maxPrecision = rs.getByte(1);
            }

            rs.close();
            stmt.close();
        }
    }

    /**
     * Retrive the shared socket.
     *
     * @return The <code>SharedSocket</code> object.
     */
    SharedSocket getSocket() {
        return this.socket;
    }

    /**
     * Retrieve the TDS protocol version.
     *
     * @return The TDS version as an <code>int</code>.
     */
    int getTdsVersion() {
        return this.tdsVersion;
    }

    /**
     * Retrieve the next unique stored procedure name.
     * <p>Notes:
     * <ol>
     * <li>Some versions of Sybase require an id with
     * a length of &lt;= 10.
     * <li>The format of this name works for sybase and Microsoft
     * and allows for 16M names per session.
     * <li>The leading '#jtds' indicates this is a temporary procedure and
     * the '#' is removed by the lower level TDS5 routines.
     * </ol>
     * @return The sp name as a <code>String</code>.
     */
    String getProcName() {
        String seq = "000000" + Integer.toHexString(spSequenceNo++).toUpperCase();

        return "#jtds" + seq.substring(seq.length() - 6, seq.length());
    }

    /**
     * Try to convert the SQL statement into a stored procedure.
     * <p>
     * Synchronized because it accesses the procedure cache and the
     * <code>baseTds</code>, but the method call also needs to made in a
     * <code>synchronized (connection)</code> block together with the execution
     * (if the prepared statement is actually executed) to ensure the
     * transaction isn't rolled back between this method call and the actual
     * execution.
     *
     * @param sql    the SQL statement to prepare
     * @param params the parameters
     * @return the SQL procedure name as a <code>String</code> or null if the
     *         SQL cannot be prepared
     */
    synchronized String prepareSQL(JtdsPreparedStatement pstmt,
                                   String sql,
                                   ParamInfo[] params,
                                   boolean returnKeys)
            throws SQLException {
        if (prepareSql == TdsCore.UNPREPARED
                || prepareSql == TdsCore.EXECUTE_SQL) {
            return null; // User selected not to use procs
        }

        if (serverType == Driver.SYBASE) {
            if (tdsVersion != Driver.TDS50) {
                return null; // No longer support stored procs with 4.2
            }

            if (returnKeys) {
                return null; // Sybase cannot use @@IDENTITY in proc
            }

            if (pstmt.getResultSetConcurrency() == ResultSet.CONCUR_UPDATABLE
                || pstmt.getResultSetType() != ResultSet.TYPE_FORWARD_ONLY
                || pstmt.cursorName != null) {
                //
                // We are going to use the CachedResultSet so there is
                // no point in preparing the SQL as it will be discarded
                // in favour of a version with "FOR BROWSE" appended.
                //
                return null;
            }
        }

        //
        // Check parameters set and obtain native types
        //
        for (int i = 0; i < params.length; i++) {
            if (!params[i].isSet) {
                throw new SQLException(Messages.get("error.prepare.paramnotset",
                                                    Integer.toString(i+1)),
                                       "07000");
            }

            TdsData.getNativeType(this, params[i]);

            if (serverType == Driver.SYBASE) {
                if (params[i].sqlType.equals("text")
                    || params[i].sqlType.equals("image")) {
                    return null; // Sybase does not support text/image params
                }
            }
        }

        String key = Support.getStatementKey(sql, params, serverType, getCatalog());

        //
        // See if we have already built this one
        //
        ProcEntry proc = (ProcEntry) statementCache.get(key);

        if (proc != null) {
            if (serverType == Driver.SYBASE) {
                pstmt.setColMetaData(proc.colMetaData);
                pstmt.setParamMetaData(proc.paramMetaData);
            }

            return proc.name;
        }

        //
        // No, so create the stored procedure now
        //
        proc = new ProcEntry();

        if (serverType == Driver.SQLSERVER) {
            proc.name = baseTds.microsoftPrepare(sql, params,
                    pstmt.getResultSetType(), pstmt.getResultSetConcurrency());

            if (proc.name == null) {
                return null;
            }
            // TODO Find some way of getting parameter meta data for MS
        } else {
            proc.name = baseTds.sybasePrepare(sql, params);

            if (proc.name == null) {
                return null;
            }

            // Sybase gives us lots of useful information about the result set
            proc.colMetaData = baseTds.getColumns();
            proc.paramMetaData = baseTds.getParameters();
            pstmt.setColMetaData(proc.colMetaData);
            pstmt.setParamMetaData(proc.paramMetaData);
        }

        // OK we have built a proc so add it to the cache.
        addCachedProcedure(key, proc);

        // Add the handle to the prepared statement so that the handles
        // can be used to clean up the statement cache properly when the
        // prepared statement is closed.
        if (pstmt.handles == null) {
        	pstmt.handles = new ArrayList(1);
        }

        pstmt.handles.add(proc);

        // Give the user the name
        return proc.name;
    }

    /**
     * Add a stored procedure to the cache.
     * <p>
     * Not explicitly synchronized because it's only called by synchronized
     * methods.
     *
     * @param key The signature of the procedure to cache.
     * @param proc The stored procedure descriptor.
     */
    void addCachedProcedure(String key, ProcEntry proc) {
        statementCache.put(key, proc);

        if (!autoCommit) {
            procInTran.add(key);
        }
    }

    /**
     * Remove a stored procedure from the cache.
     * <p>
     * Not explicitly synchronized because it's only called by synchronized
     * methods.
     *
     * @param key The signature of the procedure to remove from the cache.
     */
    void removeCachedProcedure(String key) {
        statementCache.remove(key);

        if (!autoCommit) {
            procInTran.remove(key);
        }
    }

    /**
     * Retrieves the server type.
     *
     * @return the server type as an <code>int</code> where 1 == SQLSERVER and
     *         2 == SYBASE.
     */
    int getServerType() {
        return this.serverType;
    }

    /**
     * Sets the network packet size.
     *
     * @param size the new packet size
     */
    void setNetPacketSize(int size) {
        this.netPacketSize = size;
    }

    /**
     * Retrieves the network packet size.
     *
     * @return the packet size as an <code>int</code>
     */
    int getNetPacketSize() {
        return this.netPacketSize;
    }

    /**
     * Retrieves the current row count on this connection.
     *
     * @return the row count as an <code>int</code>
     */
    int getRowCount() {
        return this.rowCount;
    }

    /**
     * Sets the current row count on this connection.
     *
     * @param count the new row count
     */
    void setRowCount(int count) {
        rowCount = count;
    }

    /**
     * Retrieves the current maximum textsize on this connection.
     *
     * @return the maximum textsize as an <code>int</code>
     */
    public int getTextSize() {
        return textSize;
    }

    /**
     * Sets the current maximum textsize on this connection.
     *
     * @param textSize the new maximum textsize
     */
    public void setTextSize(int textSize) {
        this.textSize = textSize;
    }

    /**
     * Retrieves the status of the lastUpdateCount flag.
     *
     * @return the lastUpdateCount flag as a <code>boolean</code>
     */
    boolean isLastUpdateCount() {
        return this.lastUpdateCount;
    }

    /**
     * Retrieves the maximum decimal precision.
     *
     * @return the precision as an <code>int</code>
     */
    int getMaxPrecision() {
        return this.maxPrecision;
    }

    /**
     * Retrieves the LOB buffer size.
     *
     * @return the LOB buffer size as a <code>long</code>
     */
    long getLobBuffer() {
        return this.lobBuffer;
    }

    /**
     * Retrieves the Prepared SQL method.
     *
     * @return the Prepared SQL method
     */
    int getPrepareSql() {
        return this.prepareSql;
    }

    /**
     * Retrieves the batch size to be used internally.
     *
     * @return the batch size as an <code>int</code>
     */
    int getBatchSize() {
        return this.batchSize;
    }

    /**
     * Transfers the properties to the local instance variables.
     *
     * @param info The connection properties Object.
     * @throws SQLException If an invalid property value is found.
     */
    protected void unpackProperties(Properties info)
            throws SQLException {

        serverName = info.getProperty(Messages.get(Driver.SERVERNAME));
        portNumber = parseIntegerProperty(info, Driver.PORTNUMBER);
        serverType = parseIntegerProperty(info, Driver.SERVERTYPE);
        databaseName = info.getProperty(Messages.get(Driver.DATABASENAME));
        instanceName = info.getProperty(Messages.get(Driver.INSTANCE));
        domainName = info.getProperty(Messages.get(Driver.DOMAIN));
        user = info.getProperty(Messages.get(Driver.USER));
        password = info.getProperty(Messages.get(Driver.PASSWORD));
        macAddress = info.getProperty(Messages.get(Driver.MACADDRESS));
        appName = info.getProperty(Messages.get(Driver.APPNAME));
        progName = info.getProperty(Messages.get(Driver.PROGNAME));
        wsid = info.getProperty(Messages.get(Driver.WSID));
        serverCharset = info.getProperty(Messages.get(Driver.CHARSET));
        language = info.getProperty(Messages.get(Driver.LANGUAGE));
        lastUpdateCount = "true".equalsIgnoreCase(
                info.getProperty(Messages.get(Driver.LASTUPDATECOUNT)));
        useUnicode = "true".equalsIgnoreCase(
                info.getProperty(Messages.get(Driver.SENDSTRINGPARAMETERSASUNICODE)));
        namedPipe = "true".equalsIgnoreCase(
                info.getProperty(Messages.get(Driver.NAMEDPIPE)));
        tcpNoDelay = "true".equalsIgnoreCase(
                info.getProperty(Messages.get(Driver.TCPNODELAY)));
        xaEmulation = "true".equalsIgnoreCase(
                info.getProperty(Messages.get(Driver.XAEMULATION)));
        charsetSpecified = serverCharset.length() > 0;

        Integer parsedTdsVersion =
                DefaultProperties.getTdsVersion(info.getProperty(Messages.get(Driver.TDS)));
        if (parsedTdsVersion == null) {
            throw new SQLException(Messages.get("error.connection.badprop",
                    Messages.get(Driver.TDS)), "08001");
        }
        tdsVersion = parsedTdsVersion.intValue();

        packetSize = parseIntegerProperty(info, Driver.PACKETSIZE);
        if (packetSize < TdsCore.MIN_PKT_SIZE) {
            if (tdsVersion >= Driver.TDS70) {
                // Default of 0 means let the server specify packet size
                packetSize = (packetSize == 0) ? 0 : TdsCore.DEFAULT_MIN_PKT_SIZE_TDS70;
            } else {
                // Sensible minimum for all other versions of TDS
                packetSize = TdsCore.MIN_PKT_SIZE;
            }
        }
        if (packetSize > TdsCore.MAX_PKT_SIZE) {
            packetSize = TdsCore.MAX_PKT_SIZE;
        }
        packetSize = (packetSize / 512) * 512;

        loginTimeout = parseIntegerProperty(info, Driver.LOGINTIMEOUT);
        lobBuffer = parseLongProperty(info, Driver.LOBBUFFER);

        maxStatements = parseIntegerProperty(info, Driver.MAXSTATEMENTS);
        if (maxStatements <= 0) {
        	statementCache = new NonCachingStatementCache();
        } else if (maxStatements == Integer.MAX_VALUE) {
        	statementCache = new FastStatementCache();
        } else {
        	statementCache = new DefaultStatementCache(maxStatements);
        }

        prepareSql = parseIntegerProperty(info, Driver.PREPARESQL);
        // The TdsCore.PREPEXEC method is only available with TDS 8.0+ (SQL
        // Server 2000+); downgrade to TdsCore.PREPARE if an invalid option
        // is selected.
        if (tdsVersion < Driver.TDS80 && prepareSql == TdsCore.PREPEXEC) {
            prepareSql = TdsCore.PREPARE;
        }
        // For Sybase use equivalent of sp_executesql.
        if (tdsVersion < Driver.TDS70 && prepareSql == TdsCore.PREPARE) {
            prepareSql = TdsCore.EXECUTE_SQL;
        }
        // For SQL 6.5 sp_executesql not available so use stored procedures.
        if (tdsVersion < Driver.TDS50 && prepareSql == TdsCore.EXECUTE_SQL) {
            prepareSql = TdsCore.TEMPORARY_STORED_PROCEDURES;
        }

        ssl = info.getProperty(Messages.get(Driver.SSL));

        batchSize = parseIntegerProperty(info, Driver.BATCHSIZE);
        if (batchSize < 0) {
            throw new SQLException(Messages.get("error.connection.badprop",
                    Messages.get(Driver.BATCHSIZE)), "08001");
        }
    }

    /**
     * Parse a string property value into an integer value.
     *
     * @param info The connection properties object.
     * @param key The message key used to retrieve the property name.
     * @return The integer value of the string property value.
     * @throws SQLException If the property value can't be parsed.
     */
    private int parseIntegerProperty(final Properties info, final String key)
            throws SQLException {

        final String propertyName = Messages.get(key);
        try {
            return Integer.parseInt(info.getProperty(propertyName));
        } catch (NumberFormatException e) {
            throw new SQLException(
                    Messages.get("error.connection.badprop", propertyName), "08001");
        }
    }

    /**
     * Parse a string property value into a long value.
     *
     * @param info The connection properties object.
     * @param key The message key used to retrieve the property name.
     * @return The long value of the string property value.
     * @throws SQLException If the property value can't be parsed.
     */
    private long parseLongProperty(final Properties info, final String key)
            throws SQLException {

        final String propertyName = Messages.get(key);
        try {
            return Long.parseLong(info.getProperty(propertyName));
        } catch (NumberFormatException e) {
            throw new SQLException(
                    Messages.get("error.connection.badprop", propertyName), "08001");
        }
    }

    /**
     * Retrieve the Java charset to use for encoding.
     *
     * @return the Charset name as a <code>String</code>
     */
    protected String getCharset() {
        return charsetInfo.getCharset();
    }

    /**
     * Retrieve the multibyte status of the current character set.
     *
     * @return <code>boolean</code> true if a multi byte character set
     */
    protected boolean isWideChar() {
        return charsetInfo.isWideChars();
    }

    /**
     * Retrieve the <code>CharsetInfo</code> instance used by this connection.
     *
     * @return the default <code>CharsetInfo</code> for this connection
     */
    protected CharsetInfo getCharsetInfo() {
        return charsetInfo;
    }

    /**
     * Retrieve the sendParametersAsUnicode flag.
     *
     * @return <code>boolean</code> true if parameters should be sent as unicode.
     */
    protected boolean isUseUnicode() {
        return this.useUnicode;
    }

    /**
     * Retrieve the Sybase capability data.
     *
     * @return Capability bit mask as an <code>int</code>.
     */
    protected boolean getSybaseInfo(int flag) {
        return (this.sybaseInfo & flag) != 0;
    }

    /**
     * Set the Sybase capability data.
     *
     * @param mask The capability bit mask.
     */
    protected void setSybaseInfo(int mask) {
        this.sybaseInfo = mask;
    }

    /**
     * Called by the protocol to change the current character set.
     *
     * @param charset the server character set name
     */
    protected void setServerCharset(final String charset) throws SQLException {
        // If the user specified a charset, ignore environment changes
        if (charsetSpecified) {
            Logger.println("Server charset " + charset +
                    ". Ignoring as user requested " + serverCharset + '.');
            return;
        }

        if (!charset.equals(serverCharset)) {
            loadCharset(charset);

            if (Logger.isActive()) {
                Logger.println("Set charset to " + serverCharset + '/'
                        + charsetInfo);
            }
        }
    }

    /**
     * Load the Java charset to match the server character set.
     *
     * @param charset the server character set
     */
    private void loadCharset(String charset) throws SQLException {
        // Do not default to any charset; if the charset is not found we want
        // to know about it
        CharsetInfo tmp = CharsetInfo.getCharset(charset);

        if (tmp == null) {
            throw new SQLException(
                    Messages.get("error.charset.nomapping", charset), "2C000");
        }

        loadCharset(tmp, charset);
        serverCharset = charset;
    }

    /**
     * Load the Java charset to match the server character set.
     *
     * @param ci the <code>CharsetInfo</code> to load
     */
    private void loadCharset(CharsetInfo ci, String ref) throws SQLException {
        try {
            "This is a test".getBytes(ci.getCharset());

            charsetInfo = ci;
        } catch (UnsupportedEncodingException ex) {
            throw new SQLException(
                    Messages.get("error.charset.invalid", ref,
                            charsetInfo.getCharset()),
                    "2C000");
        }

        socket.setCharsetInfo(charsetInfo);
    }

    /**
     * Discovers the server charset for server versions that do not send
     * <code>ENVCHANGE</code> packets on login ack, by executing a DB
     * vendor/version specific query.
     * <p>
     * Will throw an <code>SQLException</code> if used on SQL Server 7.0 or
     * 2000; the idea is that the charset should already be determined from
     * <code>ENVCHANGE</code> packets for these DB servers.
     * <p>
     * Should only be called from the constructor.
     *
     * @return the default server charset
     * @throws SQLException if an error condition occurs
     */
    private String determineServerCharset() throws SQLException {
        String queryStr = null;

        switch (serverType) {
            case Driver.SQLSERVER:
                if (databaseProductVersion.indexOf("6.5") >= 0) {
                    queryStr = SQL_SERVER_65_CHARSET_QUERY;
                } else {
                    // This will never happen. Versions 7.0 and 2000 of SQL
                    // Server always send ENVCHANGE packets, even over TDS 4.2.
                    throw new SQLException(
                            "Please use TDS protocol version 7.0 or higher");
                }
                break;
            case Driver.SYBASE:
                // There's no need to check for versions here
                queryStr = SYBASE_SERVER_CHARSET_QUERY;
                break;
        }

        Statement stmt = this.createStatement();
        ResultSet rs = stmt.executeQuery(queryStr);
        rs.next();
        String charset = rs.getString(1);
        rs.close();
        stmt.close();

        return charset;
    }

    /**
     * Set the default collation for this connection.
     * <p>
     * Set by a SQL Server 2000 environment change packet. The collation
     * consists of the following fields:
     * <ul>
     * <li>bits 0-19  - The locale eg 0x0409 for US English which maps to code
     *                  page 1252 (Latin1_General).
     * <li>bits 20-31 - Reserved.
     * <li>bits 32-39 - Sort order (csid from syscharsets)
     * </ul>
     * If the sort order is non-zero it determines the character set, otherwise
     * the character set is determined by the locale id.
     *
     * @param collation The new collation.
     */
    void setCollation(byte[] collation) throws SQLException {
        String strCollation = "0x" + Support.toHex(collation);
        // If the user specified a charset, ignore environment changes
        if (charsetSpecified) {
            Logger.println("Server collation " + strCollation +
                    ". Ignoring as user requested " + serverCharset + '.');
            return;
        }

        CharsetInfo tmp = CharsetInfo.getCharset(collation);

        loadCharset(tmp, strCollation);
        this.collation = collation;

        if (Logger.isActive()) {
            Logger.println("Set collation to " + strCollation + '/'
                    + charsetInfo);
        }
    }

    /**
     * Retrieve the SQL Server 2000 default collation.
     *
     * @return The collation as a <code>byte[5]</code>.
     */
    byte[] getCollation() {
        return this.collation;
    }

    /**
     * Retrieves whether a specific charset was requested on creation. If this
     * is the case, all character data should be encoded/decoded using that
     * charset.
     */
    boolean isCharsetSpecified() {
        return charsetSpecified;
    }

    /**
     * Called by the protcol to change the current database context.
     *
     * @param newDb The new database selected on the server.
     * @param oldDb The old database as known by the server.
     * @throws SQLException
     */
    protected void setDatabase(final String newDb, final String oldDb)
            throws SQLException {
        if (currentDatabase != null && !oldDb.equalsIgnoreCase(currentDatabase)) {
            throw new SQLException(Messages.get("error.connection.dbmismatch",
                                                      oldDb, databaseName),
                                   "HY096");
        }

        currentDatabase = newDb;

        if (Logger.isActive()) {
            Logger.println("Changed database from " + oldDb + " to " + newDb);
        }
    }

    /**
     * Update the connection instance with information about the server.
     *
     * @param databaseProductName The server name eg SQL Server.
     * @param databaseMajorVersion The major version eg 11
     * @param databaseMinorVersion The minor version eg 92
     * @param buildNumber The server build number.
     */
    protected void setDBServerInfo(String databaseProductName,
                                   int databaseMajorVersion,
                                   int databaseMinorVersion,
                                   int buildNumber) {
        this.databaseProductName = databaseProductName;
        this.databaseMajorVersion = databaseMajorVersion;
        this.databaseMinorVersion = databaseMinorVersion;

        if (tdsVersion >= Driver.TDS70) {
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
     * Remove a statement object from the list maintained by the connection and
     * clean up the statement cache if necessary.
     * <p>
     * Synchronized because it accesses the statement list, the statement cache
     * and the <code>baseTds</code>.
     *
     * @param statement the statement to remove
     */
    synchronized void removeStatement(JtdsStatement statement)
            throws SQLException {
        // Remove the JtdsStatement
        for (int i = 0; i < statements.size(); i++) {
            WeakReference wr = (WeakReference) statements.get(i);

            if (wr != null) {
                Statement stmt = (Statement) wr.get();

                if (stmt != null && stmt == statement) {
                    statements.set(i, null);
                }
            }
        }

        if (statement instanceof JtdsPreparedStatement) {
            // Clean up the prepared statement cache
            Collection handles = statementCache.getObsoleteHandles(
                    ((JtdsPreparedStatement) statement).handles);

            if (handles != null) {
                StringBuffer cleanupSql = new StringBuffer(handles.size() * 32);

                for (Iterator iterator = handles.iterator(); iterator.hasNext(); ) {
                    String handle = iterator.next().toString();

                    // FIXME - Add support for sp_cursorunprepare
                    if (TdsCore.isPreparedProcedureName(handle)) {
                        cleanupSql.append("EXEC sp_unprepare ");
                    } else {
                        cleanupSql.append("DROP PROC ");
                    }

                    cleanupSql.append(handle);
                    cleanupSql.append('\n');
                }

                baseTds.executeSQL(cleanupSql.toString(), null, null, true, 0,
                        -1, -1, true);
                baseTds.clearResponseQueue();
            }
        }
    }

    /**
     * Add a statement object to the list maintained by the connection.
     * <p>WeakReferences are used so that statements can still be
     * closed and garbage collected even if not explicitly closed
     * by the connection.
     *
     * @param statement The statement to add.
     */
    void addStatement(JtdsStatement statement) {
        synchronized (statements) {
            for (int i = 0; i < statements.size(); i++) {
                WeakReference wr = (WeakReference) statements.get(i);

                if (wr == null) {
                    statements.set(i, new WeakReference(statement));
                    return;
                }
            }

            statements.add(new WeakReference(statement));
        }
    }

    /**
     * Check that this connection is still open.
     *
     * @throws SQLException if connection closed.
     */
    void checkOpen() throws SQLException {
        if (closed) {
            throw new SQLException(
                                  Messages.get("error.generic.closed", "Connection"), "HY010");
        }
    }

    /**
     * Check that this connection is in local transaction mode.
     *
     * @param method The method name being tested.
     * @throws SQLException if in XA distributed transaction mode
     */
    void checkLocal(String method) throws SQLException {
        if (xaTransaction) {
            throw new SQLException(
                    Messages.get("error.connection.badxaop", method), "HY010");
        }
    }

    /**
     * Report that user tried to call a method which has not been implemented.
     *
     * @param method The method name to report in the error message.
     * @throws SQLException
     */
    void notImplemented(String method) throws SQLException {
        throw new SQLException(
                              Messages.get("error.generic.notimp", method), "HYC00");
    }

    /**
     * Retrive the DBMS major version.
     *
     * @return The version as an <code>int</code>.
     */
    int getDatabaseMajorVersion() {
        return this.databaseMajorVersion;
    }

    /**
     * Retrive the DBMS minor version.
     *
     * @return The version as an <code>int</code>.
     */
    int getDatabaseMinorVersion() {
        return this.databaseMinorVersion;
    }

    /**
     * Retrive the DBMS product name.
     *
     * @return The name as a <code>String</code>.
     */
    String getDatabaseProductName() {
        return this.databaseProductName;
    }

    /**
     * Retrive the DBMS proeuct version.
     *
     * @return The version as a <code>String</code>.
     */
    String getDatabaseProductVersion() {
        return this.databaseProductVersion;
    }

    /**
     * Retrieve the original connection URL.
     *
     * @return The connection url as a <code>String</code>.
     */
    String getURL() {
        return this.url;
    }

    /**
     * Retrieve the host and port for this connection.
     * <p>
     * Used to identify same resource manager in XA transactions.
     *
     * @return the hostname and port as a <code>String</code>.
     */
    public String getRmHost()
    {
        return serverName + ":" + portNumber;
    }

    /**
     * Used to force the closed status on the statement if an
     * IO error has occurred.
     */
    void setClosed() {
        closed = true;

        // Make sure we release the socket and all data buffered at the socket
        // level
        try {
            socket.close();
        } catch (IOException e) {
            // Ignore; shouldn't happen anyway
        }
    }

    /**
     * Invoke the <code>xp_jtdsxa</code> extended stored procedure on the server.
     * <p>
     * Synchronized because it accesses the <code>baseTds</code>.
     *
     * @param args the arguments eg cmd, rmid, flags etc.
     * @param data option byte data eg open string xid etc.
     * @return optional byte data eg OLE cookie.
     * @throws SQLException if an error condition occurs
     */
    synchronized byte[][] sendXaPacket(int args[], byte[] data)
            throws SQLException
    {
        ParamInfo params[] = new ParamInfo[6];
        params[0] = new ParamInfo(Types.INTEGER, null, ParamInfo.RETVAL);
        params[1] = new ParamInfo(Types.INTEGER, new Integer(args[1]), ParamInfo.INPUT);
        params[2] = new ParamInfo(Types.INTEGER, new Integer(args[2]), ParamInfo.INPUT);
        params[3] = new ParamInfo(Types.INTEGER, new Integer(args[3]), ParamInfo.INPUT);
        params[4] = new ParamInfo(Types.INTEGER, new Integer(args[4]), ParamInfo.INPUT);
        params[5] = new ParamInfo(Types.VARBINARY, data, ParamInfo.OUTPUT);
        //
        // Execute our extended stored procedure (let's hope it is installed!).
        //
        baseTds.executeSQL(null, "master..xp_jtdsxa", params, false, 0, -1, -1,
                true);
        //
        // Now process results
        //
        ArrayList xids = new ArrayList();
        while (!baseTds.isEndOfResponse()) {
            if (baseTds.getMoreResults()) {
                // This had better be the results from a xa_recover command
                while (baseTds.getNextRow()) {
                    Object row[] = baseTds.getRowData();
                    if (row.length == 1 && row[0] instanceof byte[]) {
                        xids.add(row[0]);
                    }
                }
            }
        }
        messages.checkErrors();
        if (params[0].getOutValue() instanceof Integer) {
            // Should be return code from XA command
            args[0] = ((Integer)params[0].getOutValue()).intValue();
        } else {
            args[0] = -7; // XAException.XAER_RMFAIL
        }
        if (xids.size() > 0) {
            // List of XIDs from xa_recover
            byte list[][] = new byte[xids.size()][];
            for (int i = 0; i < xids.size(); i++) {
                list[i] = (byte[])xids.get(i);
            }
            return list;
        } else
        if (params[5].getOutValue() instanceof byte[]) {
            // xa_open  the xa connection ID
            // xa_start OLE Transaction cookie
            byte cookie[][] = new byte[1][];
            cookie[0] = (byte[])params[5].getOutValue();
            return cookie;
        } else {
            // All other cases
            return null;
        }
    }

    /**
     * Enlist the current connection in a distributed transaction.
     *
     * @param oleTranID the OLE transaction cookie or null to delist
     * @throws SQLException if an error condition occurs
     */
    synchronized void enlistConnection(byte[] oleTranID)
            throws SQLException
    {
        if (oleTranID != null) {
            // TODO: Stored procs are no good but maybe prepare will be OK.
            this.prepareSql = TdsCore.EXECUTE_SQL;
            baseTds.enlistConnection(1, oleTranID);
            xaTransaction = true;
        } else {
            baseTds.enlistConnection(1, null);
            xaTransaction = false;
        }
    }

    /**
     * Set the XA transaction ID when running in emulation mode.
     *
     * @param xid the XA Transaction ID
     */
    void setXid(Object xid) {
        this.xid = xid;
        xaTransaction = xid != null;
    }

    /**
     * Get the XA transaction ID when running in emulation mode.
     *
     * @return the transaction ID as an <code>Object</code>
     */
    Object getXid() {
        return xid;
    }

    /**
     * Set the XA state variable.
     *
     * @param value the XA state value
     */
    void setXaState(int value) {
        this.xaState = value;
    }

    /**
     * Retrieve the XA state variable.
     *
     * @return the xa state variable as an <code>int</code>
     */
    int getXaState() {
        return this.xaState;
    }

    /**
     * Retrieve the XA Emulation flag.
     * @return True if in XA emulation mode.
     */
    boolean isXaEmulation() {
        return xaEmulation;
    }

    /**
     * Retrieve the connection mutex.
     *
     * @return the mutex object as a <code>Semaphore</code>
     */
    Semaphore getMutex() {
        return this.mutex;
    }

    //
    // ------------------- java.sql.Connection interface methods -------------------
    //

    public int getHoldability() throws SQLException {
        checkOpen();

        return JtdsResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    public int getTransactionIsolation() throws SQLException {
        checkOpen();

        return this.transactionIsolation;
    }

    public void clearWarnings() throws SQLException {
        checkOpen();
        messages.clearWarnings();
    }

    /**
     * Releases this <code>Connection</code> object's database and JDBC
     * resources immediately instead of waiting for them to be automatically
     * released.
     * <p>
     * Calling the method close on a <code>Connection</code> object that is
     * already closed is a no-op.
     * <p>
     * <b>Note:</b> A <code>Connection</code> object is automatically closed
     * when it is garbage collected. Certain fatal errors also close a
     * <code>Connection</code> object.
     * <p>
     * Synchronized because it accesses the statement list and the
     * <code>baseTds</code>.
     *
     * @throws SQLException if a database access error occurs
     */
    synchronized public void close() throws SQLException {
        if (!closed) {
            try {
                //
                // Close any open statements
                //
                ArrayList tmpList;

                synchronized (statements) {
                    tmpList = new ArrayList(statements);
                }

                for (int i = 0; i < tmpList.size(); i++) {
                    WeakReference wr = (WeakReference)tmpList.get(i);

                    if (wr != null) {
                        Statement stmt = (Statement)wr.get();
                        if (stmt != null) {
                            stmt.close();
                        }
                    }
                }

                //
                // Tell the server the session is ending
                //
                baseTds.closeConnection();
                //
                // Close network connection
                //
                baseTds.close();
                socket.close();
            } catch (IOException e) {
                // Ignore
            } finally {
                closed = true;
            }
        }
    }

    synchronized public void commit() throws SQLException {
        checkOpen();
        checkLocal("commit");

        if (getAutoCommit()) {
            throw new SQLException(
                    Messages.get("error.connection.autocommit"), "25000");
        }

        baseTds.submitSQL("IF @@TRANCOUNT > 0 COMMIT TRAN");
        procInTran.clear();
        clearSavepoints();
    }

    synchronized public void rollback() throws SQLException {
        checkOpen();
        checkLocal("rollback");

        if (getAutoCommit()) {
            throw new SQLException(
                    Messages.get("error.connection.autocommit"), "25000");
        }

        baseTds.submitSQL("IF @@TRANCOUNT > 0 ROLLBACK TRAN");

        for (int i = 0; i < procInTran.size(); i++) {
            String key = (String) procInTran.get(i);
            if (key != null && tdsVersion != Driver.TDS50) {
                statementCache.remove(key);
            }
        }
        procInTran.clear();

        clearSavepoints();
    }

    public boolean getAutoCommit() throws SQLException {
        checkOpen();

        return this.autoCommit;
    }

    public boolean isClosed() throws SQLException {
        return closed;
    }

    public boolean isReadOnly() throws SQLException {
        checkOpen();

        return this.readOnly;
    }

    public void setHoldability(int holdability) throws SQLException {
        checkOpen();
        switch (holdability) {
            case JtdsResultSet.HOLD_CURSORS_OVER_COMMIT:
                break;
            case JtdsResultSet.CLOSE_CURSORS_AT_COMMIT:
                throw new SQLException(Messages.get("error.generic.optvalue",
                                                          "CLOSE_CURSORS_AT_COMMIT",
                                                          "setHoldability"), "HY092");
            default:
                throw new SQLException(Messages.get("error.generic.badoption",
                                                          Integer.toString(holdability),
                                                          "setHoldability"), "HY092");
        }
    }

    synchronized public void setTransactionIsolation(int level) throws SQLException {
        checkOpen();

        if (transactionIsolation == level) {
            // No need to submit a request
            return;
        }

        String sql = "SET TRANSACTION ISOLATION LEVEL ";
        boolean sybase = serverType == Driver.SYBASE;

        switch (level) {
            case java.sql.Connection.TRANSACTION_READ_UNCOMMITTED:
                sql += (sybase)? "0": "READ UNCOMMITTED";
                break;
            case java.sql.Connection.TRANSACTION_READ_COMMITTED:
                sql += (sybase)? "1": "READ COMMITTED";
                break;
            case java.sql.Connection.TRANSACTION_REPEATABLE_READ:
                sql += (sybase)? "2": "REPEATABLE READ";
                break;
            case java.sql.Connection.TRANSACTION_SERIALIZABLE:
                sql += (sybase)? "3": "SERIALIZABLE";
                break;
            case java.sql.Connection.TRANSACTION_NONE:
                throw new SQLException(Messages.get("error.generic.optvalue",
                                                          "TRANSACTION_NONE",
                                                          "setTransactionIsolation"), "HY024");
            default:
                throw new SQLException(Messages.get("error.generic.badoption",
                                                          Integer.toString(level),
                                                          "setTransactionIsolation"), "HY092");
        }

        transactionIsolation = level;
        baseTds.submitSQL(sql);
    }

    synchronized public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkOpen();
        checkLocal("setAutoCommit");

        if (!this.autoCommit) {
            // If we're in manual commit mode the spec requires that we commit
            // the transaction when setAutoCommit() is called
            commit();
        }

        if (this.autoCommit == autoCommit) {
            // If we don't need to change the current auto commit mode, don't
            // submit a request
            return;
        }

        String sql;

        if (serverType == Driver.SYBASE) {
            if (autoCommit) {
                sql = "SET CHAINED OFF";
            } else {
                sql = "SET CHAINED ON";
            }
        } else {
            if (autoCommit) {
                sql = "SET IMPLICIT_TRANSACTIONS OFF";
            } else {
                sql = "SET IMPLICIT_TRANSACTIONS ON";
            }
        }

        baseTds.submitSQL(sql);
        this.autoCommit = autoCommit;
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
        checkOpen();
        this.readOnly = readOnly;
    }

    public String getCatalog() throws SQLException {
        checkOpen();

        return this.currentDatabase;
    }

    synchronized public void setCatalog(String catalog) throws SQLException {
        checkOpen();

        if (currentDatabase != null && currentDatabase.equals(catalog)) {
            return;
        }

        if (catalog.length() > 32 || catalog.length() < 1) {
            throw new SQLException(
                                  Messages.get("error.generic.badparam",
                                                     catalog, "setCatalog"), "3D000");
        }

        String sql = tdsVersion >= Driver.TDS70 ?
                     ("use [" + catalog + ']') : "use " + catalog;
        baseTds.submitSQL(sql);
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        checkOpen();

        return new JtdsDatabaseMetaData(this);
    }

    public SQLWarning getWarnings() throws SQLException {
        checkOpen();

        return messages.getWarnings();
    }

    public Savepoint setSavepoint() throws SQLException {
        checkOpen();
        notImplemented("Connection.setSavepoint()");

        return null;
    }

    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        checkOpen();
        notImplemented("Connection.releaseSavepoint(Savepoint)");
    }

    public void rollback(Savepoint savepoint) throws SQLException {
        checkOpen();
        notImplemented("Connection.rollback(Savepoint)");
    }

    public Statement createStatement() throws SQLException {
        checkOpen();

        return createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,
                               java.sql.ResultSet.CONCUR_READ_ONLY);
    }

    public Statement createStatement(int type, int concurrency) throws SQLException {
        checkOpen();

        JtdsStatement stmt = new JtdsStatement(this, type, concurrency);
        addStatement(stmt);

        return stmt;
    }

    public Statement createStatement(int type, int concurrency, int holdability)
    throws SQLException {
        checkOpen();
        setHoldability(holdability);

        return createStatement(type, concurrency);
    }

    public Map getTypeMap() throws SQLException {
        checkOpen();

        return new HashMap();
    }

    public void setTypeMap(Map map) throws SQLException {
        checkOpen();
        notImplemented("Connection.setTypeMap(Map)");
    }

    public String nativeSQL(String sql) throws SQLException {
        checkOpen();

        if (sql == null || sql.length() == 0) {
            throw new SQLException(Messages.get("error.generic.nosql"), "HY000");
        }

        String[] result = new SQLParser(sql, new ArrayList(), this).parse(false);

        return result[0];
    }

    public CallableStatement prepareCall(String sql) throws SQLException {
        checkOpen();

        return prepareCall(sql,
                           java.sql.ResultSet.TYPE_FORWARD_ONLY,
                           java.sql.ResultSet.CONCUR_READ_ONLY);
    }

    public CallableStatement prepareCall(String sql, int type, int concurrency)
    throws SQLException {
        checkOpen();

        if (sql == null || sql.length() == 0) {
            throw new SQLException(Messages.get("error.generic.nosql"), "HY000");
        }

        JtdsCallableStatement stmt = new JtdsCallableStatement(this,
                                                               sql,
                                                               type,
                                                               concurrency);
        addStatement(stmt);

        return stmt;
    }

    public CallableStatement prepareCall(
                                        String sql,
                                        int type,
                                        int concurrency,
                                        int holdability)
    throws SQLException {
        checkOpen();
        setHoldability(holdability);
        return prepareCall(sql, type, concurrency);
    }

    public PreparedStatement prepareStatement(String sql)
    throws SQLException {
        checkOpen();

        return prepareStatement(sql,
                                java.sql.ResultSet.TYPE_FORWARD_ONLY,
                                java.sql.ResultSet.CONCUR_READ_ONLY);
    }

    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
    throws SQLException {
        checkOpen();

        if (sql == null || sql.length() == 0) {
            throw new SQLException(Messages.get("error.generic.nosql"), "HY000");
        }

        if (autoGeneratedKeys != JtdsStatement.RETURN_GENERATED_KEYS &&
            autoGeneratedKeys != JtdsStatement.NO_GENERATED_KEYS) {
            throw new SQLException(
                    Messages.get("error.generic.badoption",
                            Integer.toString(autoGeneratedKeys),
                            "executeUpdate"),
                    "HY092");
        }

        JtdsPreparedStatement stmt = new JtdsPreparedStatement(this,
                sql,
                java.sql.ResultSet.TYPE_FORWARD_ONLY,
                java.sql.ResultSet.CONCUR_READ_ONLY,
                autoGeneratedKeys == JtdsStatement.RETURN_GENERATED_KEYS);
        addStatement(stmt);

        return stmt;
    }

    public PreparedStatement prepareStatement(String sql, int type, int concurrency)
    throws SQLException {
        checkOpen();

        if (sql == null || sql.length() == 0) {
            throw new SQLException(Messages.get("error.generic.nosql"), "HY000");
        }

        JtdsPreparedStatement stmt = new JtdsPreparedStatement(this,
                                                               sql,
                                                               type,
                                                               concurrency,
                                                               false);
        addStatement(stmt);

        return stmt;
    }

    public PreparedStatement prepareStatement(
                                             String sql,
                                             int type,
                                             int concurrency,
                                             int holdability)
    throws SQLException {
        checkOpen();
        setHoldability(holdability);

        return prepareStatement(sql, type, concurrency);
    }

    public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
    throws SQLException {
        if (columnIndexes == null) {
            throw new SQLException(
                                  Messages.get("error.generic.nullparam", "prepareStatement"),"HY092");
        } else if (columnIndexes.length != 1) {
            throw new SQLException(
                                  Messages.get("error.generic.needcolindex", "prepareStatement"),"HY092");
        }

        return prepareStatement(sql, JtdsStatement.RETURN_GENERATED_KEYS);
    }

    public Savepoint setSavepoint(String name) throws SQLException {
        checkOpen();
        notImplemented("Connection.setSavepoint(String)");

        return null;
    }

    public PreparedStatement prepareStatement(String sql, String[] columnNames)
    throws SQLException {
        if (columnNames == null) {
            throw new SQLException(
                                  Messages.get("error.generic.nullparam", "prepareStatement"),"HY092");
        } else if (columnNames.length != 1) {
            throw new SQLException(
                                  Messages.get("error.generic.needcolname", "prepareStatement"),"HY092");
        }

        return prepareStatement(sql, JtdsStatement.RETURN_GENERATED_KEYS);
    }


    /**
     * Releases all savepoints. Used internally when committing or rolling back
     * a transaction.
     */
    void clearSavepoints() {
    }
}
