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

import java.sql.CallableStatement;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.Properties;
import java.net.UnknownHostException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.lang.ref.WeakReference;

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
 * @version $Id: ConnectionJDBC2.java,v 1.6 2004-07-22 17:09:57 bheineman Exp $
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
    }

    /**
     * Simple timer class used to implement login timeouts.
     * <p>When the timer expires the network socket is closed
     * crashing any pending network I/O. Not elegant but it works!
     */
    private static class LoginTimer extends Thread {
        private SharedSocket socket;
        private int timeout;
        private boolean exitNow = false;

        LoginTimer(SharedSocket socket, int timeout) {
            this.socket  = socket;
            this.timeout = timeout;
        }

        public void run() {
            while (!exitNow) {
                try {
                    sleep(timeout * 1000);
                    socket.forceClose();
                    return;
                } catch (java.lang.InterruptedException e) {
                    // nop
                }
            }
        }

        void stopTimer() {
            exitNow = true;
            this.interrupt();
        }
    }

    /*
     * Conection attributes
     */

    /** The orginal connection URL. */
    private String url;
    /** The server host name. */
    private String serverName;
    /** The server port number. */
    private int serverPort;
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
    /** The library name. */
    private String libName;
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
    /** Connections current rowcount limit. */
    private int rowCount = 0;
    /** Maximum decimal precision. */
    private int maxPrecision = 38; // Sybase default
    /** Stored procedure unique ID number. */
    private int spSequenceNo = 1;
    /** Procedure cache.*/
    private HashMap procedures = new HashMap();
    /** Procedures in this transaction. */
    private ArrayList procInTran = new ArrayList();
    /** Charset properties list. */
    private static Properties charsets = new Properties();
    /** Indicates current charset is wide. */
    private boolean wideChars = false;
    /** java charset for encoding. */
    private String javaCharset;
    /** Convert Prepared Statements to procs. */
    private boolean prepareSql = true;
    /** The amount of LOB data to buffer in memory. */
    private long lobBuffer;
    /** Send parameters as unicode. */
    private boolean useUnicode = true;
    /** Only return the last update count. */
    private boolean lastUpdateCount = false;
    /** Login timeout value in seconds or 0. */
    private int loginTimeout = 0;

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
        // get the instance port, if it is specified...
        //
        if (instanceName.length() > 0) {
            final MSSqlServerInfo msInfo = new MSSqlServerInfo(serverName);

            serverPort = msInfo.getPortForInstance(instanceName);

            if (serverPort == -1) {
                throw new SQLException(
                                      Support.getMessage("error.msinfo.badinst", serverName, instanceName),
                                      "08003");
            }
        }
        //
        // TODO These parameters should be set from the connection properties
        //
        SharedSocket.setMemoryBudget(100000);
        SharedSocket.setMinMemPkts(8);

        try {
            socket = new SharedSocket(serverName, serverPort, tdsVersion, serverType);
            loadCharset(serverCharset);
            //
            // Create TDS protocol object
            //
            baseTds = new TdsCore(this, messages);
            LoginTimer timer = null;

            if (loginTimeout > 0) {
                // Start a login timer
                timer = new LoginTimer(socket, loginTimeout);
                timer.start();
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
                          libName,
                          language,
                          macAddress,
                          packetSize);

            if (loginTimeout > 0) {
                // Cancel loginTimer
                timer.stopTimer();
            }

            if (tdsVersion < TdsCore.TDS70 && databaseName.length() > 0) {
                // Need to select the default database
                setCatalog(databaseName);
            }
        } catch (UnknownHostException e) {
            throw Support.linkException(
                                       new SQLException(
                                                       Support.getMessage(
                                                                         "error.connection.badhost", e.getMessage()),
                                                       "08S03"), e);
        } catch (IOException e) {
            throw Support.linkException(
                                       new SQLException(
                                                       Support.getMessage(
                                                                         "error.connection.ioerror", e.getMessage()),
                                                       "08S01"), e);
        } catch (SQLException e) {
            if (loginTimeout > 0 && e.getMessage().indexOf("socket closed") >= 0) {
                throw new SQLException(
                                      Support.getMessage(
                                                        "error.connection.timeout"),
                                      "08S01");
            }
            
            throw e;
        }

        // Discover the maximum decimal precision normal 28 for MS SQL < 2000
        if (serverType == TdsCore.SQLSERVER) {
            Statement stmt = this.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT @@MAX_PRECISION");

            if (rs.next()) {
                maxPrecision = rs.getByte(1);
            }

            rs.close();
            stmt.close();
        }

        //
        // Initial database settings
        //
        String sql;

        if (serverType == TdsCore.SYBASE) {
            sql = "SET QUOTED_IDENTIFIER ON SET TEXTSIZE 2147483647 ";
        } else {
            sql = "SET QUOTED_IDENTIFIER ON SET TEXTSIZE 2147483647 ";
        }

        baseTds.submitSQL(sql);
        setAutoCommit(true);
        setTransactionIsolation(transactionIsolation);
    }

    /**
     * Load the correct Charset to match the server character set.
     *
     * @param charset The Server character set.
     */
    protected void loadCharset(String charset) {
        synchronized (charsets) {
            if (charsets.size() == 0) {
                try {
                    InputStream stream;
                    // getContextClassLoader needed to ensure driver
                    // works with Tomcat class loading rules.
                    ClassLoader classLoader =
                    Thread.currentThread().getContextClassLoader();

                    if (classLoader == null) {
                        classLoader = getClass().getClassLoader();
                    }

                    stream = classLoader.getResourceAsStream("net/sourceforge/jtds/jdbc/Charsets.properties");

                    if (stream != null) {
                        charsets.load(stream);
                    } else {
                        Logger.println("Can't load charset.properties");
                    }
                } catch (IOException e) {
                    Logger.logException(e);
                    // Can't load properties file for some reason
                }
            }
        }

        String tmp = charsets.getProperty(charset.toUpperCase(), "1|ISO-8859-1");

        wideChars = !tmp.substring(0, 1).equals("1");
        javaCharset = tmp.substring(2);

        try {
            "This is a test".getBytes(charset);
        } catch (UnsupportedEncodingException e) {
            javaCharset = "ISO-8859-1"; // Fall back iso_1
            wideChars = false;
        }

        socket.setCharset(javaCharset);
        socket.setWideChars(wideChars);
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
        String name = "#jtds" + seq.substring(seq.length() - 6, seq.length());

        return name;
    }

    /**
     * Find a stored procedure in the cache.
     *
     * @param key The signature of the procedure.
     * @return The procedure name as a <code>String</code> or null.
     */
    ProcEntry getCachedProcedure(String key) {
        return(ProcEntry) procedures.get(key);
    }

    /**
     * Try to convert the SQL statement into a stored procedure.
     *
     * @param sql The SQL statement to prepare.
     * @param params The parameters.
     * @return The SQL procedure name as a <code>String</code> or
     * null if the SQL cannot be prepared.
     */
    synchronized String prepareSQL(JtdsPreparedStatement pstmt,
                                   String sql,
                                   ParamInfo[] params,
                                   boolean returnKeys)
    throws SQLException {
        if (!prepareSql) {
            return null; // User selected not to use procs
        }

        if (serverType == TdsCore.SYBASE) {
            if (tdsVersion != TdsCore.TDS50) {
                return null; // No longer support stored procs with 4.2
            }

            if (returnKeys) {
                return null; // Sybase cannot use @@IDENTITY in proc
            }
        }

        //
        // Check parameters set and obtain native types
        //
        for (int i = 0; i < params.length; i++) {
            if (!params[i].isSet) {
                throw new SQLException(Support.getMessage("error.prepare.paramnotset",
                                                          Integer.toString(i+1)),
                                       "07000");
            }

            TdsData.getNativeType(this, params[i]);

            if (serverType == TdsCore.SYBASE) {
                if (params[i].sqlType.equals("text")
                    || params[i].sqlType.equals("image")) {
                    return null; // Sybase does not support text/image params
                }
            }
        }

        StringBuffer key = new StringBuffer(sql.length() + 64);

        key.append(getCatalog()); // Not sure if this is required for sybase
        key.append(sql);

        //
        // Append parameter data types to key (not needed for sybase).
        //
        for (int i = 0; i < params.length && serverType != TdsCore.SYBASE; i++) {
            key.append(params[i].sqlType);
        }

        //
        // See if we have already built this one
        //
        ProcEntry proc = getCachedProcedure(key.toString());

        if (proc != null) {
            if (serverType == TdsCore.SYBASE) {
                pstmt.setColMetaData(proc.colMetaData);
                pstmt.setParamMetaData(proc.paramMetaData);
            }

            return proc.name;
        }

        //
        // No so create the stored procedure now
        //
        proc = new ProcEntry();
        proc.name = getProcName();

        if (serverType == TdsCore.SQLSERVER) {
            if (!baseTds.microsoftPrepare(sql, proc.name, params)) {
                return null;
            }
            // TODO Find some way of getting parameter meta data for MS
        } else {
            if (!baseTds.sybasePrepare(sql, proc.name, params)) {
                return null;
            }

            // Sybase gives us lots of useful information about the result set
            proc.colMetaData = baseTds.getColumns();
            proc.paramMetaData = baseTds.getParameters();
            pstmt.setColMetaData(proc.colMetaData);
            pstmt.setParamMetaData(proc.paramMetaData);
        }

        // OK we have built a proc so add it to the cache.
        addCachedProcedure(key.toString(), proc);

        // Give the user the name
        return proc.name;
    }

    /**
     * Add a stored procedure to the cache.
     *
     * @param key The signature of the procedure to cache.
     * @param proc The stored procedure descriptor.
     */
    void addCachedProcedure(String key, ProcEntry proc) {
        procedures.put(key, proc);

        if (!autoCommit) {
            procInTran.add(key);
        }
    }

    /**
     * Remove a stored procedure from the cache.
     *
     * @param key The signature of the procedure to remove from the cache.
     */
    void removeCachedProcedure(String key) {
        procedures.remove(key);

        if (!autoCommit) {
            procInTran.remove(key);
        }
    }

    /**
     * Retrive the server type.
     *
     * @return The server type as an <code>int</code> where
     * 1 = SQLSERVER and 2 = SYBASE.
     */
    int getServerType() {
        return this.serverType;
    }

    /**
     * Set the network packet size.
     *
     * @param size The new packet size.
     */
    void setNetPacketSize(int size) {
        this.netPacketSize = size;
    }

    /**
     * Retrive the network packet size.
     *
     * @return The packet size as an <code>int</code>.
     */
    int getNetPacketSize() {
        return this.netPacketSize;
    }

    /**
     * Retrive the current row count on this connection.
     *
     * @return The packet size as an <code>int</code>.
     */
    int getRowCount() {
        return this.rowCount;
    }

    /**
     * Set the current row count on this connection.
     *
     * @param count The new row count.
     */
    void setRowCount(int count) {
        rowCount = count;
    }

    /**
     * Retrieve the status of the lastUpdateCount flag.
     *
     * @return The lastUpdateCount flag as a <code>boolean</code>.
     */
    boolean isLastUpdateCount() {
        return this.lastUpdateCount;
    }

    /**
     * Retrive the maximum decimal precision.
     *
     * @return The precision as an <code>int</code>.
     */
    int getMaxPrecision() {
        return this.maxPrecision;
    }

    /**
     * Retrive the LOB buffer size.
     *
     * @return The LOB buffer size as a <code>long</code>.
     */
    long getLobBuffer() {
        return this.lobBuffer;
    }
    
    /**
     * Transfer the properties to the local instance variables.
     *
     * @param info The connection properties Object.
     * @throws SQLException
     */
    protected void unpackProperties(Properties info)
    throws SQLException {
        serverName = info.getProperty(Support.getMessage("prop.servername"));

        try {
            serverPort  = Integer.parseInt(
                                          info.getProperty(Support.getMessage("prop.portnumber"), "1433"));
        } catch (NumberFormatException e) {
            throw new SQLException(
                                  Support.getMessage("error.connection.badprop",
                                                     Support.getMessage("prop.portnumber")), "08001");
        }

        try {
            serverType  = Integer.parseInt(
                                          info.getProperty(Support.getMessage("prop.servertype")));
        } catch (NumberFormatException e) {
            throw new SQLException(
                                  Support.getMessage("error.connection.badprop",
                                                     Support.getMessage("prop.portservertype")), "08001");
        }

        databaseName = info.getProperty(Support.getMessage("prop.databasename"),"");
        instanceName = info.getProperty(Support.getMessage("prop.instance"),"");
        domainName = info.getProperty(Support.getMessage("prop.domain"),"");
        user = info.getProperty(Support.getMessage("prop.user"));
        password = info.getProperty(Support.getMessage("prop.password"));
        macAddress = info.getProperty(Support.getMessage("prop.macaddress"), "");
        appName = info.getProperty(Support.getMessage("prop.appname"), "java.exe");
        libName = info.getProperty(Support.getMessage("prop.libname"), "jTDS");
        serverCharset = info.getProperty(Support.getMessage("prop.charset"));
        language = info.getProperty(Support.getMessage("prop.language"), "us_english");
        prepareSql = info.getProperty(Support.getMessage("prop.preparesql"), "true").equalsIgnoreCase("true");
        lastUpdateCount = info.getProperty(Support.getMessage("prop.lastupdatecount"), "true").equalsIgnoreCase("true");
        useUnicode = info.getProperty(Support.getMessage("prop.useunicode"), "true").equalsIgnoreCase("true");
        charsetSpecified = (serverCharset != null && serverCharset.length() > 0);

        if (!charsetSpecified) {
            serverCharset = "iso_1";
        }

        String tmp = info.getProperty(Support.getMessage("prop.tds"),
                                      (serverType == TdsCore.SQLSERVER)? "7.0": "5.0");

        if (tmp.equals("4.2")) {
            tdsVersion = TdsCore.TDS42;
        } else if (tmp.equals("5.0")) {
            tdsVersion = TdsCore.TDS50;
        } else if (tmp.equals("7.0")) {
            tdsVersion = TdsCore.TDS70;
        } else if (tmp.equals("8.0")) {
            tdsVersion = TdsCore.TDS80;
        } else {
            throw new SQLException(
                                  Support.getMessage("error.connection.badprop",
                                                     Support.getMessage("prop.tds")), "08001");
        }

        try {
            packetSize  = Integer.parseInt(
                                          info.getProperty(Support.getMessage("prop.packetsize"), "0"));
        } catch (NumberFormatException e) {
            throw new SQLException(
                                  Support.getMessage("error.connection.badprop",
                                                     Support.getMessage("prop.packetsize")), "08001");
        }

        if (packetSize < 512) {
            if (tdsVersion >= TdsCore.TDS70) {
                // Default of 0 means let the server specify packet size
                packetSize = (packetSize == 0) ? 0 : 4096;
            } else {
                // Sensible minimum for all other versions of TDS
                packetSize = 512;
            }
        }

        if (packetSize > 32768) {
            packetSize = 32768;
        }

        packetSize = (packetSize / 512) * 512;

        try {
            loginTimeout = Integer.parseInt(
                                            info.getProperty(Support.getMessage("prop.logintimeout"), "0"));
        } catch (NumberFormatException e) {
            throw new SQLException(
                                  Support.getMessage("error.connection.badprop",
                                                     Support.getMessage("prop.logintimeout")), "08001");
        }
        
        try {
            lobBuffer = Long.parseLong(
                                       info.getProperty(Support.getMessage("prop.lobbuffer"), "32768"));
        } catch (NumberFormatException e) {
            throw new SQLException(
                                  Support.getMessage("error.connection.badprop",
                                                     Support.getMessage("prop.lobbuffer")), "08001");
        }
        
    }

    /**
     * Retrieve the java Charset to use for encoding.
     *
     * @return The Charset name as a <code>String</code>.
     */
    protected String getCharSet() {
        return this.javaCharset;
    }

    /**
     * Retrieve the multibyte status of the current character set.
     *
     * @return <code>boolean</code> true if a multi byte character set.
     */
    protected boolean isWideChar() {
        return this.wideChars;
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
     * Called by the protocol to change the current character set.
     *
     * @param charset The new character set name.
     */
    protected void setCharset(String charset) {
        // If the user specified a charset, ignore environment changes
        if (charsetSpecified) {
            Logger.println("Server charset " + charset +
                           ". Ignoring as driver requested " + serverCharset);
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

        if (!charset.equals(serverCharset)) {
            loadCharset(charset);
        }

        if (Logger.isActive()) {
            Logger.println("Set charset to " + serverCharset + '/' + javaCharset);
        }
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
            throw new SQLException(Support.getMessage("error.connection.dbmismatch",
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

        if (tdsVersion >= TdsCore.TDS70) {
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
     * Set the default collation for this connection.
     * <p>Set by a SQL Server 2000 environment change packet.
     * The collation consists of the following fields:
     * <ol>
     * <li>short - The LCID eg 0x0409 for US English which maps to code page 1252 (Latin1_General).
     * <li>short - Unknown flags.
     * <li>byte  - CSID from syscharsets?
     * </ol>
     *
     * @param collation The new collation.
     */
    void setCollation(byte[] collation) {
        this.collation = collation;
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
     * Remove a statement object from the list maintained by the connection.
     *
     * @param statement The statement to remove.
     */
    void removeStatement(JtdsStatement statement) {
        synchronized (statements) {
            for (int i = 0; i < statements.size(); i++) {
                WeakReference wr = (WeakReference) statements.get(i);

                if (wr != null) {
                    Statement stmt = (Statement) wr.get();

                    if (stmt != null && stmt == statement) {
                        statements.set(i, null);
                    }
                }
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

                if (wr != null) {
                    Statement stmt = (Statement)wr.get();

                    if (stmt == null) {
                        statements.set(i, new WeakReference(statement));
                        return;
                    }
                } else {
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
                                  Support.getMessage("error.generic.closed", "Connection"), "HY010");
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
                              Support.getMessage("error.generic.notimp", method), "HYC00");
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
    String getUrl() {
        return this.url;
    }

    /**
     * Used to force the closed status on the statement if an
     * IO error has occurred.
     */
    void setClosed() {
        closed = true;

        //
        // Close any open statements
        //
        ArrayList tmpList;

        synchronized (statements) {
            tmpList = new ArrayList(statements);
        }

        for (int i = 0; i < tmpList.size(); i++) {
            try {
                WeakReference wr = (WeakReference) tmpList.get(i);

                if (wr != null) {
                    JtdsStatement stmt = (JtdsStatement) wr.get();

                    if (stmt != null) {
                        stmt.setClosed();
                    }
                }
            } catch (SQLException e) {
                // Ignore
            }
        }
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

        baseTds.submitSQL("IF @@TRANCOUNT > 0 COMMIT TRAN");
        procInTran.clear();
        clearSavepoints();
    }

    synchronized public void rollback() throws SQLException {
        checkOpen();

        baseTds.submitSQL("IF @@TRANCOUNT > 0 ROLLBACK TRAN");

        synchronized (procedures) {
            for (int i = 0; i < procInTran.size(); i++) {
                String key = (String) procInTran.get(i);

                if (key != null && tdsVersion != TdsCore.TDS50) {
                    procedures.remove(key);
                }
            }

            procInTran.clear();
        }

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
                throw new SQLException(Support.getMessage("error.generic.optvalue",
                                                          "CLOSE_CURSORS_AT_COMMIT",
                                                          "setHoldability"), "HY092");
            default:
                throw new SQLException(Support.getMessage("error.generic.badoption",
                                                          Integer.toString(holdability),
                                                          "setHoldability"), "HY092");
        }
    }

    synchronized public void setTransactionIsolation(int level) throws SQLException {
        checkOpen();
        String sql = "SET TRANSACTION ISOLATION LEVEL ";

        switch (level) {
            case java.sql.Connection.TRANSACTION_READ_COMMITTED:
                sql += "READ COMMITTED";
                break;
            case java.sql.Connection.TRANSACTION_READ_UNCOMMITTED:
                sql += "READ UNCOMMITTED";
                break;
            case java.sql.Connection.TRANSACTION_REPEATABLE_READ:
                sql += "REPEATABLE READ";
                break;
            case java.sql.Connection.TRANSACTION_SERIALIZABLE:
                sql += "SERIALIZABLE";
                break;
            case java.sql.Connection.TRANSACTION_NONE:
                throw new SQLException(Support.getMessage("error.generic.optvalue",
                                                          "TRANSACTION_NONE",
                                                          "setTransactionIsolation"), "HY024");
            default:
                throw new SQLException(Support.getMessage("error.generic.badoption",
                                                          Integer.toString(level),
                                                          "setTransactionIsolation"), "HY092");
        }

        transactionIsolation = level;
        baseTds.submitSQL(sql);
    }

    synchronized public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkOpen();

        if (this.autoCommit != autoCommit) {
            commit();
        }

        String sql;

        if (serverType == TdsCore.SYBASE) {
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
                                  Support.getMessage("error.generic.badparam",
                                                     catalog, "setCatalog"), "3D000");
        }

        String sql = tdsVersion >= TdsCore.TDS70 ?
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
            throw new SQLException(Support.getMessage("error.generic.nosql"), "HY000");
        }

        String[] result = new SQLParser(sql, new ArrayList()).parse();

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
            throw new SQLException(Support.getMessage("error.generic.nosql"), "HY000");
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
            throw new SQLException(Support.getMessage("error.generic.nosql"), "HY000");
        }

        boolean returnKeys;

        if (autoGeneratedKeys != JtdsStatement.RETURN_GENERATED_KEYS &&
            autoGeneratedKeys != JtdsStatement.NO_GENERATED_KEYS) {
            throw new SQLException(Support.getMessage("error.generic.badoption",
                                                      Integer.toString(autoGeneratedKeys),
                                                      "executeUpate"),
                                   "HY092");
        }

        JtdsPreparedStatement stmt =
        new JtdsPreparedStatement(this,
                                  sql,
                                  java.sql.ResultSet.TYPE_FORWARD_ONLY,
                                  java.sql.ResultSet.CONCUR_READ_ONLY,
                                  autoGeneratedKeys ==
                                  JtdsStatement.RETURN_GENERATED_KEYS);
        addStatement(stmt);

        return stmt;
    }

    public PreparedStatement prepareStatement(String sql, int type, int concurrency)
    throws SQLException {
        checkOpen();

        if (sql == null || sql.length() == 0) {
            throw new SQLException(Support.getMessage("error.generic.nosql"), "HY000");
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
                                  Support.getMessage("error.generic.nullparam", "prepareStatement"),"HY092");
        } else if (columnIndexes.length != 1) {
            throw new SQLException(
                                  Support.getMessage("error.generic.needcolindex", "prepareStatement"),"HY092");
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
                                  Support.getMessage("error.generic.nullparam", "prepareStatement"),"HY092");
        } else if (columnNames.length != 1) {
            throw new SQLException(
                                  Support.getMessage("error.generic.needcolname", "prepareStatement"),"HY092");
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
