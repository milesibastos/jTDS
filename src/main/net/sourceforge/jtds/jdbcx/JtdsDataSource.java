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
package net.sourceforge.jtds.jdbcx;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;
import javax.sql.DataSource;
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.sql.ConnectionPoolDataSource;

import net.sourceforge.jtds.jdbc.DefaultProperties;
import net.sourceforge.jtds.jdbc.Driver;
import net.sourceforge.jtds.jdbc.Messages;
import net.sourceforge.jtds.util.Logger;

/**
 * The jTDS <code>DataSource</code>, <code>ConnectionPoolDataSource</code> and
 * <code>XADataSource</code> implementation.
 *
 * @author Alin Sinplean
 * @since  jTDS 0.3
 * @version $Id: JtdsDataSource.java,v 1.24 2005-01-14 06:02:24 alin_sinpalean Exp $
 */
public class JtdsDataSource
        implements DataSource, ConnectionPoolDataSource, XADataSource, Referenceable, Serializable {
    protected String serverName;
    protected String serverType;
    protected String portNumber;
    protected String databaseName;
    protected String tdsVersion;
    protected String charset;
    protected String language;
    protected String domain;
    protected String instance;
    protected String lastUpdateCount;
    protected String sendStringParametersAsUnicode;
    protected String namedPipe;
    protected String macAddress;
    protected String prepareSql;
    protected String packetSize;
    protected String tcpNoDelay;
    protected String user;
    protected String password;
    protected String loginTimeout;
    protected String lobBuffer;
    protected String maxStatements;
    protected String appName;
    protected String progName;
    protected String xaEmulation;
    protected String logFile;
    protected String ssl;

    protected String description;

    /**
     * Constructs a new datasource.
     */
    public JtdsDataSource() {
        Properties props = new Properties();
        props.setProperty(Messages.get(Driver.SERVERTYPE), String.valueOf(Driver.SQLSERVER));
        props = DefaultProperties.addDefaultProperties(props);

        serverName = props.getProperty(Messages.get(Driver.SERVERNAME));
        serverType = props.getProperty(Messages.get(Driver.SERVERTYPE));
        portNumber = props.getProperty(Messages.get(Driver.PORTNUMBER));
        databaseName = props.getProperty(Messages.get(Driver.DATABASENAME));
        tdsVersion = props.getProperty(Messages.get(Driver.TDS));
        charset = props.getProperty(Messages.get(Driver.CHARSET));
        language = props.getProperty(Messages.get(Driver.LANGUAGE));
        domain = props.getProperty(Messages.get(Driver.DOMAIN));
        instance = props.getProperty(Messages.get(Driver.INSTANCE));
        lastUpdateCount = props.getProperty(Messages.get(Driver.LASTUPDATECOUNT));
        sendStringParametersAsUnicode = props.getProperty(Messages.get(Driver.SENDSTRINGPARAMETERSASUNICODE));
        namedPipe = props.getProperty(Messages.get(Driver.NAMEDPIPE));
        macAddress = props.getProperty(Messages.get(Driver.MACADDRESS));
        prepareSql = props.getProperty(Messages.get(Driver.PREPARESQL));
        packetSize = props.getProperty(Messages.get(Driver.PACKETSIZE));
        tcpNoDelay = props.getProperty(Messages.get(Driver.TCPNODELAY));
        user = props.getProperty(Messages.get(Driver.USER));
        password = props.getProperty(Messages.get(Driver.PASSWORD));
        loginTimeout = props.getProperty(Messages.get(Driver.LOGINTIMEOUT));
        lobBuffer = props.getProperty(Messages.get(Driver.LOBBUFFER));
        maxStatements = props.getProperty(Messages.get(Driver.MAXSTATEMENTS));
        appName = props.getProperty(Messages.get(Driver.APPNAME));
        progName = props.getProperty(Messages.get(Driver.PROGNAME));
        xaEmulation = props.getProperty(Messages.get(Driver.XAEMULATION));
        logFile = props.getProperty(Messages.get(Driver.LOGFILE));
        ssl = props.getProperty(Messages.get(Driver.SSL));
    }

    /**
     * Returns a new XA database connection.
     *
     * @return a new database connection
     * @throws SQLException if an error occurs
     */
    public XAConnection getXAConnection() throws SQLException {
        return new JtdsXAConnection(this, getConnection(user, password));
    }
    /**
     * Returns a new XA database connection for the user and password specified.
     *
     * @param user     the user name to connect with
     * @param password the password to connect with
     * @return a new database connection
     * @throws SQLException if an error occurs
     */
    public XAConnection getXAConnection(String user, String password) throws SQLException {
        return new JtdsXAConnection(this, getConnection(user, password));
    }

    /**
     * Returns a new database connection.
     *
     * @return a new database connection
     * @throws SQLException if an error occurs
     */
    public Connection getConnection() throws SQLException {
        return getConnection(user, password);
    }

    /**
     * Returns a new database connection for the user and password specified.
     *
     * @param user the user name to connect with
     * @param password the password to connect with
     * @return a new database connection
     * @throws SQLException if an error occurs
     */
    public Connection getConnection(String user, String password)
            throws SQLException {
        Properties props = new Properties();

        if (serverName == null) {
            throw new SQLException(Messages.get("error.connection.nohost"), "08001");
        }

        //
        // This maybe the only way to initialise the logging subsystem
        // with some containers such as JBOSS.
        //
        if (getLogWriter() == null && logFile != null && logFile.length() > 0) {
            // Try to initialise a PrintWriter
            try {
                setLogWriter(new PrintWriter(new FileOutputStream(logFile), true));
            } catch (IOException e) {
                System.err.println("jTDS: Failed to set log file " + e);
            }
        }

        props.setProperty(Messages.get(Driver.SERVERNAME), serverName);
        props.setProperty(Messages.get(Driver.PORTNUMBER), portNumber);
        props.setProperty(Messages.get(Driver.DATABASENAME), databaseName);
        props.setProperty(Messages.get(Driver.TDS), tdsVersion);
        if (charset != null) {
            props.setProperty(Messages.get(Driver.CHARSET), charset);
        }
        if (language != null) {
            props.setProperty(Messages.get(Driver.LANGUAGE), language);
        }
        if (domain != null) {
            props.setProperty(Messages.get(Driver.DOMAIN), domain);
        }
        if (instance != null) {
            props.setProperty(Messages.get(Driver.INSTANCE), instance);
        }
        props.setProperty(Messages.get(Driver.LASTUPDATECOUNT), lastUpdateCount);
        props.setProperty(Messages.get(Driver.SENDSTRINGPARAMETERSASUNICODE), sendStringParametersAsUnicode);
        props.setProperty(Messages.get(Driver.NAMEDPIPE), namedPipe);
        props.setProperty(Messages.get(Driver.MACADDRESS), macAddress);
        props.setProperty(Messages.get(Driver.PREPARESQL), prepareSql);
        props.setProperty(Messages.get(Driver.PACKETSIZE), packetSize);
        props.setProperty(Messages.get(Driver.TCPNODELAY), tcpNoDelay);
        props.setProperty(Messages.get(Driver.XAEMULATION), xaEmulation);
        props.setProperty(Messages.get(Driver.USER), user);
        props.setProperty(Messages.get(Driver.PASSWORD), password);
        props.setProperty(Messages.get(Driver.LOGINTIMEOUT), loginTimeout);
        props.setProperty(Messages.get(Driver.LOBBUFFER), lobBuffer);
        props.setProperty(Messages.get(Driver.MAXSTATEMENTS), maxStatements);
        props.setProperty(Messages.get(Driver.APPNAME), appName);
        props.setProperty(Messages.get(Driver.PROGNAME), progName);
        props.setProperty(Messages.get(Driver.SSL), ssl);

        java.sql.Driver driver = new net.sourceforge.jtds.jdbc.Driver();

        String url = "jdbc:jtds:" + DefaultProperties.getServerType(Integer.parseInt(serverType)) + ":";

        return driver.connect(url, props);
    }

    public Reference getReference() throws NamingException {
        Reference ref = new Reference(getClass().getName(),
                                      JtdsObjectFactory.class.getName(),
                                      null);

        ref.add(new StringRefAddr(Messages.get(Driver.SERVERNAME), serverName));
        ref.add(new StringRefAddr(Messages.get(Driver.SERVERTYPE), serverType));
        ref.add(new StringRefAddr(Messages.get(Driver.PORTNUMBER), portNumber));
        ref.add(new StringRefAddr(Messages.get(Driver.DATABASENAME), databaseName));
        ref.add(new StringRefAddr(Messages.get(Driver.TDS), tdsVersion));
        ref.add(new StringRefAddr(Messages.get(Driver.CHARSET), charset));
        ref.add(new StringRefAddr(Messages.get(Driver.LANGUAGE), language));
        ref.add(new StringRefAddr(Messages.get(Driver.DOMAIN), domain));
        ref.add(new StringRefAddr(Messages.get(Driver.INSTANCE), instance));
        ref.add(new StringRefAddr(Messages.get(Driver.LASTUPDATECOUNT), lastUpdateCount));
        ref.add(new StringRefAddr(Messages.get(Driver.SENDSTRINGPARAMETERSASUNICODE), sendStringParametersAsUnicode));
        ref.add(new StringRefAddr(Messages.get(Driver.NAMEDPIPE), namedPipe));
        ref.add(new StringRefAddr(Messages.get(Driver.MACADDRESS), macAddress));
        ref.add(new StringRefAddr(Messages.get(Driver.PREPARESQL), prepareSql));
        ref.add(new StringRefAddr(Messages.get(Driver.PACKETSIZE), packetSize));
        ref.add(new StringRefAddr(Messages.get(Driver.TCPNODELAY), tcpNoDelay));
        ref.add(new StringRefAddr(Messages.get(Driver.XAEMULATION), xaEmulation));
        ref.add(new StringRefAddr(Messages.get(Driver.USER), user));
        ref.add(new StringRefAddr(Messages.get(Driver.PASSWORD), password));
        ref.add(new StringRefAddr(Messages.get(Driver.LOGINTIMEOUT), loginTimeout));
        ref.add(new StringRefAddr(Messages.get(Driver.LOBBUFFER), lobBuffer));
        ref.add(new StringRefAddr(Messages.get(Driver.MAXSTATEMENTS), maxStatements));
        ref.add(new StringRefAddr(Messages.get(Driver.APPNAME), appName));
        ref.add(new StringRefAddr(Messages.get(Driver.PROGNAME), progName));
        ref.add(new StringRefAddr(Messages.get(Driver.LOGFILE), logFile));
        ref.add(new StringRefAddr(Messages.get(Driver.SSL), ssl));

        return ref;
    }

    //
    // ConnectionPoolDataSource methods
    //

    /**
     * Returns a new pooled database connection.
     *
     * @return a new pooled database connection
     * @throws SQLException if an error occurs
     */
    public javax.sql.PooledConnection getPooledConnection()
            throws SQLException {
        return getPooledConnection(user, password);
    }

    /**
     * Returns a new pooled database connection for the user and password specified.
     *
     * @param user the user name to connect with
     * @param password the password to connect with
     * @return a new pooled database connection
     * @throws SQLException if an error occurs
     */
    public synchronized javax.sql.PooledConnection getPooledConnection(String user,
                                                                       String password)
            throws SQLException {
        return new net.sourceforge.jtds.jdbcx.PooledConnection(getConnection(user, password));
    }

    //
    // Getters and setters
    //

    public PrintWriter getLogWriter() throws SQLException {
        return Logger.getLogWriter();
    }

    public void setLogWriter(PrintWriter out) throws SQLException {
        Logger.setLogWriter(out);
    }

    public void setLoginTimeout(int loginTimeout) throws SQLException {
        this.loginTimeout = String.valueOf(loginTimeout);
    }

    public int getLoginTimeout() throws SQLException {
        return Integer.parseInt(loginTimeout);
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getPassword() {
        return password;
    }

    public void setPortNumber(int portNumber) {
        this.portNumber = String.valueOf(portNumber);
    }

    public int getPortNumber() {
        return Integer.parseInt(portNumber);
    }

    public void setServerName(String serverName) {
        this.serverName = serverName;
    }

    public String getServerName() {
        return serverName;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getUser() {
        return user;
    }

    public void setTds(String tds) {
        this.tdsVersion = tds;
    }

    public String getTds() {
        return tdsVersion;
    }

    // TODO Use sqlserver/sybase for this (instead of numeric values)
    public void setServerType(int serverType) {
        this.serverType = String.valueOf(serverType);
    }

    public int getServerType() {
        return Integer.parseInt(serverType);
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getInstance() {
        return instance;
    }

    public void setInstance(String instance) {
        this.instance = instance;
    }

    public boolean getSendStringParametersAsUnicode() {
        return Boolean.valueOf(sendStringParametersAsUnicode).booleanValue();
    }

    public void setSendStringParametersAsUnicode(boolean sendStringParametersAsUnicode) {
        this.sendStringParametersAsUnicode = String.valueOf(sendStringParametersAsUnicode);
    }

    public boolean getNamedPipe() {
        return Boolean.valueOf(namedPipe).booleanValue();
    }

    public void setNamedPipe(boolean namedPipe) {
        this.namedPipe = String.valueOf(namedPipe);
    }

    public boolean getLastUpdateCount() {
        return Boolean.valueOf(lastUpdateCount).booleanValue();
    }

    public void setLastUpdateCount(boolean lastUpdateCount) {
        this.lastUpdateCount = String.valueOf(lastUpdateCount);
    }

    public boolean getXaEmulation() {
        return Boolean.valueOf(xaEmulation).booleanValue();
    }

    public void setXaEmulation(boolean xaEmulation) {
        this.xaEmulation = String.valueOf(xaEmulation);
    }

    public String getCharset() {
        return charset;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public String getMacAddress() {
        return macAddress;
    }

    public void setMacAddress(String macAddress) {
        this.macAddress = macAddress;
    }

    public void setPacketSize(int packetSize) {
        this.packetSize = String.valueOf(packetSize);
    }

    public int getPacketSize() {
        return Integer.parseInt(packetSize);
    }

    public boolean getTcpNoDelay() {
        return Boolean.valueOf(tcpNoDelay).booleanValue();
    }

    public void setTcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = String.valueOf(tcpNoDelay);
    }

    public void setPrepareSql(int prepareSql) {
        this.prepareSql = String.valueOf(prepareSql);
    }

    public int getPrepareSql() {
        return Integer.parseInt(prepareSql);
    }

    public void setLobBuffer(long lobBuffer) {
        this.lobBuffer = String.valueOf(lobBuffer);
    }

    public long getLobBuffer() {
        return Long.parseLong(lobBuffer);
    }

    public void setMaxStatements(int maxStatements) {
        this.maxStatements = String.valueOf(maxStatements);
    }

    public int getMaxStatements() {
        return Integer.parseInt(maxStatements);
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getAppName() {
        return appName;
    }

    public void setProgName(String progName) {
        this.progName = progName;
    }

    public String getProgName() {
        return progName;
    }

    public void setLogFile(String logFile) {
        this.logFile = logFile;
    }

    public String getLogFile() {
        return logFile;
    }

    public void setSsl(String ssl) {
        this.ssl = ssl;
    }

    public String getSsl() {
        return ssl;
    }
}
