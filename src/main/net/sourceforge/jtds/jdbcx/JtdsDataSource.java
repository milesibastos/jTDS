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
 * @version $Id: JtdsDataSource.java,v 1.18 2004-10-25 19:33:40 bheineman Exp $
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
    protected String user;
    protected String password;
    protected String loginTimeout;
    protected String lobBuffer;
    protected String maxStatements;
    protected String appName;
    protected String progName;

    protected String description;

    /**
     * Constructs a new datasource.
     */
    public JtdsDataSource() {
        Properties props = new Properties();
        props.setProperty(Messages.get("prop.servertype"), String.valueOf(Driver.SQLSERVER));
        props = DefaultProperties.addDefaultProperties(props);

        serverName = props.getProperty(Messages.get("prop.servername"));
        serverType = props.getProperty(Messages.get("prop.servertype"));
        portNumber = props.getProperty(Messages.get("prop.portnumber"));
        databaseName = props.getProperty(Messages.get("prop.databasename"));
        tdsVersion = props.getProperty(Messages.get("prop.tds"));
        charset = props.getProperty(Messages.get("prop.charset"));
        language = props.getProperty(Messages.get("prop.language"));
        domain = props.getProperty(Messages.get("prop.domain"));
        instance = props.getProperty(Messages.get("prop.instance"));
        lastUpdateCount = props.getProperty(Messages.get("prop.lastupdatecount"));
        sendStringParametersAsUnicode = props.getProperty(Messages.get("prop.useunicode"));
        namedPipe = props.getProperty(Messages.get("prop.namedpipe"));
        macAddress = props.getProperty(Messages.get("prop.macaddress"));
        prepareSql = props.getProperty(Messages.get("prop.preparesql"));
        packetSize = props.getProperty(Messages.get("prop.packetsize"));
        user = props.getProperty(Messages.get("prop.user"));
        password = props.getProperty(Messages.get("prop.password"));
        loginTimeout = props.getProperty(Messages.get("prop.logintimeout"));
        lobBuffer = props.getProperty(Messages.get("prop.lobbuffer"));
        maxStatements = props.getProperty(Messages.get("prop.maxstatements"));
        appName = props.getProperty(Messages.get("prop.appname"));
        progName = props.getProperty(Messages.get("prop.progname"));
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

        props.setProperty(Messages.get("prop.servername"), serverName);
        props.setProperty(Messages.get("prop.portnumber"), portNumber);
        props.setProperty(Messages.get("prop.databasename"), databaseName);
        props.setProperty(Messages.get("prop.tds"), tdsVersion);
        if (charset != null) {
            props.setProperty(Messages.get("prop.charset"), charset);
        }
        if (language != null) {
            props.setProperty(Messages.get("prop.language"), language);
        }
        if (domain != null) {
            props.setProperty(Messages.get("prop.domain"), domain);
        }
        if (instance != null) {
            props.setProperty(Messages.get("prop.instance"), instance);
        }
        props.setProperty(Messages.get("prop.lastupdatecount"), lastUpdateCount);
        props.setProperty(Messages.get("prop.useunicode"), sendStringParametersAsUnicode);
        props.setProperty(Messages.get("prop.namedpipe"), namedPipe);
        props.setProperty(Messages.get("prop.macaddress"), macAddress);
        props.setProperty(Messages.get("prop.preparesql"), prepareSql);
        props.setProperty(Messages.get("prop.packetsize"), packetSize);
        props.setProperty(Messages.get("prop.user"), user);
        props.setProperty(Messages.get("prop.password"), password);
        props.setProperty(Messages.get("prop.logintimeout"), loginTimeout);
        props.setProperty(Messages.get("prop.lobbuffer"), lobBuffer);
        props.setProperty(Messages.get("prop.maxstatement"), maxStatements);
        props.setProperty(Messages.get("prop.appname"), appName);
        props.setProperty(Messages.get("prop.progname"), progName);

        java.sql.Driver driver = new net.sourceforge.jtds.jdbc.Driver();

        String url = "jdbc:jtds:" + DefaultProperties.getServerType(Integer.parseInt(serverType)) + ":";

        return driver.connect(url, props);
    }

    public Reference getReference() throws NamingException {
        Reference ref = new Reference(getClass().getName(),
                                      JtdsObjectFactory.class.getName(),
                                      null);

        ref.add(new StringRefAddr(Messages.get("prop.servername"), serverName));
        ref.add(new StringRefAddr(Messages.get("prop.servertype"), serverType));
        ref.add(new StringRefAddr(Messages.get("prop.portnumber"), portNumber));
        ref.add(new StringRefAddr(Messages.get("prop.databasename"), databaseName));
        ref.add(new StringRefAddr(Messages.get("prop.tds"), tdsVersion));
        ref.add(new StringRefAddr(Messages.get("prop.charset"), charset));
        ref.add(new StringRefAddr(Messages.get("prop.language"), language));
        ref.add(new StringRefAddr(Messages.get("prop.domain"), domain));
        ref.add(new StringRefAddr(Messages.get("prop.instance"), instance));
        ref.add(new StringRefAddr(Messages.get("prop.lastupdatecount"), lastUpdateCount));
        ref.add(new StringRefAddr(Messages.get("prop.useunicode"), sendStringParametersAsUnicode));
        ref.add(new StringRefAddr(Messages.get("prop.namedpipe"), namedPipe));
        ref.add(new StringRefAddr(Messages.get("prop.macaddress"), macAddress));
        ref.add(new StringRefAddr(Messages.get("prop.preparesql"), prepareSql));
        ref.add(new StringRefAddr(Messages.get("prop.packetsize"), packetSize));
        ref.add(new StringRefAddr(Messages.get("prop.user"), user));
        ref.add(new StringRefAddr(Messages.get("prop.password"), password));
        ref.add(new StringRefAddr(Messages.get("prop.logintimeout"), loginTimeout));
        ref.add(new StringRefAddr(Messages.get("prop.lobbuffer"), lobBuffer));
        ref.add(new StringRefAddr(Messages.get("prop.maxstatements"), maxStatements));
        ref.add(new StringRefAddr(Messages.get("prop.appname"), appName));
        ref.add(new StringRefAddr(Messages.get("prop.progname"), progName));

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

    public void setMaxStatements(long maxStatements) {
        this.maxStatements = String.valueOf(maxStatements);
    }

    public long getMaxStatements() {
        return Long.parseLong(maxStatements);
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
}
