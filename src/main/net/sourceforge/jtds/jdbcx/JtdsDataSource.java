//jTDS JDBC Driver for Microsoft SQL Server
//Copyright (C) 2004 The jTDS Project
//
//This library is free software; you can redistribute it and/or
//modify it under the terms of the GNU Lesser General Public
//License as published by the Free Software Foundation; either
//version 2.1 of the License, or (at your option) any later version.
//
//This library is distributed in the hope that it will be useful,
//but WITHOUT ANY WARRANTY; without even the implied warranty of
//MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
//Lesser General Public License for more details.
//
//You should have received a copy of the GNU Lesser General Public
//License along with this library; if not, write to the Free Software
//Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
//

package net.sourceforge.jtds.jdbcx;

import java.io.*;
import java.sql.*;
import java.util.*;

import javax.naming.*;
import javax.sql.*;

import net.sourceforge.jtds.jdbc.*;
import net.sourceforge.jtds.util.*;

/**
 * A plain <code>DataSource</code> implementation.
 *
 * @author Alin Sinplean
 * @since  jTDS 0.3
 * @version $Id: JtdsDataSource.java,v 1.1 2004-06-27 17:00:55 bheineman Exp $
 */
public class JtdsDataSource
implements ConnectionPoolDataSource, DataSource, Referenceable, Serializable {
    private int loginTimeout = 0;
    private String databaseName = "";
    private int portNumber = 1433;
    private String serverName;
    private String user;
    private String password = "";
    private String description;
    private String tds = "7.0";
    private int serverType = 1;
    private String charset = "";
    private String language = "";
    private String domain = "";
    private String instance = "";
    private boolean lastUpdateCount = false;
    private boolean sendStringParametersAsUnicode = true;
    private String macAddress = "";
    private int packetSize = 0;
    private boolean prepareSql = true;

    /**
     * Constructs a new datasource.
     */
    public JtdsDataSource() {
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

        props.setProperty(Support.getMessage("prop.servername"), serverName);
        props.setProperty(Support.getMessage("prop.servertype"), String.valueOf(serverType));
        props.setProperty(Support.getMessage("prop.portnumber"), String.valueOf(portNumber));
        props.setProperty(Support.getMessage("prop.databasename"), databaseName);
        props.setProperty(Support.getMessage("prop.tds"), tds);
        props.setProperty(Support.getMessage("prop.charset"), charset);
        props.setProperty(Support.getMessage("prop.language"), language);
        props.setProperty(Support.getMessage("prop.domain"), domain);
        props.setProperty(Support.getMessage("prop.instance"), instance);
        props.setProperty(Support.getMessage("prop.lastupdatecount"),
                          String.valueOf(isLastUpdateCount()));
        props.setProperty(Support.getMessage("prop.useunicode"),
                          String.valueOf(sendStringParametersAsUnicode));
        props.setProperty(Support.getMessage("prop.macaddress"), macAddress);
        props.setProperty(Support.getMessage("prop.preparesql"), String.valueOf(prepareSql));
        props.setProperty(Support.getMessage("prop.packetsize"), String.valueOf(packetSize));
        props.setProperty(Support.getMessage("prop.user"), user);
        props.setProperty(Support.getMessage("prop.password"), password);
        props.setProperty(Support.getMessage("prop.logintimeout"), Integer.toString(loginTimeout));
        java.sql.Driver driver = new net.sourceforge.jtds.jdbc.Driver();
        String url = "jdbc:jtds:" + ((serverType == 1)? "sqlserver:": "sybase:");
        return driver.connect(url, props);
    }

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

    public PrintWriter getLogWriter() throws SQLException {
        return Logger.getLogWriter();
    }

    public void setLogWriter(PrintWriter out) throws SQLException {
        Logger.setLogWriter(out);
    }

    public void setLoginTimeout(int loginTimeout) throws SQLException {
        this.loginTimeout = loginTimeout;
    }

    public int getLoginTimeout() throws SQLException {
        return loginTimeout;
    }

    public Reference getReference() throws NamingException {
        Reference ref = new Reference(getClass().getName(),
                                      JtdsObjectFactory.class.getName(),
                                      null);

        ref.add(new StringRefAddr(Support.getMessage("prop.servername"), serverName));
        ref.add(new StringRefAddr(Support.getMessage("prop.portnumber"),
                                  String.valueOf(portNumber)));
        ref.add(new StringRefAddr(Support.getMessage("prop.databasename"), databaseName));
        ref.add(new StringRefAddr(Support.getMessage("prop.user"), user));
        ref.add(new StringRefAddr(Support.getMessage("prop.password"), password));
        ref.add(new StringRefAddr(Support.getMessage("prop.charset"), charset));
        ref.add(new StringRefAddr(Support.getMessage("prop.language"), language));
        ref.add(new StringRefAddr(Support.getMessage("prop.tds"), tds));
        ref.add(new StringRefAddr(Support.getMessage("prop.servertype"),
                                  String.valueOf(serverType)));
        ref.add(new StringRefAddr(Support.getMessage("prop.domain"), domain));
        ref.add(new StringRefAddr(Support.getMessage("prop.instance"), instance));
        ref.add(new StringRefAddr(Support.getMessage("prop.lastupdatecount"),
                                  String.valueOf(isLastUpdateCount())));
        ref.add(new StringRefAddr(Support.getMessage("prop.useunicode"),
                                  String.valueOf(sendStringParametersAsUnicode)));
        ref.add(new StringRefAddr(Support.getMessage("prop.macaddress"), macAddress));
        ref.add(new StringRefAddr(Support.getMessage("prop.preparesql"),
                                  String.valueOf(prepareSql)));
        ref.add(new StringRefAddr(Support.getMessage("prop.packetsize"),
                                  String.valueOf(packetSize)));

        return ref;
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
        this.portNumber = portNumber;
    }

    public int getPortNumber() {
        return portNumber;
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
        this.tds = tds;
    }

    public String getTds() {
        return tds;
    }

    public void setServerType(int serverType) {
        this.serverType = serverType;
    }
    public int getServerType() {
        return serverType;
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
        return sendStringParametersAsUnicode;
    }

    public void setSendStringParametersAsUnicode(boolean sendStringParametersAsUnicode) {
        this.sendStringParametersAsUnicode = sendStringParametersAsUnicode;
    }

    public boolean isLastUpdateCount() {
        return lastUpdateCount;
    }

    public void setLastUpdateCount(boolean lastUpdateCount) {
        this.lastUpdateCount = lastUpdateCount;
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
        this.packetSize = packetSize;
    }

    public int getPacketSize() {
        return packetSize;
    }

    public void setPrepareSql(boolean value) {
        this.prepareSql = value;
    }

    public boolean getPrepareSql() {
        return prepareSql;
    }
}
