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
import java.sql.*;
import java.util.*;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;
import javax.sql.DataSource;

import net.sourceforge.jtds.jdbc.*;
import net.sourceforge.jtds.jdbc.Driver;
import net.sourceforge.jtds.util.Logger;

/**
 * A plain <code>DataSource</code> implementation.
 *
 * @author Alin Sinplean
 * @since  jTDS 0.3
 * @version $Id: JtdsDataSource.java,v 1.13 2004-08-28 15:01:34 bheineman Exp $
 */
public class JtdsDataSource
implements DataSource, Referenceable, Serializable {
    protected int loginTimeout = DefaultProperties.LOGIN_TIMEOUT;
    protected String databaseName = DefaultProperties.DATABASE_NAME;
    protected int portNumber = DefaultProperties.PORT_NUMBER_SQLSERVER;
    protected String serverName;
    protected String user = DefaultProperties.USER;
    protected String password = DefaultProperties.PASSWORD;
    protected String description;
    protected String tdsVersion = DefaultProperties.TDS_VERSION_70;
    protected int serverType = Driver.SQLSERVER;
    protected String charset = "";
    protected String language = "";
    protected String domain = "";
    protected String instance = "";
    protected boolean lastUpdateCount = DefaultProperties.LAST_UPDATE_COUNT;
    protected boolean sendStringParametersAsUnicode = DefaultProperties.USE_UNICODE;
    protected boolean namedPipe = DefaultProperties.NAMED_PIPE;
    protected String macAddress = DefaultProperties.MAC_ADDRESS;
    protected int packetSize = DefaultProperties.PACKET_SIZE_70_80;
    protected int prepareSql = DefaultProperties.PREPARE_SQL;
    protected long lobBuffer = DefaultProperties.LOB_BUFFER_SIZE;
    protected String appName = DefaultProperties.APP_NAME;
    protected String progName = DefaultProperties.PROG_NAME;

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

        props.setProperty(Messages.get("prop.servername"), serverName);
        props.setProperty(Messages.get("prop.servertype"), String.valueOf(serverType));
        props.setProperty(Messages.get("prop.portnumber"), String.valueOf(portNumber));
        props.setProperty(Messages.get("prop.databasename"), databaseName);
        props.setProperty(Messages.get("prop.tds"), tdsVersion);
        props.setProperty(Messages.get("prop.charset"), charset);
        props.setProperty(Messages.get("prop.language"), language);
        props.setProperty(Messages.get("prop.domain"), domain);
        props.setProperty(Messages.get("prop.instance"), instance);
        props.setProperty(Messages.get("prop.lastupdatecount"),
                          String.valueOf(getLastUpdateCount()));
        props.setProperty(Messages.get("prop.useunicode"),
                          String.valueOf(sendStringParametersAsUnicode));
        props.setProperty(Messages.get("prop.namedpipe"),
                          String.valueOf(namedPipe));
        props.setProperty(Messages.get("prop.macaddress"), macAddress);
        props.setProperty(Messages.get("prop.preparesql"), String.valueOf(prepareSql));
        props.setProperty(Messages.get("prop.packetsize"), String.valueOf(packetSize));
        props.setProperty(Messages.get("prop.user"), user);
        props.setProperty(Messages.get("prop.password"), password);
        props.setProperty(Messages.get("prop.logintimeout"), Integer.toString(loginTimeout));
        
        java.sql.Driver driver = new net.sourceforge.jtds.jdbc.Driver();
        
        String url = "jdbc:jtds:" + DefaultProperties.getServerType(serverType) + ":";
        
        return driver.connect(url, props);
    }

    public Reference getReference() throws NamingException {
        Reference ref = new Reference(getClass().getName(),
                                      JtdsObjectFactory.class.getName(),
                                      null);

        ref.add(new StringRefAddr(Messages.get("prop.servername"), serverName));
        ref.add(new StringRefAddr(Messages.get("prop.portnumber"),
                                  String.valueOf(portNumber)));
        ref.add(new StringRefAddr(Messages.get("prop.databasename"), databaseName));
        ref.add(new StringRefAddr(Messages.get("prop.user"), user));
        ref.add(new StringRefAddr(Messages.get("prop.password"), password));
        ref.add(new StringRefAddr(Messages.get("prop.charset"), charset));
        ref.add(new StringRefAddr(Messages.get("prop.language"), language));
        ref.add(new StringRefAddr(Messages.get("prop.tds"), tdsVersion));
        ref.add(new StringRefAddr(Messages.get("prop.servertype"),
                                  String.valueOf(serverType)));
        ref.add(new StringRefAddr(Messages.get("prop.domain"), domain));
        ref.add(new StringRefAddr(Messages.get("prop.instance"), instance));
        ref.add(new StringRefAddr(Messages.get("prop.lastupdatecount"),
                                  String.valueOf(getLastUpdateCount())));
        ref.add(new StringRefAddr(Messages.get("prop.logintimeout"),
                                  String.valueOf(loginTimeout)));
        ref.add(new StringRefAddr(Messages.get("prop.useunicode"),
                                  String.valueOf(sendStringParametersAsUnicode)));
        ref.add(new StringRefAddr(Messages.get("prop.namedpipe"),
                                  String.valueOf(namedPipe)));
        ref.add(new StringRefAddr(Messages.get("prop.macaddress"), macAddress));
        ref.add(new StringRefAddr(Messages.get("prop.packetsize"),
                                  String.valueOf(packetSize)));
        ref.add(new StringRefAddr(Messages.get("prop.preparesql"),
                                  String.valueOf(prepareSql)));
        ref.add(new StringRefAddr(Messages.get("prop.lobbuffer"),
                                  String.valueOf(lobBuffer)));
        ref.add(new StringRefAddr(Messages.get("prop.appname"),
                                  String.valueOf(appName)));
        ref.add(new StringRefAddr(Messages.get("prop.progname"),
                                  String.valueOf(progName)));

        return ref;
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
        this.tdsVersion = tds;
    }
  
    public String getTds() {
        return tdsVersion;
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

    public boolean getNamedPipe() {
        return namedPipe;
    }

    public void setNamedPipe(boolean namedPipe) {
        this.namedPipe = namedPipe;
    }

    public boolean getLastUpdateCount() {
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
  
    public void setPrepareSql(int prepareSql) {
        this.prepareSql = prepareSql;
    }
  
    public int getPrepareSql() {
        return prepareSql;
    }
    
    public void setLobBuffer(long lobBuffer) {
        this.lobBuffer = lobBuffer;
    }
  
    public long getLobBuffer() {
        return lobBuffer;
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
