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
package net.sourceforge.jtds.jdbcx;

import java.io.Serializable;
import java.io.PrintWriter;
import java.sql.SQLException;
import javax.sql.DataSource;
import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;

import net.sourceforge.jtds.jdbc.Driver;
import net.sourceforge.jtds.jdbc.Support;
import net.sourceforge.jtds.jdbc.TdsCore;
import net.sourceforge.jtds.jdbc.DefaultProperties;
import net.sourceforge.jtds.jdbc.Messages;
import net.sourceforge.jtds.util.Logger;

/**
* An abstract <code>DataSource</code> implementation.
*
* @author Alin Sinplean
* @since  jTDS 0.3
* @version $Id: AbstractDataSource.java,v 1.14 2004-08-05 01:45:23 ddkilzer Exp $
*/
abstract class AbstractDataSource
implements DataSource, Referenceable, Serializable {
    protected int loginTimeout = 0;
    protected String databaseName = "";
    protected int portNumber = DefaultProperties.PORT_NUMBER_SQLSERVER;
    protected String serverName;
    protected String user;
    protected String password = "";
    protected String description;
    protected String tds = DefaultProperties.TDS_VERSION_70;
    protected int serverType = Driver.SQLSERVER;
    protected String charset = "";
    protected String language = "";
    protected String domain = "";
    protected String instance = "";
    protected boolean lastUpdateCount = false;
    protected boolean sendStringParametersAsUnicode = true;
    protected boolean namedPipe = false;
    protected String macAddress = "";
    protected int packetSize = 0;
    protected boolean prepareSql = true;
    protected long lobBuffer = DefaultProperties.LOB_BUFFER_SIZE;

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
        ref.add(new StringRefAddr(Messages.get("prop.tds"), tds));
        ref.add(new StringRefAddr(Messages.get("prop.servertype"),
                                  String.valueOf(serverType)));
        ref.add(new StringRefAddr(Messages.get("prop.domain"), domain));
        ref.add(new StringRefAddr(Messages.get("prop.instance"), instance));
        ref.add(new StringRefAddr(Messages.get("prop.lastupdatecount"),
                                  String.valueOf(isLastUpdateCount())));
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

    public boolean getNamedPipe() {
        return namedPipe;
    }

    public void setNamedPipe(boolean namedPipe) {
        this.namedPipe = namedPipe;
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
    
    public void setLobBuffer(long lobBuffer) {
        this.lobBuffer = lobBuffer;
    }
  
    public long getLobBuffer() {
        return lobBuffer;
    }
}
