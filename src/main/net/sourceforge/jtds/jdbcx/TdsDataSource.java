package net.sourceforge.jtds.jdbcx;

import java.io.*;
import java.sql.*;
import java.util.*;

import javax.naming.*;
import javax.sql.*;

import net.sourceforge.jtds.jdbc.*;

/**
 * A plain <code>DataSource</code> implementation.
 *
 * @author Alin Sinplean
 * @since  jTDS 0.3
 */
public class TdsDataSource
implements ConnectionPoolDataSource, DataSource, Referenceable, Serializable {
    private int loginTimeout;
    private String databaseName = "";
    private int portNumber = 1433;
    private String serverName;
    private String user;
    private String password = "";
    private String description;
    private String tds = "7.0";
    private int serverType = Tds.SQLSERVER;
    private String charset = "";
    private String domain = "";
    private String instance = "";
    private boolean lastUpdateCount = false;
    private boolean sendStringParametersAsUnicode = true;
    private String macAddress = "";

    /**
     * Constructs a new datasource.
     */
    public TdsDataSource() {
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

        props.setProperty(Tds.PROP_SERVERNAME, serverName);
        props.setProperty(Tds.PROP_SERVERTYPE, String.valueOf(serverType));
        props.setProperty(Tds.PROP_PORT, String.valueOf(portNumber));
        props.setProperty(Tds.PROP_DBNAME, databaseName);
        props.setProperty(Tds.PROP_TDS, tds);
        props.setProperty(Tds.PROP_CHARSET, charset);
        props.setProperty(Tds.PROP_DOMAIN, domain);
        props.setProperty(Tds.PROP_INSTANCE, instance);
        props.setProperty(Tds.PROP_LAST_UPDATE_COUNT,
                          String.valueOf(isLastUpdateCount()));
        props.setProperty(Tds.PROP_USEUNICODE,
                          String.valueOf(sendStringParametersAsUnicode));
        props.setProperty(Tds.PROP_MAC_ADDR, macAddress);

        props.setProperty(Tds.PROP_USER, user);
        props.setProperty(Tds.PROP_PASSWORD, password);

        return net.sourceforge.jtds.jdbc.Driver.getConnection(props);
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
        throw new java.lang.UnsupportedOperationException("Method getLogWriter() not yet implemented.");
    }

    public void setLogWriter(PrintWriter out) throws SQLException {
        throw new java.lang.UnsupportedOperationException("Method setLogWriter() not yet implemented.");
    }

    public void setLoginTimeout(int loginTimeout) throws SQLException {
        this.loginTimeout = loginTimeout;
    }

    public int getLoginTimeout() throws SQLException {
        return loginTimeout;
    }

    public Reference getReference() throws NamingException {
        Reference ref = new Reference(getClass().getName(),
                                      TdsObjectFactory.class.getName(),
                                      null);

        ref.add(new StringRefAddr("serverName", serverName));
        ref.add(new StringRefAddr("portNumber", String.valueOf(portNumber)));
        ref.add(new StringRefAddr("databaseName", databaseName));
        ref.add(new StringRefAddr("user", user));
        ref.add(new StringRefAddr("password", password));
        ref.add(new StringRefAddr("charset", charset));
        ref.add(new StringRefAddr("tds", tds));
        ref.add(new StringRefAddr("serverType", String.valueOf(serverType)));
        ref.add(new StringRefAddr("domain", domain));
        ref.add(new StringRefAddr("instance", instance));
        ref.add(new StringRefAddr("lastUpdateCount",
                                  String.valueOf(isLastUpdateCount())));
        ref.add(new StringRefAddr("sendStringParametersAsUnicode",
                                  String.valueOf(sendStringParametersAsUnicode)));
        ref.add(new StringRefAddr("macAddress", macAddress));

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

    public String getMacAddress() {
        return macAddress;
    }

    public void setMacAddress(String macAddress) {
        this.macAddress = macAddress;
    }
}
