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
    private int _loginTimeout;
    private String _databaseName = "";
    private String _description;
    private String _password = "";
    private int _portNumber = 1433;
    private String _serverName;
    private String _user;
    private String _tdsVersion = "7.0";
    private int _serverType = Tds.SQLSERVER;
    private String _domain = "";
    private String _instance = "";
    private boolean _sendStringParametersAsUnicode = true;

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
        return getConnection(_user, _password);
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

        props.setProperty(Tds.PROP_HOST, _serverName);
        props.setProperty(Tds.PROP_SERVERTYPE, String.valueOf(_serverType));
        props.setProperty(Tds.PROP_PORT, String.valueOf(_portNumber));
        props.setProperty(Tds.PROP_DBNAME, _databaseName);
        props.setProperty(Tds.PROP_TDS, _tdsVersion);
        props.setProperty(Tds.PROP_DOMAIN, _domain);
        props.setProperty(Tds.PROP_INSTANCE, _instance);
        props.setProperty(Tds.PROP_USEUNICODE,
                          String.valueOf(_sendStringParametersAsUnicode));

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
    public synchronized javax.sql.PooledConnection getPooledConnection()
    throws SQLException {
        return new net.sourceforge.jtds.jdbcx.PooledConnection(getConnection());
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
        _loginTimeout = loginTimeout;
    }

    public int getLoginTimeout() throws SQLException {
        return _loginTimeout;
    }

    public Reference getReference() throws NamingException {
        Reference ref = new Reference(getClass().getName(),
                                      TdsObjectFactory.class.getName(),
                                      null);

        ref.add(new StringRefAddr("serverName", _serverName));
        ref.add(new StringRefAddr("portNumber", String.valueOf(_portNumber)));
        ref.add(new StringRefAddr("databaseName", _databaseName));
        ref.add(new StringRefAddr("user", _user));
        ref.add(new StringRefAddr("password", _password));
        ref.add(new StringRefAddr("tdsVersion", _tdsVersion));
        ref.add(new StringRefAddr("serverType", String.valueOf(_serverType)));
        ref.add(new StringRefAddr("domain", _domain));
        ref.add(new StringRefAddr("instance", _instance));
        ref.add(new StringRefAddr("sendStringParametersAsUnicode",
                                  String.valueOf(_sendStringParametersAsUnicode)));

        return ref;
    }

    public void setDatabaseName(String databaseName) {
        _databaseName = databaseName;
    }

    public String getDatabaseName() {
        return _databaseName;
    }

    public void setDescription(String description) {
        _description = description;
    }
    public String getDescription() {
        return _description;
    }

    public void setPassword(String password) {
        _password = password;
    }

    public String getPassword() {
        return _password;
    }

    public void setPortNumber(int portNumber) {
        _portNumber = portNumber;
    }

    public int getPortNumber() {
        return _portNumber;
    }

    public void setServerName(String serverName) {
        _serverName = serverName;
    }

    public String getServerName() {
        return _serverName;
    }

    public void setUser(String user) {
        _user = user;
    }

    public String getUser() {
        return _user;
    }

    public void setTdsVersion(String tdsVersion) {
        _tdsVersion = tdsVersion;
    }

    public String getTdsVersion() {
        return _tdsVersion;
    }

    public void setServerType(int serverType) {
        _serverType = serverType;
    }
    public int getServerType() {
        return _serverType;
    }

    public String getDomain() {
        return _domain;
    }

    public void setDomain(String domain) {
        _domain = domain;
    }

    public String getInstance() {
        return _instance;
    }

    public void setInstance(String instance) {
        _instance = instance;
    }

    public boolean getSendStringParametersAsUnicode() {
        return _sendStringParametersAsUnicode;
    }

    public void setSendStringParametersAsUnicode(boolean sendStringParametersAsUnicode) {
        _sendStringParametersAsUnicode = sendStringParametersAsUnicode;
    }
}