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
public class TdsDataSource implements DataSource, Referenceable, Serializable
{
    private int loginTimeout;
    private String databaseName = "";
    private String description;
    private String password = "";
    private int portNumber = 1433;
    private String serverName;
    private String user;
    private String tdsVersion = "7.0";
    private int serverType = Tds.SQLSERVER;
    private String domain;
    private String instance;

    public TdsDataSource() {}

    public Connection getConnection() throws SQLException
    {
        return getConnection(getUser(), getPassword());
    }

    public Connection getConnection(String username, String password) throws SQLException
    {
        Properties props = new Properties();

        props.setProperty(Tds.PROP_HOST, getServerName());
        props.setProperty(Tds.PROP_SERVERTYPE, String.valueOf(getServerType()));
        props.setProperty(Tds.PROP_PORT, String.valueOf(getPortNumber()));
        props.setProperty(Tds.PROP_DBNAME, getDatabaseName());
        props.setProperty(Tds.PROP_TDS, getTdsVersion());
        props.setProperty(Tds.PROP_DOMAIN, getDomain());
        props.setProperty(Tds.PROP_INSTANCE, getInstance());

        props.setProperty(Tds.PROP_USER, username);
        props.setProperty(Tds.PROP_PASSWORD, password);

        try
        {
            return new TdsConnection(props);
        }
        catch( TdsException ex )
        {
            throw new SQLException(ex.getMessage());
        }
    }

    public PrintWriter getLogWriter() throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Method getLogWriter() not yet implemented.");
    }

    public void setLogWriter(PrintWriter out) throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Method setLogWriter() not yet implemented.");
    }

    public void setLoginTimeout(int seconds) throws SQLException
    {
        loginTimeout = seconds;
    }

    public int getLoginTimeout() throws SQLException
    {
        return loginTimeout;
    }

    public Reference getReference() throws NamingException
    {
        Reference ref = new Reference(getClass().getName(),
            TdsObjectFactory.class.getName(), null);

        ref.add( new StringRefAddr("serverName", getServerName()));
        ref.add( new StringRefAddr("portNumber", String.valueOf(getPortNumber())));
        ref.add( new StringRefAddr("databaseName", getDatabaseName()));
        ref.add( new StringRefAddr("user", getUser()));
        ref.add( new StringRefAddr("password", getPassword()));
        ref.add( new StringRefAddr("tdsVersion", getTdsVersion()));
        ref.add( new StringRefAddr("serverType", String.valueOf(getServerType())));
        ref.add( new StringRefAddr("domain", getDomain()));
        ref.add( new StringRefAddr("instance", getInstance()));

        return ref;
    }

    public void setDatabaseName(String newDatabaseName)
    {
        databaseName = newDatabaseName;
    }
    public String getDatabaseName()
    {
        return databaseName;
    }

    public void setDescription(String newDescription)
    {
        description = newDescription;
    }
    public String getDescription()
    {
        return description;
    }

    public void setPassword(String newPassword)
    {
        password = newPassword;
    }
    public String getPassword()
    {
        return password;
    }

    public void setPortNumber(int newPortNumber)
    {
        portNumber = newPortNumber;
    }
    public int getPortNumber()
    {
        return portNumber;
    }

    public void setServerName(String newServerName)
    {
        serverName = newServerName;
    }
    public String getServerName()
    {
        return serverName;
    }

    public void setUser(String newUser)
    {
        user = newUser;
    }
    public String getUser()
    {
        return user;
    }

    public void setTdsVersion(String newTdsVersion)
    {
        tdsVersion = newTdsVersion;
    }
    public String getTdsVersion()
    {
        return tdsVersion;
    }

    public void setServerType(int newServerType)
    {
        serverType = newServerType;
    }
    public int getServerType()
    {
        return serverType;
    }

    public String getDomain()
    {
        return domain;
    }

    public void setDomain(String domain)
    {
        this.domain = domain;
    }

    public String getInstance()
    {
        return instance;
    }

    public void setInstance(String instance)
    {
        this.instance = instance;
    }
}