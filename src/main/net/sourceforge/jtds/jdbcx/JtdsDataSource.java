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

import java.sql.*;
import java.util.*;

import net.sourceforge.jtds.jdbc.*;
import net.sourceforge.jtds.jdbc.Driver;

/**
 * A plain <code>DataSource</code> implementation.
 *
 * @author Alin Sinplean
 * @since  jTDS 0.3
 * @version $Id: JtdsDataSource.java,v 1.6 2004-08-05 01:45:23 ddkilzer Exp $
 */
public class JtdsDataSource extends AbstractDataSource {

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
        props.setProperty(Messages.get("prop.tds"), tds);
        props.setProperty(Messages.get("prop.charset"), charset);
        props.setProperty(Messages.get("prop.language"), language);
        props.setProperty(Messages.get("prop.domain"), domain);
        props.setProperty(Messages.get("prop.instance"), instance);
        props.setProperty(Messages.get("prop.lastupdatecount"),
                          String.valueOf(isLastUpdateCount()));
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
        
        String url = "jdbc:jtds:" + ((serverType == Driver.SQLSERVER) ? "sqlserver:" : "sybase:");
        
        return driver.connect(url, props);
    }
}
