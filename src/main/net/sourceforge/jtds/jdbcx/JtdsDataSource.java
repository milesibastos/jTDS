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

/**
 * A plain <code>DataSource</code> implementation.
 *
 * @author Alin Sinplean
 * @since  jTDS 0.3
 * @version $Id: JtdsDataSource.java,v 1.4 2004-07-27 01:22:05 ddkilzer Exp $
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
        props.setProperty(Support.getMessage("prop.namedpipe"),
                          String.valueOf(namedPipe));
        props.setProperty(Support.getMessage("prop.macaddress"), macAddress);
        props.setProperty(Support.getMessage("prop.preparesql"), String.valueOf(prepareSql));
        props.setProperty(Support.getMessage("prop.packetsize"), String.valueOf(packetSize));
        props.setProperty(Support.getMessage("prop.user"), user);
        props.setProperty(Support.getMessage("prop.password"), password);
        props.setProperty(Support.getMessage("prop.logintimeout"), Integer.toString(loginTimeout));
        
        java.sql.Driver driver = new net.sourceforge.jtds.jdbc.Driver();
        
        String url = "jdbc:jtds:" + ((serverType == TdsCore.SQLSERVER) ? "sqlserver:" : "sybase:");
        
        return driver.connect(url, props);
    }
}
