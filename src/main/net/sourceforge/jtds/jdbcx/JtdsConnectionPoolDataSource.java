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

import java.sql.*;

import javax.sql.*;

/**
* A <code>ConnectionPoolDataSource</code> implementation.
*
* @author Brian Heineman
* @since  jTDS 0.9
* @version $Id: JtdsConnectionPoolDataSource.java,v 1.2 2004-08-24 17:45:07 bheineman Exp $
*/
public class JtdsConnectionPoolDataSource extends JtdsDataSource
implements ConnectionPoolDataSource {

    /**
     * Constructs a new datasource.
     */
    public JtdsConnectionPoolDataSource() {
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
}
