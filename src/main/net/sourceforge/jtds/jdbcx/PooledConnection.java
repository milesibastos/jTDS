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

import javax.sql.*;
import net.sourceforge.jtds.jdbc.*;
import net.sourceforge.jtds.jdbcx.proxy.*;

/**
 *
 * @version $Id: PooledConnection.java,v 1.5 2004-07-23 12:19:08 bheineman Exp $
 */
public class PooledConnection implements javax.sql.PooledConnection {
    private final ArrayList _listeners = new ArrayList();
    private Connection _connection;
    private ConnectionProxy _connectionProxy = null;

    public PooledConnection(Connection connection) {
        _connection = connection;
    }

    /**
     * Adds the specified listener to the list.
     *
     * @see #fireConnectionEvent
     * @see #removeConnectionEventListener
     */
    public synchronized void addConnectionEventListener(ConnectionEventListener listener) {
        _listeners.add(listener);
    }

    /**
     * Closes the database connection.
     *
     * @throws SQLException if an error occurs
     */
    public synchronized void close() throws SQLException {
        _connection.close();
        _connection = null; // Garbage collect the connection
        fireConnectionEvent(false, null);
    }

    /**
     * Fires a new connection event on all listeners.
     *
     * @param closed <code>true</code> if <code>close</code> has been called on the
     *        connection; <code>false</code> if the <code>sqlException</code> represents
     *        an error where the connection may not longer be used.
     * @param sqlException the SQLException to pass to the listeners
     */
    public synchronized void fireConnectionEvent(boolean closed, SQLException sqlException) {
        if (_listeners.size() > 0) {
            ConnectionEvent connectionEvent = new ConnectionEvent(this, sqlException);
            Iterator iterator = _listeners.iterator();

            while (iterator.hasNext()) {
                ConnectionEventListener listener = (ConnectionEventListener) iterator.next();

                if (closed) {
                    listener.connectionClosed(connectionEvent);
                } else {
                    listener.connectionErrorOccurred(connectionEvent);
                }
            }
        }
    }

    /**
     * Returns a ConnectionProxy.
     *
     * @throws SQLException if an error occurs
     */
    public synchronized Connection getConnection() throws SQLException {
        if (_connection == null) {
            fireConnectionEvent(false,
                new SQLException(Support.getMessage("error.jdbcx.conclosed"),
                                 "08003"));

            return null;
        }

        if (_connectionProxy != null) {
            _connectionProxy.close();
        }

        // Sould the SQLException be captured here for safety in the future even though
        // no SQLException is being thrown by the ConnectionProxy at the moment???
        _connectionProxy = new ConnectionProxy(this, _connection);

        return _connectionProxy;
    }

    /**
     * Removes the specified listener from the list.
     *
     * @see #addConnectionEventListener
     * @see #fireConnectionEvent
     */
    public synchronized void removeConnectionEventListener(ConnectionEventListener listener) {
        _listeners.remove(listener);
    }
}
