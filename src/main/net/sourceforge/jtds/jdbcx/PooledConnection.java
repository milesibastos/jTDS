package net.sourceforge.jtds.jdbcx;

import java.sql.*;
import java.util.*;

import javax.sql.*;

public class PooledConnection implements javax.sql.PooledConnection {
    private Connection _connection;
    private ConnectionProxy _connectionProxy = null;
    private ArrayList _listeners = null;

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
        if (_listeners == null) {
            _listeners = new ArrayList();
        }

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
    synchronized void fireConnectionEvent(boolean closed, SQLException sqlException) {
        if (_listeners != null && _listeners.size() > 0) {
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
            fireConnectionEvent(false, new SQLException("Connection closed."));

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
        if (_listeners != null) {
            _listeners.remove(listener);
        }
    }
}
