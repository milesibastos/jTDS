package net.sourceforge.jtds.jdbcx;

import java.sql.*;
import java.util.Map;

import net.sourceforge.jtds.jdbc.TdsConnection;

/**
 * This class would be better implemented as a java.lang.reflect.Proxy.  However, this
 * feature was not added until 1.3 and reflection performance was not improved until 1.4.
 * Since the driver still needs to be compatible with 1.2 and 1.3 this class is used
 * to delegate the calls to the connection with minimal overhead.
 */
public class ConnectionProxy implements Connection {
    private net.sourceforge.jtds.jdbcx.PooledConnection _pooledConnection;
    private TdsConnection _connection;
    private boolean _closed = false;

    /**
     * Constructs a new connection proxy.
     */
    public ConnectionProxy(net.sourceforge.jtds.jdbcx.PooledConnection pooledConnection,
                           Connection connection) {
        _pooledConnection = pooledConnection;
        _connection = (TdsConnection) connection;
    }

    /**
     * Delgates calls to the connection; SQLExceptions thrown from the connection 
     * will cause an event to be fired on the connection pool listeners.
     *
     * @throws SQLException if an error occurs
     */
    public void clearWarnings() throws SQLException {
        validateConnection();

        try {
            _connection.clearWarnings();
        } catch (SQLException sqlException) {
            processSQLException(sqlException);
        }
    }

    /**
     * Delgates calls to the connection; SQLExceptions thrown from the connection 
     * will cause an event to be fired on the connection pool listeners.
     *
     * @throws SQLException if an error occurs
     */
    public void close() throws SQLException {
        if (_closed) {
            return;
        }

        _pooledConnection.fireConnectionEvent(true, null);
        _closed = true;
    }

    /**
     * Delgates calls to the connection; SQLExceptions thrown from the connection 
     * will cause an event to be fired on the connection pool listeners.
     *
     * @throws SQLException if an error occurs
     */
    public void commit() throws SQLException {
        validateConnection();

        try {
            _connection.commit();
        } catch (SQLException sqlException) {
            processSQLException(sqlException);
        }
    }

    /**
     * Delgates calls to the connection; SQLExceptions thrown from the connection 
     * will cause an event to be fired on the connection pool listeners.
     *
     * @throws SQLException if an error occurs
     */
    public Statement createStatement() throws SQLException {
        validateConnection();

        try {
            return _connection.createStatement();
        } catch (SQLException sqlException) {
            processSQLException(sqlException);
        }

        return null;
    }

    /**
     * Delgates calls to the connection; SQLExceptions thrown from the connection 
     * will cause an event to be fired on the connection pool listeners.
     *
     * @throws SQLException if an error occurs
     */
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        validateConnection();

        try {
            return _connection.createStatement(resultSetType, resultSetConcurrency);
        } catch (SQLException sqlException) {
            processSQLException(sqlException);
        }

        return null;
    }

    /**
     * Delgates calls to the connection; SQLExceptions thrown from the connection 
     * will cause an event to be fired on the connection pool listeners.
     *
     * @throws SQLException if an error occurs
     */
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        validateConnection();

        try {
            return _connection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
        } catch (SQLException sqlException) {
            processSQLException(sqlException);
        }

        return null;
    }

    /**
     * Delgates calls to the connection; SQLExceptions thrown from the connection 
     * will cause an event to be fired on the connection pool listeners.
     *
     * @throws SQLException if an error occurs
     */
    public boolean getAutoCommit() throws SQLException {
        validateConnection();

        try {
            return _connection.getAutoCommit();
        } catch (SQLException sqlException) {
            processSQLException(sqlException);
        }

        return false;
    }

    /**
     * Delgates calls to the connection; SQLExceptions thrown from the connection 
     * will cause an event to be fired on the connection pool listeners.
     *
     * @throws SQLException if an error occurs
     */
    public String getCatalog() throws SQLException {
        validateConnection();

        try {
            return _connection.getCatalog();
        } catch (SQLException sqlException) {
            processSQLException(sqlException);
        }

        return null;
    }

    /**
     * Delgates calls to the connection; SQLExceptions thrown from the connection 
     * will cause an event to be fired on the connection pool listeners.
     *
     * @throws SQLException if an error occurs
     */
    public int getHoldability() throws SQLException {
        validateConnection();

        try {
            return _connection.getHoldability();
        } catch (SQLException sqlException) {
            processSQLException(sqlException);
        }

        return Integer.MIN_VALUE;
    }

    /**
     * Delgates calls to the connection; SQLExceptions thrown from the connection 
     * will cause an event to be fired on the connection pool listeners.
     *
     * @throws SQLException if an error occurs
     */
    public int getTransactionIsolation() throws SQLException {
        validateConnection();

        try {
            return _connection.getTransactionIsolation();
        } catch (SQLException sqlException) {
            processSQLException(sqlException);
        }

        return Integer.MIN_VALUE;
    }

    /**
     * Delgates calls to the connection; SQLExceptions thrown from the connection 
     * will cause an event to be fired on the connection pool listeners.
     *
     * @throws SQLException if an error occurs
     */
    public Map getTypeMap() throws SQLException {
        validateConnection();

        try {
            return _connection.getTypeMap();
        } catch (SQLException sqlException) {
            processSQLException(sqlException);
        }

        return null;
    }

    /**
     * Delgates calls to the connection; SQLExceptions thrown from the connection 
     * will cause an event to be fired on the connection pool listeners.
     *
     * @throws SQLException if an error occurs
     */
    public SQLWarning getWarnings() throws SQLException {
        validateConnection();

        try {
            return _connection.getWarnings();
        } catch (SQLException sqlException) {
            processSQLException(sqlException);
        }

        return null;
    }

    /**
     * Delgates calls to the connection; SQLExceptions thrown from the connection 
     * will cause an event to be fired on the connection pool listeners.
     *
     * @throws SQLException if an error occurs
     */
    public DatabaseMetaData getMetaData() throws SQLException {
        validateConnection();

        try {
            return _connection.getMetaData();
        } catch (SQLException sqlException) {
            processSQLException(sqlException);
        }

        return null;
    }

    /**
     * Delgates calls to the connection; SQLExceptions thrown from the connection 
     * will cause an event to be fired on the connection pool listeners.
     *
     * @throws SQLException if an error occurs
     */
    public boolean isClosed() throws SQLException {
        if (_closed) {
            return true;
        }

        try {
            return _connection.isClosed();
        } catch (SQLException sqlException) {
            processSQLException(sqlException);
        }

        return _closed;
    }

    /**
     * Delgates calls to the connection; SQLExceptions thrown from the connection 
     * will cause an event to be fired on the connection pool listeners.
     *
     * @throws SQLException if an error occurs
     */
    public boolean isReadOnly() throws SQLException {
        validateConnection();

        try {
            return _connection.isReadOnly();
        } catch (SQLException sqlException) {
            processSQLException(sqlException);
        }

        return false;
    }

    /**
     * Delgates calls to the connection; SQLExceptions thrown from the connection 
     * will cause an event to be fired on the connection pool listeners.
     *
     * @throws SQLException if an error occurs
     */
    public String nativeSQL(String sql) throws SQLException {
        validateConnection();

        try {
            return _connection.nativeSQL(sql);
        } catch (SQLException sqlException) {
            processSQLException(sqlException);
        }

        return null;
    }

    /**
     * Delgates calls to the connection; SQLExceptions thrown from the connection 
     * will cause an event to be fired on the connection pool listeners.
     *
     * @throws SQLException if an error occurs
     */
    public CallableStatement prepareCall(String sql) throws SQLException {
        validateConnection();

        try {
            return _connection.prepareCall(sql);
        } catch (SQLException sqlException) {
            processSQLException(sqlException);
        }

        return null;
    }

    /**
     * Delgates calls to the connection; SQLExceptions thrown from the connection 
     * will cause an event to be fired on the connection pool listeners.
     *
     * @throws SQLException if an error occurs
     */
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        validateConnection();

        try {
            return _connection.prepareCall(sql, resultSetType, resultSetConcurrency);
        } catch (SQLException sqlException) {
            processSQLException(sqlException);
        }

        return null;
    }

    /**
     * Delgates calls to the connection; SQLExceptions thrown from the connection 
     * will cause an event to be fired on the connection pool listeners.
     *
     * @throws SQLException if an error occurs
     */
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        validateConnection();

        try {
            return _connection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        } catch (SQLException sqlException) {
            processSQLException(sqlException);
        }

        return null;
    }

    /**
     * Delgates calls to the connection; SQLExceptions thrown from the connection 
     * will cause an event to be fired on the connection pool listeners.
     *
     * @throws SQLException if an error occurs
     */
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        validateConnection();

        try {
            return _connection.prepareStatement(sql);
        } catch (SQLException sqlException) {
            processSQLException(sqlException);
        }

        return null;
    }

    /**
     * Delgates calls to the connection; SQLExceptions thrown from the connection 
     * will cause an event to be fired on the connection pool listeners.
     *
     * @throws SQLException if an error occurs
     */
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        validateConnection();

        try {
            return _connection.prepareStatement(sql, autoGeneratedKeys);
        } catch (SQLException sqlException) {
            processSQLException(sqlException);
        }

        return null;
    }

    /**
     * Delgates calls to the connection; SQLExceptions thrown from the connection 
     * will cause an event to be fired on the connection pool listeners.
     *
     * @throws SQLException if an error occurs
     */
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        validateConnection();

        try {
            return _connection.prepareStatement(sql, columnIndexes);
        } catch (SQLException sqlException) {
            processSQLException(sqlException);
        }

        return null;
    }

    /**
     * Delgates calls to the connection; SQLExceptions thrown from the connection 
     * will cause an event to be fired on the connection pool listeners.
     *
     * @throws SQLException if an error occurs
     */
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        validateConnection();

        try {
            return _connection.prepareStatement(sql, columnNames);
        } catch (SQLException sqlException) {
            processSQLException(sqlException);
        }

        return null;
    }

    /**
     * Delgates calls to the connection; SQLExceptions thrown from the connection 
     * will cause an event to be fired on the connection pool listeners.
     *
     * @throws SQLException if an error occurs
     */
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        validateConnection();

        try {
            return _connection.prepareStatement(sql, resultSetType, resultSetConcurrency);
        } catch (SQLException sqlException) {
            processSQLException(sqlException);
        }

        return null;
    }

    /**
     * Delgates calls to the connection; SQLExceptions thrown from the connection 
     * will cause an event to be fired on the connection pool listeners.
     *
     * @throws SQLException if an error occurs
     */
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        validateConnection();

        try {
            return _connection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        } catch (SQLException sqlException) {
            processSQLException(sqlException);
        }

        return null;
    }

    /**
     * Delgates calls to the connection; SQLExceptions thrown from the connection 
     * will cause an event to be fired on the connection pool listeners.
     *
     * @throws SQLException if an error occurs
     */
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        validateConnection();

        try {
            _connection.releaseSavepoint(savepoint);
        } catch (SQLException sqlException) {
            processSQLException(sqlException);
        }
    }

    /**
     * Delgates calls to the connection; SQLExceptions thrown from the connection 
     * will cause an event to be fired on the connection pool listeners.
     *
     * @throws SQLException if an error occurs
     */
    public void rollback() throws SQLException {
        validateConnection();

        try {
            _connection.rollback();
        } catch (SQLException sqlException) {
            processSQLException(sqlException);
        }
    }

    /**
     * Delgates calls to the connection; SQLExceptions thrown from the connection 
     * will cause an event to be fired on the connection pool listeners.
     *
     * @throws SQLException if an error occurs
     */
    public void rollback(Savepoint savepoint) throws SQLException {
        validateConnection();

        try {
            _connection.rollback(savepoint);
        } catch (SQLException sqlException) {
            processSQLException(sqlException);
        }
    }

    /**
     * Delgates calls to the connection; SQLExceptions thrown from the connection 
     * will cause an event to be fired on the connection pool listeners.
     *
     * @throws SQLException if an error occurs
     */
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        validateConnection();

        try {
            _connection.setAutoCommit(autoCommit);
        } catch (SQLException sqlException) {
            processSQLException(sqlException);
        }
    }

    /**
     * Delgates calls to the connection; SQLExceptions thrown from the connection 
     * will cause an event to be fired on the connection pool listeners.
     *
     * @throws SQLException if an error occurs
     */
    public void setCatalog(String catalog) throws SQLException {
        validateConnection();

        try {
            _connection.setCatalog(catalog);
        } catch (SQLException sqlException) {
            processSQLException(sqlException);
        }
    }

    /**
     * Delgates calls to the connection; SQLExceptions thrown from the connection 
     * will cause an event to be fired on the connection pool listeners.
     *
     * @throws SQLException if an error occurs
     */
    public void setHoldability(int holdability) throws SQLException {
        validateConnection();

        try {
            _connection.setHoldability(holdability);
        } catch (SQLException sqlException) {
            processSQLException(sqlException);
        }
    }

    /**
     * Delgates calls to the connection; SQLExceptions thrown from the connection 
     * will cause an event to be fired on the connection pool listeners.
     *
     * @throws SQLException if an error occurs
     */
    public void setReadOnly(boolean readOnly) throws SQLException {
        validateConnection();

        try {
            _connection.setReadOnly(readOnly);
        } catch (SQLException sqlException) {
            processSQLException(sqlException);
        }
    }

    /**
     * Delgates calls to the connection; SQLExceptions thrown from the connection 
     * will cause an event to be fired on the connection pool listeners.
     *
     * @throws SQLException if an error occurs
     */
    public Savepoint setSavepoint() throws SQLException {
        validateConnection();

        try {
            return _connection.setSavepoint();
        } catch (SQLException sqlException) {
            processSQLException(sqlException);
        }

        return null;
    }

    /**
     * Delgates calls to the connection; SQLExceptions thrown from the connection 
     * will cause an event to be fired on the connection pool listeners.
     *
     * @throws SQLException if an error occurs
     */
    public Savepoint setSavepoint(String name) throws SQLException {
        validateConnection();

        try {
            return _connection.setSavepoint(name);
        } catch (SQLException sqlException) {
            processSQLException(sqlException);
        }

        return null;
    }

    /**
     * Delgates calls to the connection; SQLExceptions thrown from the connection 
     * will cause an event to be fired on the connection pool listeners.
     *
     * @throws SQLException if an error occurs
     */
    public void setTransactionIsolation(int level) throws SQLException {
        validateConnection();

        try {
            _connection.setTransactionIsolation(level);
        } catch (SQLException sqlException) {
            processSQLException(sqlException);
        }
    }

    /**
     * Delgates calls to the connection; SQLExceptions thrown from the connection 
     * will cause an event to be fired on the connection pool listeners.
     *
     * @throws SQLException if an error occurs
     */
    public void setTypeMap(Map map) throws SQLException {
        validateConnection();

        try {
            _connection.setTypeMap(map);
        } catch (SQLException sqlException) {
            processSQLException(sqlException);
        }
    }

    /**
     * Validates the connection state.
     */
    private void validateConnection() throws SQLException {
        if (_closed) {
            throw new SQLException("Connection has been returned to pool and this "
                                   + "reference is no longer valid.");
        }
    }

    /**
     * Processes SQLExceptions.
     */
    private void processSQLException(SQLException sqlException) throws SQLException {
        _pooledConnection.fireConnectionEvent(false, sqlException);

        throw sqlException;
    }
}

