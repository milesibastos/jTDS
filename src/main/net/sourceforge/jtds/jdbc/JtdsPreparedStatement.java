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
package net.sourceforge.jtds.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Collection;
import java.util.ArrayList;
import java.util.TimeZone;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.text.NumberFormat;

/**
 * jTDS implementation of the java.sql.PreparedStatement interface.
 * <p>
 * Implementation notes:
 * <ol>
 * <li>Generally a simple subclass of Statement mainly adding support for the
 *     setting of parameters.
 * <li>The stream logic is taken over from the work Brian did to add Blob support
 *     to the original jTDS.
 * <li>Use of Statement specific method calls eg executeQuery(sql) is blocked by
 *     this version of the driver. This is unlike the original jTDS but inline
 *     with all the other JDBC drivers that I have been able to test.
 * </ol>
 *
 * @author Mike Hutchinson
 * @author Brian Heineman
 * @version $Id: JtdsPreparedStatement.java,v 1.37 2005-02-09 09:57:35 alin_sinpalean Exp $
 */
public class JtdsPreparedStatement extends JtdsStatement implements PreparedStatement {
    /** The SQL statement being prepared. */
    protected String sql;
    /** The first SQL keyword in the SQL string.*/
    protected String sqlWord;
    /** The procedure name for CallableStatements. */
    protected String procName;
    /** The parameter list for the call. */
    protected ParamInfo[] parameters;
    /** True to return generated keys. */
    private boolean returnKeys = false;
    /** The cached column meta data. */
    protected ColInfo[] colMetaData = null;
    /** The cached parameter meta data. */
    protected ParamInfo[] paramMetaData = null;
    /** Used to format numeric values when scale is specified. */
    private static NumberFormat f = NumberFormat.getInstance();
    /** Collection of handles used by this statement */
    Collection handles = null;

    /**
     * Construct a new preparedStatement object.
     *
     * @param connection The parent connection.
     * @param sql   The SQL statement to prepare.
     * @param resultSetType The result set type eg SCROLLABLE etc.
     * @param concurrency The result set concurrency eg READONLY.
     * @param returnKeys True if generated keys should be returned.
     * @throws SQLException
     */
    JtdsPreparedStatement(ConnectionJDBC2 connection,
                          String sql,
                          int resultSetType,
                          int concurrency,
                          boolean returnKeys)
        throws SQLException {
        super(connection, resultSetType, concurrency);

        // Parse the SQL looking for escapes and parameters
        if (this instanceof JtdsCallableStatement) {
            sql = normalizeCall(sql);
        }

        ArrayList params = new ArrayList();
        String[] parsedSql = new SQLParser(sql, params, connection).parse(false);

        if (parsedSql[0].length() == 0) {
            throw new SQLException(Messages.get("error.prepare.nosql"), "07000");
        }

        this.sql = parsedSql[0];

        if (parsedSql[1].length() > 1) {
            if (this instanceof JtdsCallableStatement) {
                // Procedure call
                this.procName = parsedSql[1];
            }
        }
        sqlWord = parsedSql[2];

        if (returnKeys && sqlWord.equals("insert")) {
            if (connection.getServerType() == Driver.SQLSERVER
                    && connection.getDatabaseMajorVersion() >= 8) {
                this.sql += " SELECT SCOPE_IDENTITY() AS ID";
            } else {
                this.sql += " SELECT @@IDENTITY AS ID";
            }
            this.returnKeys = true;
        } else {
            this.returnKeys = false;
        }

        parameters = (ParamInfo[]) params.toArray(new ParamInfo[params.size()]);
    }

    /**
     * This method converts native call syntax into (hopefully) valid JDBC escape syntax.
     * NB. This method is required for backwards compatibility with previous versions of jTDS.
     * Strictly speaking only the JDBC syntax needs to be recognised,
     * constructions such as "?=#testproc ?,?" are neither valid native syntax nor valid escapes.
     * All the substrings and trims below are not as bad as they look. The objects created all refer back
     * to the original sql string it is just the start and length positions which change.
     * @param sql The SQL statement to process.
     * @return The SQL possibly in original form.
     */
    protected String normalizeCall(String sql) {
        String original = sql;
        sql = sql.trim();

        if (sql.length() > 0 && sql.charAt(0) == '{') {
            return original; // Assume already escaped
        }

        if (sql.length() > 4 && sql.substring(0, 5).equalsIgnoreCase("exec ")) {
            sql = sql.substring(4).trim();
        } else if (sql.length() > 7 && sql.substring(0, 8).equalsIgnoreCase("execute ")){
            sql = sql.substring(7).trim();
        }

        if (sql.length() > 1 && sql.charAt(0) == '?') {
            sql = sql.substring(1).trim();

            if (sql.length() < 1 || sql.charAt(0) != '=') {
                return original; // Give up error will be reported elsewhere
            }

            sql = sql.substring(1).trim();

            // OK now reconstruct as JDBC escaped call
            return "{?=call " + sql + "}";
        }

        return "{call " + sql + "}";
    }

    /**
     * Check that this statement is still open.
     *
     * @throws SQLException if statement closed.
     */
    protected void checkOpen() throws SQLException {
        if (closed) {
            throw new SQLException(
                    Messages.get("error.generic.closed", "PreparedStatement"), "HY010");
        }
    }

    /**
     * Report that user tried to call a method not supported on this type of statement.
     *
     * @param method The method name to report in the error message.
     * @throws SQLException
     */
    protected void notSupported(String method) throws SQLException {
        throw new SQLException(
                Messages.get("error.generic.notsup", method), "HYC00");
    }

    /**
     * Executes a <code>ParamInfo</code> array (list of parameters).
     *
     * @param value list of parameters to execute
     * @param last  <code>true</code> if this is the last parameter list in the
     *              batch
     */
    protected void executeBatchOther(Object value, boolean last)
            throws SQLException {
        if (value instanceof ParamInfo[]) {
            // procName will contain the procedure name for CallableStatements
            // and null for PreparedStatements
            tds.executeSQL(sql, procName, (ParamInfo[])value, false, 0, -1, -1, last);
        } else {
            super.executeBatchOther(value, last);
        }
    }
/*
    protected int executeBatchOther(Object value) throws SQLException {
        if (value instanceof ParamInfo[]) {
            ParamInfo[] saveParameters = parameters;

            try {
                parameters = (ParamInfo[]) value;

                return executeUpdate();
            } finally {
                parameters = saveParameters;
            }
        }

        super.executeBatchOther(value);

        return Integer.MIN_VALUE;
    }
*/

    /**
     * Check the supplied index and return the selected parameter.
     *
     * @param parameterIndex the parameter index 1 to n.
     * @return the parameter as a <code>ParamInfo</code> object.
     * @throws SQLException if the statement is closed;
     *                      if <code>parameterIndex</code> is less than 0;
     *                      if <code>parameterIndex</code> is greater than the
     *                      number of parameters;
     *                      if <code>checkIfSet</code> was <code>true</code>
     *                      and the parameter was not set
     */
    protected ParamInfo getParameter(int parameterIndex) throws SQLException {
        checkOpen();

        if (parameterIndex < 1 || parameterIndex > parameters.length) {
            throw new SQLException(Messages.get("error.prepare.paramindex",
                                                      Integer.toString(parameterIndex)),
                                                      "07009");
        }

        ParamInfo pi = parameters[parameterIndex - 1];

        return pi;
    }

    /**
     * Generic setObject method.
     *
     * @param parameterIndex Parameter index 1 to n.
     * @param x The value to set.
     * @param targetSqlType The java.sql.Types constant describing the data.
     * @param scale The decimal scale -1 if not set.
     */
    public void setObjectBase(int parameterIndex, Object x, int targetSqlType, int scale)
            throws SQLException {
        checkOpen();

        int length = 0;

        if (targetSqlType == java.sql.Types.CLOB) {
            targetSqlType = java.sql.Types.LONGVARCHAR;
        } else if (targetSqlType == java.sql.Types.BLOB) {
            targetSqlType = java.sql.Types.LONGVARBINARY;
        }

        if (x != null) {
            x = Support.convert(this, x, targetSqlType, connection.getCharset());

            if (scale >= 0) {
                if (x instanceof BigDecimal) {
                    x = ((BigDecimal) x).setScale(scale, BigDecimal.ROUND_HALF_UP);
                } else if (x instanceof Number) {
                    synchronized (f) {
                        f.setGroupingUsed(false);
                        f.setMaximumFractionDigits(scale);
                        x = Support.convert(this, f.format(x), targetSqlType,
                                connection.getCharset());
                    }
                }
            }

            if (x instanceof Blob) {
                Blob blob = (Blob) x;
                length = (int) blob.length();
                x = blob.getBinaryStream();
            } else if (x instanceof Clob) {
                Clob clob = (Clob) x;
                length = (int) clob.length();
                x = clob.getCharacterStream();
            }
        }

        setParameter(parameterIndex, x, targetSqlType, scale, length);
    }

    /**
     * Update the ParamInfo object for the specified parameter.
     *
     * @param parameterIndex Parameter index 1 to n.
     * @param x The value to set.
     * @param targetSqlType The java.sql.Types constant describing the data.
     * @param scale The decimal scale -1 if not set.
     * @param length The length of the data item.
     */
    protected void setParameter(int parameterIndex, Object x, int targetSqlType, int scale, int length)
        throws SQLException {
        ParamInfo pi = getParameter(parameterIndex);

        if (Support.getJdbcTypeName(targetSqlType).equals("ERROR")) {
            throw new SQLException(Messages.get("error.generic.badtype",
                                Integer.toString(targetSqlType)), "HY092");
        }

        pi.scale = (scale < 0) ? 0 : scale;
        // Update parameter descriptor
        if (targetSqlType == java.sql.Types.DECIMAL
            || targetSqlType == java.sql.Types.NUMERIC) {
            pi.precision = connection.getMaxPrecision();

            if (x instanceof BigDecimal) {
                x = Support.normalizeBigDecimal((BigDecimal) x, pi.precision);
                pi.scale = ((BigDecimal) x).scale();
            }
        }

        if (x instanceof String) {
            pi.length = ((String) x).length();
        } else if (x instanceof byte[]) {
            pi.length = ((byte[]) x).length;
        } else {
            pi.length   = length;
        }

        pi.value = x;
        pi.jdbcType = targetSqlType;
        pi.isSet = true;
        pi.isUnicode = connection.isUseUnicode();
    }

    /**
     * Update the cached column meta data information.
     *
     * @param value The Column meta data array.
     */
    void setColMetaData(ColInfo[] value) {
        this.colMetaData = value;
    }

    /**
     * Update the cached parameter meta data information.
     *
     * @param value The Column meta data array.
     */
    void setParamMetaData(ParamInfo[] value) {
        for (int i = 0; i < value.length && i < parameters.length; i++) {
            if (!parameters[i].isSet) {
                // Only update parameter descriptors if the user
                // has not yet set them.
                parameters[i].jdbcType = value[i].jdbcType;
                parameters[i].isOutput = value[i].isOutput;
                parameters[i].precision = value[i].precision;
                parameters[i].scale = value[i].scale;
                parameters[i].sqlType = value[i].sqlType;
            }
        }
    }

// -------------------- java.sql.PreparedStatement methods follow -----------------

    public int executeUpdate() throws SQLException {
        checkOpen();

        if (procName == null && !(this instanceof JtdsCallableStatement)) {
            // Sync on the connection to make sure rollback() isn't called
            // between the moment when the statement is prepared and the moment
            // when it's executed.
            synchronized (connection) {
                String spName = connection.prepareSQL(this, sql, parameters, returnKeys);
                executeSQL(sql, spName, sqlWord, parameters, returnKeys, true);
            }
        } else {
            executeSQL(sql, procName, sqlWord, parameters, returnKeys, true);
        }

        return getUpdateCount(0);
    }

    public synchronized void addBatch() throws SQLException {
        checkOpen();

        if (batchValues == null) {
            batchValues = new ArrayList();
        }

        if (parameters.length == 0) {
            // This is likely to be an error. Batch execution
            // of a prepared statement with no parameters means
            // exactly the same SQL will be executed each time!
            batchValues.add(sql);
        } else {
            batchValues.add(parameters);

            ParamInfo tmp[] = new ParamInfo[parameters.length];

            for (int i = 0; i < parameters.length; ++i) {
                tmp[i] = (ParamInfo) parameters[i].clone();
            }

            parameters = tmp;
        }
    }

    public void clearParameters() throws SQLException {
        checkOpen();

        for (int i = 0; i < parameters.length; i++) {
            parameters[i].clearInValue();
        }
    }

    public boolean execute() throws SQLException {
        checkOpen();
        resultQueue.clear();
        genKeyResultSet = null;

        if (procName == null && !(this instanceof JtdsCallableStatement)) {
            // Sync on the connection to make sure rollback() isn't called
            // between the moment when the statement is prepared and the moment
            // when it's executed.
            synchronized (connection) {
                String spName = connection.prepareSQL(this, sql, parameters, returnKeys);
                return executeSQL(sql, spName, sqlWord, parameters, returnKeys, false);
            }
        } else {
            return executeSQL(sql, procName, sqlWord, parameters, returnKeys, false);
        }
    }

    public void setByte(int parameterIndex, byte x) throws SQLException {
        setParameter(parameterIndex, new Integer((int) (x & 0xFF)), java.sql.Types.TINYINT, 0, 0);
    }

    public void setDouble(int parameterIndex, double x) throws SQLException {
        setParameter(parameterIndex, new Double(x), java.sql.Types.DOUBLE, 0, 0);
    }

    public void setFloat(int parameterIndex, float x) throws SQLException {
        setParameter(parameterIndex, new Double(x), java.sql.Types.FLOAT, 0, 0);
    }

    public void setInt(int parameterIndex, int x) throws SQLException {
        setParameter(parameterIndex, new Integer(x), java.sql.Types.INTEGER, 0, 0);
    }

    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        if (sqlType == java.sql.Types.CLOB) {
            sqlType = java.sql.Types.LONGVARCHAR;
        } else if (sqlType == java.sql.Types.BLOB) {
            sqlType = java.sql.Types.LONGVARBINARY;
        }

        setParameter(parameterIndex, null, sqlType, -1, 0);
    }

    public void setLong(int parameterIndex, long x) throws SQLException {
        setParameter(parameterIndex, new Long(x), java.sql.Types.BIGINT, 0, 0);
    }

    public void setShort(int parameterIndex, short x) throws SQLException {
        setParameter(parameterIndex, new Integer(x), java.sql.Types.SMALLINT, 0, 0);
    }

    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        setParameter(parameterIndex, x ? Boolean.TRUE : Boolean.FALSE, BOOLEAN, 0, 0);
    }

    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        setParameter(parameterIndex, x, java.sql.Types.BINARY, 0, 0);
    }

    public void setAsciiStream(int parameterIndex, InputStream inputStream, int length)
        throws SQLException {
        if (inputStream == null || length < 0) {
            setParameter(parameterIndex, null, java.sql.Types.LONGVARCHAR, 0, 0);
        } else {
            try {
                setCharacterStream(parameterIndex, new InputStreamReader(inputStream, "US-ASCII"), length);
            } catch (UnsupportedEncodingException e) {
                // Should never happen!
            }
        }
    }

    public void setBinaryStream(int parameterIndex, InputStream x, int length)
        throws SQLException {
        checkOpen();

        if (x == null || length < 0) {
            setBytes(parameterIndex, null);
        } else {
            setParameter(parameterIndex, x, java.sql.Types.LONGVARBINARY, 0, length);
        }
    }

    public void setUnicodeStream(int parameterIndex, InputStream inputStream, int length)
        throws SQLException {
        if (inputStream == null || length < 0) {
            setString(parameterIndex, null);
        } else {
            try {
               length = length / 2;
               char[] tmp = new char[length];
               int pos = 0;
               int b1 = inputStream.read();
               int b2 = inputStream.read();

               while (b1 >= 0 && b2 >= 0 && pos < length) {
                   tmp[pos++] = (char) (((b1 << 8) &0xFF00) | (b2 & 0xFF));
                   b1 = inputStream.read();
                   b2 = inputStream.read();
               }
               setString(parameterIndex, new String(tmp, 0, pos));
            } catch (java.io.IOException e) {
                throw new SQLException(Messages.get("error.generic.ioerror",
                                                           e.getMessage()), "HY000");
            }
        }
    }

    public void setCharacterStream(int parameterIndex, Reader reader, int length)
        throws SQLException {
        if (reader == null || length < 0) {
            setParameter(parameterIndex, null, java.sql.Types.LONGVARCHAR, 0, 0);
        } else {
            setParameter(parameterIndex, reader, java.sql.Types.LONGVARCHAR, 0, length);
        }
    }

    public void setObject(int parameterIndex, Object x) throws SQLException {
        setObjectBase(parameterIndex, x, Support.getJdbcType(x), -1);
    }

    public void setObject(int parameterIndex, Object x, int targetSqlType)
        throws SQLException {
        setObjectBase(parameterIndex, x, targetSqlType, -1);
    }

    public void setObject(int parameterIndex, Object x, int targetSqlType, int scale)
        throws SQLException {
        if (scale < 0 || scale > 28) {
            throw new SQLException(Messages.get("error.generic.badscale"), "HY092");
        }
        setObjectBase(parameterIndex, x, targetSqlType, scale);
    }

    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        notImplemented("PreparedStatement.setNull(int, int, String)");
    }

    public void setString(int parameterIndex, String x) throws SQLException {
        setParameter(parameterIndex, x, java.sql.Types.VARCHAR, 0, 0);
    }

    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        setParameter(parameterIndex, x, java.sql.Types.DECIMAL, -1, 0);
    }

    public void setURL(int parameterIndex, URL url) throws SQLException {
        setString(parameterIndex, (url == null)? null: url.toString());
    }

    public void setArray(int arg0, Array arg1) throws SQLException {
        this.notImplemented("PreparedStatement.setArray");
    }

    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        if (x == null) {
            setBytes(parameterIndex, null);
        } else {
            long length = x.length();

            if (length > Integer.MAX_VALUE) {
                throw new SQLException(Messages.get("error.resultset.longblob"), "24000");
            }

            setBinaryStream(parameterIndex, x.getBinaryStream(), (int) x.length());
        }
    }

    public void setClob(int parameterIndex, Clob x) throws SQLException {
        if (x == null) {
            this.setString(parameterIndex, null);
        } else {
            long length = x.length();

            if (length > Integer.MAX_VALUE) {
                throw new SQLException(Messages.get("error.resultset.longclob"), "24000");
            }

            setCharacterStream(parameterIndex, x.getCharacterStream(), (int) x.length());
        }
    }

    public void setDate(int parameterIndex, Date x) throws SQLException {
        setParameter(parameterIndex, x, java.sql.Types.DATE, 0, 0);
    }

    public ParameterMetaData getParameterMetaData() throws SQLException {
        checkOpen();

        //
        // NB. This is usable only with the JDBC3 version of the interface.
        //
        if (connection.getServerType() == Driver.SYBASE) {
            // Sybase does return the parameter types for prepared sql.
            connection.prepareSQL(this, sql, new ParamInfo[0], false);
        }

        try {
            Class pmdClass = Class.forName("net.sourceforge.jtds.jdbc.ParameterMetaDataImpl");
            Class[] parameterTypes = new Class[] {ParamInfo[].class};
            Object[] arguments = new Object[] {parameters};
            Constructor pmdConstructor = pmdClass.getConstructor(parameterTypes);

            return (ParameterMetaData) pmdConstructor.newInstance(arguments);
        } catch (Exception e) {
            notImplemented("PreparedStatement.getParameterMetaData");
        }

        return null;
    }

    public void setRef(int parameterIndex, Ref x) throws SQLException {
        notImplemented("PreparedStatement.setRef");
    }

    public ResultSet executeQuery() throws SQLException {
        checkOpen();
        resultQueue.clear();
        genKeyResultSet = null;
        boolean isCallableStatement = this instanceof JtdsCallableStatement;

        if (procName == null && !(this instanceof JtdsCallableStatement)) {
            // Sync on the connection to make sure rollback() isn't called
            // between the moment when the statement is prepared and the moment
            // when it's executed.
            synchronized (connection) {
                String spName = connection.prepareSQL(this, sql, parameters, false);
                return executeSQLQuery(sql, spName, parameters, isCallableStatement);
            }
        } else {
            return executeSQLQuery(sql, procName, parameters, isCallableStatement);
        }
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        checkOpen();

        if (colMetaData == null) {
            if (currentResult != null) {
                colMetaData = currentResult.columns;
            } else if (connection.getServerType() == Driver.SYBASE) {
                // Sybase can provide meta data as a by product of preparing the call.
                connection.prepareSQL(this, sql, new ParamInfo[0], false);

                if (colMetaData == null) {
                    return null; // Sorry still no go
                }
            } else {
                // For Microsoft set all parameters to null and execute the query.
                // SET FMTONLY ON asks the server just to return meta data.
                // This only works for select statements
                if (!sqlWord.equals("select")) {
                    return null;
                }

                // Copy parameters to avoid corrupting any values already set
                // by the user as we need to set a flag and null out the data.
                ParamInfo[] params = new ParamInfo[parameters.length];
                for (int i = 0; i < params.length; i++) {
                    params[i] = new ParamInfo(parameters[i].markerPos);
                    params[i].isSet = true;
                }

                // Substitute nulls into SQL String
                StringBuffer testSql = new StringBuffer(sql.length() + 128);
                testSql.append("SET FMTONLY ON ");
                testSql.append(
                        Support.substituteParameters(sql,
                                params,
                                connection.getTdsVersion()));
                testSql.append(" SET FMTONLY OFF");

                try {
                    tds.submitSQL(testSql.toString());
                    colMetaData = tds.getColumns();
                } catch (SQLException e) {
                    // Ensure FMTONLY is switched off!
                    tds.submitSQL("SET FMTONLY OFF");
                    return null;
                }
            }
        }

        return new JtdsResultSetMetaData(colMetaData, colMetaData.length);
    }

    public void setTime(int parameterIndex, Time x) throws SQLException {
        setParameter( parameterIndex, x, java.sql.Types.TIME, 0, 0 );
    }

    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        setParameter(parameterIndex, x, java.sql.Types.TIMESTAMP, 0, 0);
    }

    public void setDate(int parameterIndex, Date x, Calendar cal)
        throws SQLException {

        if (x != null && cal != null) {
            TimeZone timeZone = TimeZone.getDefault();
            long newTime = x.getTime();

            newTime -= timeZone.getRawOffset();
            newTime += cal.getTimeZone().getRawOffset();
            x = new java.sql.Date(newTime);
        }

        setDate(parameterIndex, x);
    }

    public void setTime(int parameterIndex, Time x, Calendar cal)
        throws SQLException {
        if (x != null && cal != null) {
            TimeZone timeZone = TimeZone.getDefault();
            long newTime = x.getTime();

            newTime -= timeZone.getRawOffset();
            newTime += cal.getTimeZone().getRawOffset();
            x = new java.sql.Time(newTime);
        }

        setTime(parameterIndex, x);
    }

    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
        throws SQLException {
        if (x != null && cal != null) {
            TimeZone timeZone = TimeZone.getDefault();
            long newTime = x.getTime();

            newTime -= timeZone.getRawOffset();
            newTime += cal.getTimeZone().getRawOffset();
            x = new java.sql.Timestamp(newTime);
        }

        setTimestamp(parameterIndex, x);
    }

    public int executeUpdate(String sql) throws SQLException {
        notSupported("executeUpdate(String)");
        return 0;
    }

    public void addBatch(String sql) throws SQLException {
        notSupported("executeBatch(String)");
    }

    public boolean execute(String sql) throws SQLException {
        notSupported("execute(String)");
        return false;
    }

    public int executeUpdate(String sql, int getKeys) throws SQLException {
        notSupported("executeUpdate(String, int)");
        return 0;
    }

    public boolean execute(String arg0, int arg1) throws SQLException {
        notSupported("execute(String, int)");
        return false;
    }

    public int executeUpdate(String arg0, int[] arg1) throws SQLException {
        notSupported("executeUpdate(String, int[])");
        return 0;
    }

    public boolean execute(String arg0, int[] arg1) throws SQLException {
        notSupported("execute(String, int[])");
        return false;
    }

    public int executeUpdate(String arg0, String[] arg1) throws SQLException {
        notSupported("executeUpdate(String, String[])");
        return 0;
    }

    public boolean execute(String arg0, String[] arg1) throws SQLException {
        notSupported("execute(String, String[])");
        return false;
    }

    public ResultSet executeQuery(String sql) throws SQLException {
        notSupported("executeQuery(String)");
        return null;
    }
}
