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
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;

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
 * @author
 *    Brian Heineman, Mike Hutchinson, Holger Rehn
 */
public class JtdsPreparedStatement extends JtdsStatement implements PreparedStatement {
    /** The SQL statement being prepared. */
    protected final String sql;
    /** The original SQL statement provided at construction time. */
    private final String originalSql;
    /** The first SQL keyword in the SQL string.*/
    protected String sqlWord;
    /** The procedure name for CallableStatements. */
    protected String procName;
    /** The parameter list for the call. */
    protected ParamInfo[] parameters;
    /** True to return generated keys. */
    private boolean returnKeys;
    /** The cached parameter meta data. */
    protected ParamInfo[] paramMetaData;
    /** Used to format numeric values when scale is specified. */
    private final static NumberFormat f = NumberFormat.getInstance();
    /** Collection of handles used by this statement */
    Collection handles;

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
    JtdsPreparedStatement(JtdsConnection connection,
                          String sql,
                          int resultSetType,
                          int concurrency,
                          boolean returnKeys)
        throws SQLException {
        super(connection, resultSetType, concurrency);

        // returned by toString()
        originalSql = sql;

        // Parse the SQL looking for escapes and parameters
        if (this instanceof JtdsCallableStatement) {
            sql = normalizeCall(sql);
        }

        ArrayList params = new ArrayList();
        String[] parsedSql = SQLParser.parse(sql, params, connection, false);

        if (parsedSql[0].length() == 0) {
            throw new SQLException(Messages.get("error.prepare.nosql"), "07000");
        }

        if (parsedSql[1].length() > 1) {
            if (this instanceof JtdsCallableStatement) {
                // Procedure call
                procName = parsedSql[1];
            }
        }
        sqlWord = parsedSql[2];

        if (returnKeys /*&& "insert".equals(sqlWord) REVIEW: see JtdsStatement.executeImpl */) {
            if (connection.getServerType() == Driver.SQLSERVER
                    && connection.getDatabaseMajorVersion() >= 8) {
                this.sql = parsedSql[0] + " SELECT SCOPE_IDENTITY() AS " + GENKEYCOL;
            } else {
                this.sql = parsedSql[0] + " SELECT @@IDENTITY AS " + GENKEYCOL;
            }
            this.returnKeys = true;
        } else {
            this.sql = parsedSql[0];
            this.returnKeys = false;
        }

        parameters = (ParamInfo[]) params.toArray(new ParamInfo[params.size()]);
    }

    /**
     * Returns the SQL command provided at construction time.
     */
    @Override
    public String toString() {
        return originalSql;
    }

   /**
    * <p> This method converts native call syntax into (hopefully) valid JDBC
    * escape syntax. </p>
    *
    * <p><b>Note:</b> This method is required for backwards compatibility with
    * previous versions of jTDS. Strictly speaking only the JDBC syntax needs to
    * be recognized, constructions such as "?=#testproc ?,?" are neither valid
    * native syntax nor valid escapes. All the substrings and trims below are
    * not as bad as they look. The objects created all refer back to the
    * original SQL string it is just the start and length positions which
    * change. </p>
    *
    * @param sql
    *    the SQL statement to process
    *
    * @return
    *    the SQL, possibly in original form
    *
    * @throws SQLException
    *    if the SQL statement is detected to be a normal SQL INSERT, UPDATE or
    *    DELETE statement instead of a procedure call
    */
   protected static String normalizeCall( final String sql )
      throws SQLException
   {
      try
      {
         return normalize( sql, 0 );
      }
      catch( SQLException sqle )
      {
         // if normalize was giving up due to an unrecognized syntax error it
         // would have thrown an SQLException without state
         if( sqle.getSQLState() != null )
            throw sqle;

         return sql;
      }
   }

   /**
    * <p> This method converts native call syntax into (hopefully) valid JDBC
    * escape syntax. </p>
    *
    * <p><b>Note:</b> This method is required for backwards compatibility with
    * previous versions of jTDS. Strictly speaking only the JDBC syntax needs to
    * be recognized, constructions such as "?=#testproc ?,?" are neither valid
    * native syntax nor valid escapes. All the substrings and trims below are
    * not as bad as they look. The objects created all refer back to the
    * original SQL string it is just the start and length positions which
    * change. </p>
    *
    * @param sql
    *    the SQL statement to process
    *
    * @return
    *    the SQL, possibly in original form
    *
    * @throws SQLException
    *    if the SQL statement is detected to be a normal SQL INSERT, UPDATE or
    *    DELETE statement instead of a procedure call
    */
   private static String normalize( final String sql, int level )
      throws SQLException
   {
      if( level > 1 )
         throw new SQLException();

      int len   = sql.length();
      int qmark = -1;
      int equal = -1;
      int call  = -1;

      // try to isolate the call
      for( int i = 0; i < len && call < 0; i ++ )
      {
         // skip whitespace
         while( Character.isWhitespace( sql.charAt( i ) ) )
         {
            i ++;
         }

         switch( sql.charAt( i ) )
         {
            case '{': // escape syntax with leading comment/white space or syntax error
                      return sql;

            case '?': // should be leading '?'
                      if( qmark == -1 )
                      {
                         qmark = i;
                      }
                      else
                      {
                         // syntax error, give up
                         throw new SQLException();
                      }
                      break;

            case '=': // should be leading '='
                      if( equal == -1 && qmark >= 0 )
                      {
                         equal = i;
                      }
                      else
                      {
                         // syntax error, give up
                         throw new SQLException();
                      }
                      break;

            case '-': // skip single comment
                      if( i + 1 < len && sql.charAt( i + 1 ) == '-' )
                      {
                         i += 2;

                         while( i < len && sql.charAt( i ) != '\n' && sql.charAt( i ) != '\r' )
                         {
                            i ++;
                         }
                      }
                      break;

            case '/': // skip multi line comment
                      if( i + 1 < len && sql.charAt( i + 1 ) == '*' )
                      {
                         i += 1;
                         int block = 1;

                         do
                         {
                            if( i >= len -1 )
                               throw new SQLException( Messages.get( "error.parsesql.missing", "*/" ), "22025" );

                            i ++;

                            if( sql.charAt( i ) == '/' && sql.charAt( i + 1 ) == '*' )
                            {
                               i ++;
                               block ++;
                            }
                            else if( sql.charAt( i ) == '*' && sql.charAt( i + 1 ) == '/' )
                            {
                               i ++;
                               block --;
                            }
                         }
                         while( block > 0 );
                      }
                      break;

            default:  //
                      if( len - i > 4 && ( sql.substring( i, i + 5 ).equalsIgnoreCase( "exec " ) ) || sql.substring( i, i + 5 ).equalsIgnoreCase( "call " ) )
                      {
                         return normalize( sql.substring( 0, i ) + sql.substring( i + 4, sql.length() ), level ++ );
                      }
                      else if( len - i > 7 && sql.substring( i, i + 8 ).equalsIgnoreCase( "execute " ) )
                      {
                         return normalize( sql.substring( 0, i ) + sql.substring( i + 7, sql.length() ), level ++ );
                      }

                      // keep backward compatibility, things like "testproc()" are accepted
                      call = i;
                      break;
         }
      }

      if( equal == -1 && qmark != -1 )
      {
         // syntax error, give up
         throw new SQLException();
      }

      // check for a more or less common mistake to execute a normal statement via CallableStatement (bug #637)
      if( call + 7 < len )
      {
         String sub = sql.substring( call, call + 7 );

         if( sub != null && ( sub.equalsIgnoreCase( "insert " ) || sub.equalsIgnoreCase( "update " ) || sub.equalsIgnoreCase( "delete " ) ) )
            throw new SQLException( Messages.get( "error.parsesql.noprocedurecall" ), "07000" );
      }

      // fast scan whether the statement ends with a single line comment and append an additional line break in that case
      return "{" + sql.substring( 0, call ) + "call " + sql.substring( call ) + ( openComment( sql, call ) ? "\n" : "" ) + "}";
   }

   /**
    *
    */
   private static boolean openComment( String sql, int offset )
      throws SQLException
   {
      int len = sql.length();

      for( int i = offset; i < len; i ++ )
      {
         switch( sql.charAt( i ) )
         {
            case '-': // single comment
                      if( i + 1 < len && sql.charAt( i + 1 ) == '-' )
                      {
                         i += 2;

                         while( i < len && sql.charAt( i ) != '\n' && sql.charAt( i ) != '\r' )
                         {
                            i ++;
                         }

                         if( i == len )
                         {
                            // reached end of statement, comment still open
                            return true;
                         }
                      }
                      break;

            case '/': // multi line comment
                      if( i + 1 < len && sql.charAt( i + 1 ) == '*' )
                      {
                         i += 1;
                         int block = 1;

                         do
                         {
                            if( i >= len -1 )
                               throw new SQLException( Messages.get( "error.parsesql.missing", "*/" ), "22025" );

                            i ++;

                            if( sql.charAt( i ) == '/' && sql.charAt( i + 1 ) == '*' )
                            {
                               i ++;
                               block ++;
                            }
                            else if( sql.charAt( i ) == '*' && sql.charAt( i + 1 ) == '/' )
                            {
                               i ++;
                               block --;
                            }
                         }
                         while( block > 0 );
                      }
                      break;
         }
      }

      return false;
   }

    /**
     * Check that this statement is still open.
     *
     * @throws SQLException if statement closed.
     */
    @Override
    protected void checkOpen() throws SQLException {
        if (isClosed()) {
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
     * Execute the SQL batch on a MS server.
     * <p/>
     * When running with <code>prepareSQL=1</code> or <code>3</code>, the driver will first prepare temporary stored
     * procedures or statements for each parameter combination found in the batch. The handles to these pre-preared
     * statements will then be used to execute the actual batch statements.
     *
     * @param size        the total size of the batch
     * @param executeSize the maximum number of statements to send in one request
     * @param counts      the returned update counts
     * @return chained exceptions linked to a <code>SQLException</code>
     * @throws SQLException if a serious error occurs during execution
     */
    @Override
    protected SQLException executeMSBatch(int size, int executeSize, ArrayList counts)
    throws SQLException {
        if (parameters.length == 0) {
            // There are no parameters, each SQL call is the same so execute as a simple batch
            return super.executeMSBatch(size, executeSize, counts);
        }
        SQLException sqlEx = null;
        String procHandle[] = null;

        // Prepare any statements before executing the batch
        if (connection.getPrepareSql() == TdsCore.TEMPORARY_STORED_PROCEDURES ||
                connection.getPrepareSql() == TdsCore.PREPARE) {
            procHandle = new String[size];
            for (int i = 0; i < size; i++) {
                // Prepare the statement
                procHandle[i] = connection.prepareSQL(this, sql, (ParamInfo[]) batchValues.get(i), false, false);
            }
        }

        for (int i = 0; i < size;) {
            Object value = batchValues.get(i);
            String proc = (procHandle == null) ? procName : procHandle[i];
            ++i;
            // Execute batch now if max size reached or end of batch
            boolean executeNow = (i % executeSize == 0) || i == size;

            tds.startBatch();
            tds.executeSQL(sql, proc, (ParamInfo[]) value, false, 0, -1, -1, executeNow);

            // If the batch has been sent, process the results
            if (executeNow) {
                sqlEx = tds.getBatchCounts(counts, sqlEx);

                // If a serious error then we stop execution now as count
                // is too small.
                if (sqlEx != null && counts.size() != i) {
                    break;
                }
            }
        }
        return sqlEx;
    }

    /**
     * Execute the SQL batch on a Sybase server.
     * <p/>
     * Sybase needs to have the SQL concatenated into one TDS language packet followed by up to 1000 parameters. This
     * method will be overridden for <code>CallableStatements</code>.
     *
     * @param size the total size of the batch
     * @param executeSize the maximum number of statements to send in one request
     * @param counts the returned update counts
     * @return chained exceptions linked to a <code>SQLException</code>
     * @throws SQLException if a serious error occurs during execution
     */
    @Override
    protected SQLException executeSybaseBatch(int size, int executeSize, ArrayList counts) throws SQLException {
        if (parameters.length == 0) {
            // There are no parameters each SQL call is the same so
            // execute as a simple batch
            return super.executeSybaseBatch(size, executeSize, counts);
        }
        // Revise the executeSize down if too many parameters will be required.
        // Be conservative the actual maximums are 256 for older servers and 2048.
        int maxParams = (connection.getDatabaseMajorVersion() < 12 ||
                (connection.getDatabaseMajorVersion() == 12 && connection.getDatabaseMinorVersion() < 50)) ?
                200 : 1000;
        StringBuilder sqlBuf = new StringBuilder(size * 32);
        SQLException sqlEx = null;
        if (parameters.length * executeSize > maxParams) {
            executeSize = maxParams / parameters.length;
            if (executeSize == 0) {
                executeSize = 1;
            }
        }
        ArrayList paramList = new ArrayList();
        for (int i = 0; i < size;) {
            Object value = batchValues.get(i);
            ++i;
            // Execute batch now if max size reached or end of batch
            boolean executeNow = (i % executeSize == 0) || i == size;

            int offset = sqlBuf.length();
            sqlBuf.append(sql).append(' ');
            for (int n = 0; n < parameters.length; n++) {
                ParamInfo p = ((ParamInfo[]) value)[n];
                // Allow for the position of the '?' marker in the buffer
                p.markerPos += offset;
                paramList.add(p);
            }
            if (executeNow) {
                ParamInfo args[];
                args = (ParamInfo[]) paramList.toArray(new ParamInfo[paramList.size()]);
                tds.executeSQL(sqlBuf.toString(), null, args, false, 0, -1, -1, true);
                sqlBuf.setLength(0);
                paramList.clear();
                // If the batch has been sent, process the results
                sqlEx = tds.getBatchCounts(counts, sqlEx);

                // If a serious error or a server error then we stop
                // execution now as count is too small.
                if (sqlEx != null && counts.size() != i) {
                    break;
                }
            }
        }
        return sqlEx;
    }

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

        return parameters[parameterIndex - 1];
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

        if ("ERROR".equals(Support.getJdbcTypeName(targetSqlType))) {
            throw new SQLException(Messages.get("error.generic.badtype",
                                Integer.toString(targetSqlType)), "HY092");
        }

        // Update parameter descriptor
        if (targetSqlType == java.sql.Types.DECIMAL
            || targetSqlType == java.sql.Types.NUMERIC) {

            pi.precision = connection.getMaxPrecision();
            if (x instanceof BigDecimal) {
                x = Support.normalizeBigDecimal((BigDecimal) x, pi.precision);
                pi.scale = ((BigDecimal) x).scale();
            } else {
                pi.scale = (scale < 0) ? TdsData.DEFAULT_SCALE : scale;
            }
        } else {
            pi.scale = (scale < 0) ? 0 : scale;
        }

        if (x instanceof String) {
            pi.length = ((String) x).length();
        } else if (x instanceof byte[]) {
            pi.length = ((byte[]) x).length;
        } else {
            pi.length   = length;
        }

        if (x instanceof Date) {
            x = new DateTime((Date) x);
        } else if (x instanceof Time) {
            x = new DateTime((Time) x);
        } else if (x instanceof Timestamp) {
            x = new DateTime((Timestamp) x);
        }

        pi.value = x;
        pi.jdbcType = targetSqlType;
        pi.isSet = true;
        pi.isUnicode = connection.getUseUnicode();
    }

    /**
     * Update the cached column meta data information.
     *
     * @param value The Column meta data array.
     */
    void setColMetaData(ColInfo[] value) {
        colMetaData = value;
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

    @Override
    public void close() throws SQLException {
        try {
            super.close();
        } finally {
            // Null these fields to reduce memory usage while
            // waiting for Statement.finalize() to execute.
            handles = null;
            parameters = null;
        }
    }

    @Override
    public int executeUpdate() throws SQLException {
        checkOpen();
        reset();

        if (procName == null && !(this instanceof JtdsCallableStatement)) {
            // Sync on the connection to make sure rollback() isn't called
            // between the moment when the statement is prepared and the moment
            // when it's executed.
            synchronized (connection) {
                String spName = connection.prepareSQL(this, sql, parameters, returnKeys, false);
                executeSQL(sql, spName, parameters, true, false);
            }
        } else {
            executeSQL(sql, procName, parameters, true, false);
        }

        int res = getUpdateCount();
        return res == -1 ? 0 : res;
    }

    @Override
    public void addBatch() throws SQLException {
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

    @Override
    public void clearParameters() throws SQLException {
        checkOpen();

        for (int i = 0; i < parameters.length; i++) {
            parameters[i].clearInValue();
        }
    }

    @Override
    public boolean execute() throws SQLException {
        checkOpen();
        reset();
        boolean useCursor = useCursor(returnKeys, sqlWord);

        if (procName == null && !(this instanceof JtdsCallableStatement)) {
            // Sync on the connection to make sure rollback() isn't called
            // between the moment when the statement is prepared and the moment
            // when it's executed.
            synchronized (connection) {
                String spName = connection.prepareSQL(this, sql, parameters, returnKeys, useCursor);
                return executeSQL(sql, spName, parameters, false, useCursor);
            }
        } else {
            return executeSQL(sql, procName, parameters, false, useCursor);
        }
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        setParameter(parameterIndex, new Integer((x & 0xFF)), java.sql.Types.TINYINT, 0, 0);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        setParameter(parameterIndex, new Double(x), java.sql.Types.DOUBLE, 0, 0);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        setParameter(parameterIndex, new Float(x), java.sql.Types.REAL, 0, 0);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        setParameter(parameterIndex, new Integer(x), java.sql.Types.INTEGER, 0, 0);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        if (sqlType == java.sql.Types.CLOB) {
            sqlType = java.sql.Types.LONGVARCHAR;
        } else if (sqlType == java.sql.Types.BLOB) {
            sqlType = java.sql.Types.LONGVARBINARY;
        }

        setParameter(parameterIndex, null, sqlType, -1, 0);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        setParameter(parameterIndex, new Long(x), java.sql.Types.BIGINT, 0, 0);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        setParameter(parameterIndex, new Integer(x), java.sql.Types.SMALLINT, 0, 0);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        setParameter(parameterIndex, x ? Boolean.TRUE : Boolean.FALSE, BOOLEAN, 0, 0);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        setParameter(parameterIndex, x, java.sql.Types.BINARY, 0, 0);
    }

    @Override
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

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length)
        throws SQLException {
        checkOpen();

        if (x == null || length < 0) {
            setBytes(parameterIndex, null);
        } else {
            setParameter(parameterIndex, x, java.sql.Types.LONGVARBINARY, 0, length);
        }
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream inputStream, int length)
        throws SQLException {
        checkOpen();
        if (inputStream == null || length < 0) {
            setString(parameterIndex, null);
        } else {
            try {
               length /= 2;
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

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length)
        throws SQLException {
        if (reader == null || length < 0) {
            setParameter(parameterIndex, null, java.sql.Types.LONGVARCHAR, 0, 0);
        } else {
            setParameter(parameterIndex, reader, java.sql.Types.LONGVARCHAR, 0, length);
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        setObjectBase(parameterIndex, x, Support.getJdbcType(x), -1);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType)
        throws SQLException {
        setObjectBase(parameterIndex, x, targetSqlType, -1);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scale)
        throws SQLException {
        checkOpen();
        if (scale < 0 || scale > connection.getMaxPrecision()) {
            throw new SQLException(Messages.get("error.generic.badscale"), "HY092");
        }
        setObjectBase(parameterIndex, x, targetSqlType, scale);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        notImplemented("PreparedStatement.setNull(int, int, String)");
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        setParameter(parameterIndex, x, java.sql.Types.VARCHAR, 0, 0);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        setParameter(parameterIndex, x, java.sql.Types.DECIMAL, -1, 0);
    }

    @Override
    public void setURL(int parameterIndex, URL url) throws SQLException {
        setString(parameterIndex, (url == null)? null: url.toString());
    }

    @Override
    public void setArray(int arg0, Array arg1) throws SQLException {
        notImplemented("PreparedStatement.setArray");
    }

    @Override
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

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        if (x == null) {
            setString(parameterIndex, null);
        } else {
            long length = x.length();

            if (length > Integer.MAX_VALUE) {
                throw new SQLException(Messages.get("error.resultset.longclob"), "24000");
            }

            setCharacterStream(parameterIndex, x.getCharacterStream(), (int) x.length());
        }
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        setParameter(parameterIndex, x, java.sql.Types.DATE, 0, 0);
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        checkOpen();

        //
        // NB. This is usable only with the JDBC3 version of the interface.
        //
        if (connection.getServerType() == Driver.SYBASE) {
            // Sybase does return the parameter types for prepared sql.
            connection.prepareSQL(this, sql, new ParamInfo[0], false, false);
        }

        try {
            Class pmdClass = Class.forName("net.sourceforge.jtds.jdbc.ParameterMetaDataImpl");
            Class[] parameterTypes = new Class[] {ParamInfo[].class, JtdsConnection.class};
            Object[] arguments = new Object[] {parameters, connection};
            Constructor pmdConstructor = pmdClass.getConstructor(parameterTypes);

            return (ParameterMetaData) pmdConstructor.newInstance(arguments);
        } catch (Exception e) {
            notImplemented("PreparedStatement.getParameterMetaData");
        }

        return null;
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        notImplemented("PreparedStatement.setRef");
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        checkOpen();
        reset();
        boolean useCursor = useCursor(false, null);

        if (procName == null && !(this instanceof JtdsCallableStatement)) {
            // Sync on the connection to make sure rollback() isn't called
            // between the moment when the statement is prepared and the moment
            // when it's executed.
            synchronized (connection) {
                String spName = connection.prepareSQL(this, sql, parameters, false, useCursor);
                return executeSQLQuery(sql, spName, parameters, useCursor);
            }
        } else {
            return executeSQLQuery(sql, procName, parameters, useCursor);
        }
    }

   @Override
   public ResultSetMetaData getMetaData()
      throws SQLException
   {
      checkOpen();

      if( colMetaData == null )
      {
         if( currentResult != null )
         {
            colMetaData = currentResult.columns;
         }
         else if( connection.getServerType() == Driver.SYBASE )
         {
            // Sybase can provide meta data as a by product of preparing the call
            connection.prepareSQL( this, sql, new ParamInfo[0], false, false );
         }
         else
         {
            // For Microsoft set all parameters to null and execute the query.
            // SET FMTONLY ON asks the server just to return meta data.
            // This only works for select statements
            if( "select".equals( sqlWord ) || "with".equals( sqlWord ) )
            {
               // copy parameters to avoid corrupting any values already set
               // by the user as we need to set a flag and null out the data.
               ParamInfo[] params = new ParamInfo[parameters.length];

               for( int i = 0; i < params.length; i ++ )
               {
                  params[i] = new ParamInfo( parameters[i].markerPos, false );
                  params[i].isSet = true;
               }

               // substitute nulls into SQL String
               StringBuilder testSql = new StringBuilder( sql.length() + 128 );
               testSql.append( "SET FMTONLY ON; " );
               testSql.append( Support.substituteParameters( sql, params, connection ) );
               testSql.append( "; SET FMTONLY OFF" );

               try
               {
                  tds.submitSQL( testSql.toString() );
                  colMetaData = tds.getColumns();
               }
               catch( SQLException e )
               {
                  // ensure FMTONLY is switched off!
                  tds.submitSQL( "SET FMTONLY OFF" );
               }
            }
         }
      }

      return colMetaData == null ? null : new JtdsResultSetMetaData( colMetaData, JtdsResultSet.getColumnCount( colMetaData ), connection.getUseLOBs() );
   }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        setParameter( parameterIndex, x, java.sql.Types.TIME, 0, 0 );
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        setParameter(parameterIndex, x, java.sql.Types.TIMESTAMP, 0, 0);
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal)
        throws SQLException {

        if (x != null && cal != null) {
            x = new java.sql.Date(Support.timeFromZone(x, cal));
        }

        setDate(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal)
        throws SQLException {

        if (x != null && cal != null) {
            x = new Time(Support.timeFromZone(x, cal));
        }

        setTime(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
        throws SQLException {

        if (x != null && cal != null) {
            x = new java.sql.Timestamp(Support.timeFromZone(x, cal));
        }

        setTimestamp(parameterIndex, x);
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        notSupported("executeUpdate(String)");
        return 0;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        notSupported("executeBatch(String)");
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        notSupported("execute(String)");
        return false;
    }

    @Override
    public int executeUpdate(String sql, int getKeys) throws SQLException {
        notSupported("executeUpdate(String, int)");
        return 0;
    }

    @Override
    public boolean execute(String arg0, int arg1) throws SQLException {
        notSupported("execute(String, int)");
        return false;
    }

    @Override
    public int executeUpdate(String arg0, int[] arg1) throws SQLException {
        notSupported("executeUpdate(String, int[])");
        return 0;
    }

    @Override
    public boolean execute(String arg0, int[] arg1) throws SQLException {
        notSupported("execute(String, int[])");
        return false;
    }

    @Override
    public int executeUpdate(String arg0, String[] arg1) throws SQLException {
        notSupported("executeUpdate(String, String[])");
        return 0;
    }

    @Override
    public boolean execute(String arg0, String[] arg1) throws SQLException {
        notSupported("execute(String, String[])");
        return false;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        notSupported("executeQuery(String)");
        return null;
    }

    /////// JDBC4 demarcation, do NOT put any JDBC3 code below this line ///////

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setAsciiStream(int, java.io.InputStream)
     */
    @Override
    public void setAsciiStream(int parameterIndex, InputStream x)
            throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setAsciiStream(int, java.io.InputStream, long)
     */
    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length)
            throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setBinaryStream(int, java.io.InputStream)
     */
    @Override
    public void setBinaryStream(int parameterIndex, InputStream x)
            throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setBinaryStream(int, java.io.InputStream, long)
     */
    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length)
            throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setBlob(int, java.io.InputStream)
     */
    @Override
    public void setBlob(int parameterIndex, InputStream inputStream)
            throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setBlob(int, java.io.InputStream, long)
     */
    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length)
            throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setCharacterStream(int, java.io.Reader)
     */
    @Override
    public void setCharacterStream(int parameterIndex, Reader reader)
            throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setCharacterStream(int, java.io.Reader, long)
     */
    @Override
    public void setCharacterStream(int parameterIndex, Reader reader,
            long length) throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setClob(int, java.io.Reader)
     */
    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setClob(int, java.io.Reader, long)
     */
    @Override
    public void setClob(int parameterIndex, Reader reader, long length)
            throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setNCharacterStream(int, java.io.Reader)
     */
    @Override
    public void setNCharacterStream(int parameterIndex, Reader value)
            throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setNCharacterStream(int, java.io.Reader, long)
     */
    @Override
    public void setNCharacterStream(int parameterIndex, Reader value,
            long length) throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setNClob(int, java.sql.NClob)
     */
    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setNClob(int, java.io.Reader)
     */
    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setNClob(int, java.io.Reader, long)
     */
    @Override
    public void setNClob(int parameterIndex, Reader reader, long length)
            throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setNString(int, java.lang.String)
     */
    @Override
    public void setNString(int parameterIndex, String value)
            throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setRowId(int, java.sql.RowId)
     */
    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

    /* (non-Javadoc)
     * @see java.sql.PreparedStatement#setSQLXML(int, java.sql.SQLXML)
     */
    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject)
            throws SQLException {
        // TODO Auto-generated method stub
        throw new AbstractMethodError();
    }

}