//
// Copyright 1998,1999 CDS Networks, Inc., Medford Oregon
//
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
// 1. Redistributions of source code must retain the above copyright
//    notice, this list of conditions and the following disclaimer.
// 2. Redistributions in binary form must reproduce the above copyright
//    notice, this list of conditions and the following disclaimer in the
//    documentation and/or other materials provided with the distribution.
// 3. All advertising materials mentioning features or use of this software
//    must display the following acknowledgement:
//      This product includes software developed by CDS Networks, Inc.
// 4. The name of CDS Networks, Inc.  may not be used to endorse or promote
//    products derived from this software without specific prior
//    written permission.
//
// THIS SOFTWARE IS PROVIDED BY CDS NETWORKS, INC. ``AS IS'' AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED.  IN NO EVENT SHALL CDS NETWORKS, INC. BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
// OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
// HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
// LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
// OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
// SUCH DAMAGE.
//


package net.sourceforge.jtds.jdbc;

import java.lang.reflect.Constructor;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Map;
import java.util.TimeZone;

/**
 * A SQL statement is pre-compiled and stored in a PreparedStatement object.
 * This object can then be used to efficiently execute this statement multiple
 * times.
 * <p>
 * <B>Note:</B> The setXXX methods for setting IN parameter values must specify
 * types that are compatible with the defined SQL type of the input parameter.
 * For instance, if the IN parameter has SQL type Integer then setInt should be
 * used.
 * <p>
 * If arbitrary parameter type conversions are required then the setObject
 * method should be used with a target SQL type.
 *
 * @author     Craig Spannring
 * @author     The FreeTDS project
 * @author     Alin Sinpalean
 * @version    $Id: PreparedStatement_base.java,v 1.36 2004-05-03 23:34:27 bheineman Exp $
 * @see        Connection#prepareStatement
 * @see        ResultSet
 */
public class PreparedStatement_base extends TdsStatement implements PreparedStatementHelper, PreparedStatement
{
    public final static String cvsVersion = "$Id: PreparedStatement_base.java,v 1.36 2004-05-03 23:34:27 bheineman Exp $";

    static Map typemap = null;

    String rawQueryString;
    ParameterListItem[] parameterList;

    public PreparedStatement_base(TdsConnection conn_, String sql) throws SQLException {
        this(conn_, sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    public PreparedStatement_base(TdsConnection conn_, String sql, int type, int concurrency)
    throws SQLException {
        this(conn_, type, concurrency);

        StringBuffer result = new StringBuffer(sql.length()+16); // MJH Allow for substitution
        int numberOfParameters = EscapeProcessor.parameterNativeSQL(sql, result, true);

        rawQueryString = result.toString();
        parameterList = new ParameterListItem[numberOfParameters];

        for (int i = 0; i < numberOfParameters; i++ ) {
            parameterList[i] = new ParameterListItem();
        }
    }

    public PreparedStatement_base(TdsConnection conn_, int type, int concurrency)
    throws SQLException {
        super(conn_, type, concurrency);
    }

    /**
     * In general, parameter values remain in force for repeated use of a
     * Statement. Setting a parameter value automatically clears its previous
     * value. However, in some cases it is useful to immediately release the
     * resources used by the current parameter values; this can be done by
     * calling clearParameters.
     *
     * @exception  SQLException  if a database-access error occurs.
     */
    public void clearParameters() throws SQLException {
        for (int i = 0; i < parameterList.length; i++ ) {
            parameterList[i].clear();
        }
    }

    public boolean execute() throws SQLException {
        Tds tds = getTds();
        try {
            return execute(tds);
        } finally {
            releaseTds();
        }
    }

    /**
     * Some prepared statements return multiple results; the execute method
     * handles these complex statements as well as the simpler form of
     * statements handled by executeQuery and executeUpdate.
     *
     * @exception  SQLException  if a database-access error occurs.
     * @see                      Statement#execute
     */
    public boolean execute(Tds tds) throws SQLException {
        closeResults(false);
        Procedure procedure = findOrCreateProcedure(tds);

// MJH 14/03/04 Formal parameters no longer required
//        return internalExecuteCall(procedure.getProcedureName(), procedure.getParameterList(), parameterList, tds,
//            warningChain);
        return internalExecuteCall(procedure.getProcedureName(), null, parameterList, tds,
            warningChain);
    }

    private Procedure findOrCreateProcedure(Tds tds)
            throws SQLException {
        //
        // TDS can handle prepared statements by creating a temporary
        // procedure.  Since procedure must have the datatype specified
        // in the procedure declaration we will have to defer creating
        // the actual procedure until the statement is executed.  By
        // that time we know all the types of all of the parameters.
        //
        // Map parameters to native types and assign them generated names
        ParameterUtils.createParameterMapping(
                rawQueryString, parameterList, tds, true);

        // MJH
        // Create a unique signature for this the procedure by including parameters
        StringBuffer signature = new StringBuffer();
        // Add the catalog the the signature so that the same procedure will not
        // be called for each catalog.
        signature.append(connection.getCatalog());
        signature.append(rawQueryString);

        for (int i = 0; i < parameterList.length; i++) {
            signature.append(parameterList[i].formalType);
        }

        // Find a stored procedure that is compatible with this set of parameters if one exists.
        // MJH Lookup now includes parameter list
        Procedure procedure = findCompatibleStoredProcedure(signature.toString());

        // if we don't have a suitable match then create a new temporary stored procedure
        if (procedure == null) {
            // Create the stored procedure
            // MJH Pass in the caculated signature to be used as the cache key
            procedure = new Procedure(rawQueryString, signature.toString(),
                                      parameterList, tds);

            // SAfe Submit it to the SQL Server before adding it to the cache or procedures of transaction list.
            //      Otherwise, if submitProcedure fails (e.g. because of a syntax error) it will be in our procedure
            //      cache, but not on the server.
            submitProcedure(tds, procedure);

            // store it in the procedureCache
            connection.addStoredProcedure(procedure);
        }

        return procedure;
    }

    private Procedure findCompatibleStoredProcedure(String rawQueryString)
            throws SQLException {
        return connection.findCompatibleStoredProcedure(rawQueryString);
    }

    private void submitProcedure(Tds tds, Procedure proc)
             throws SQLException {
        tds.submitProcedure(proc.getPreparedSqlString(), warningChain);
        warningChain.checkForExceptions();
    }

    /**
     * A prepared SQL query is executed and its ResultSet is returned.
     *
     * @return  a ResultSet that contains the data produced by the query; never null
     * @exception  SQLException  if a database-access error occurs.
     */
    public ResultSet executeQuery() throws SQLException {
        warningChain.clearWarnings();
        SQLWarning warn = null;

        // Try to create a CursorResultSet if needed
        if (getResultSetType() != ResultSet.TYPE_FORWARD_ONLY
                || getResultSetConcurrency() != ResultSet.CONCUR_READ_ONLY) {
            TdsConnection conn = (TdsConnection) getConnection();
            String procedureName = null;
            // SAfe We have to synchronize this, in order to avoid mixing up our
            //      actions with another CursorResultSet's and eating up its
            //      results.
            synchronized (conn.mainTdsMonitor) {
                Tds tds = conn.allocateTds(true);
                try {
                    if (this instanceof CallableStatement) {
                        procedureName = ((CallableStatement_base) this).
                                getProcedureName();
                        ParameterUtils.createParameterMapping(
                                null, parameterList, tds, false);
                    } else {
                        procedureName = findOrCreateProcedure(tds)
                                .getProcedureName();
                    }
                } finally {
                    try {
                        conn.freeTds(tds);
                    } catch (TdsException e) {
                        warningChain.addException(TdsUtil.getSQLException(null, null, e));
                    }
                }
            }

            // Check if the procedure was found
            warningChain.checkForExceptions();

            try {
                // Create the CursorResultSet
                ResultSet rs = new CursorResultSet(
                        this, procedureName, getFetchDirection(),
                        parameterList);
                cursorResults.add(rs);
                return rs;
            } catch (SQLException e) {
                // @todo Should check the error code, to make sure it was not
                //       caused by something else
                // Cursor creation failed, add a warning
                warn = new SQLWarning(
                        e.getMessage(), e.getSQLState(), e.getErrorCode());
            }
        }

        // We got here either because the requested type is FORWARD_ONLY or
        // because the cursor creation failed.
        Tds tds = getTds();

        try {
            if (!execute(tds)) {
                tds.skipToEnd();
                throw new SQLException("Was expecting a result set");
            }

            return results;
        } finally {
            if (warn != null) {
                warningChain.addWarning(warn);
            }
            releaseTds();
        }
    }

    /**
     * Execute a SQL INSERT, UPDATE or DELETE statement. In addition, SQL
     * statements that return nothing such as SQL DDL statements can be
     * executed.
     *
     * @return     either the row count for INSERT, UPDATE or
     *      DELETE; or 0 for SQL statements that return nothing
     * @exception  SQLException  if a database-access error occurs.
     */
    public int executeUpdate() throws SQLException
    {
        Tds tds = getTds();

        try {
            if ( execute( tds ) ) {
                tds.skipToEnd();
                throw new SQLException("executeUpdate can't return a result set");
            } else {
                int res;

                if (((TdsConnection) getConnection()).returnLastUpdateCount()) {
                    int lastUpdateCount = 0;

                    while ((res = getUpdateCount()) != -1) {
                        lastUpdateCount = res;

                        // If we found a ResultSet, there's a problem.
                        if (getMoreResults()) {
                            skipToEnd();
                            throw new SQLException(
                                    "executeUpdate can't return a result set");
                        }
                    }

                    return lastUpdateCount;
                } else {
                    res = getUpdateCount();
                    return (res==-1) ? 0 : res;
                }
            }
        } finally {
            releaseTds();
        }
    }

    /**
     * When a very large ASCII value is input to a LONGVARCHAR parameter, it
     * may be more practical to send it via a java.io.InputStream. JDBC will
     * read the data from the stream as needed, until it reaches end-of-file.
     * The JDBC driver will do any necessary conversion from ASCII to the
     * database char format. <P>
     *
     * <B>Note:</B> This stream object can either be a standard Java stream
     * object or your own subclass that implements the standard interface.
     *
     * @param  parameterIndex    the first parameter is 1, the second is 2, ...
     * @param  inputStream       the java input stream which contains the ASCII
     *                           parameter value
     * @param  length            the number of bytes in the stream
     * @exception  SQLException  if a database-access error occurs.
     */
    public void setAsciiStream(int parameterIndex,
                               java.io.InputStream inputStream,
                               int length)
        throws SQLException {
        if (inputStream == null) {
            setCharacterStream(parameterIndex, null, 0);
        } else {
            try {
                setCharacterStream(parameterIndex,
                    new java.io.InputStreamReader(inputStream, "ASCII"),
                    length);
            } catch (java.io.UnsupportedEncodingException e) {
                // This should never happen...
                throw TdsUtil.getSQLException("Unexpected encoding exception", null, e);
            }
        }
    }

    /**
     * Set a parameter to a java.lang.BigDecimal value. The driver converts
     * this to a SQL NUMERIC value when it sends it to the database.
     *
     * @param  parameterIndex    the first parameter is 1, the second is 2, ...
     * @param  x                 the parameter value
     * @exception  SQLException  if a database-access error occurs.
     */
    public void setBigDecimal( int parameterIndex, BigDecimal x ) throws SQLException
    {
        setParam( parameterIndex, x, Types.DECIMAL, -1 );
    }

    /**
     * When a very large binary value is input to a LONGVARBINARY parameter, it
     * may be more practical to send it via a java.io.InputStream. JDBC will
     * read the data from the stream as needed, until it reaches end-of-file.
     * <P>
     * <B>Note:</B> This stream object can either be a standard Java stream
     * object or your own subclass that implements the standard interface.
     *
     * @param  parameterIndex    the first parameter is 1, the second is 2, ...
     * @param  x                 the java input stream which contains the binary
     *      parameter value
     * @param  length            the number of bytes in the stream
     * @exception  SQLException  if a database-access error occurs.
     */
    public void setBinaryStream(int parameterIndex,
                                java.io.InputStream x,
                                int length)
             throws SQLException {
        if (x == null || length == 0) {
            setParam(parameterIndex, null, Types.BLOB, -1);
        } else {
            setParam(parameterIndex, x, Types.BLOB, length);
        }
    }

    /**
     * Set a parameter to a Java boolean value. The driver converts this to a
     * SQL BIT value when it sends it to the database.
     *
     * @param  parameterIndex    the first parameter is 1, the second is 2, ...
     * @param  x                 the parameter value
     * @exception  SQLException  if a database-access error occurs.
     */
    public void setBoolean( int parameterIndex, boolean x ) throws SQLException
    {
        setParam( parameterIndex, new Boolean( x ), Types.BIT, -1 );
    }

    /**
     * Set a parameter to a Java byte value. The driver converts this to a SQL
     * TINYINT value when it sends it to the database.
     *
     * @param  index    the first parameter is 1, the second is 2, ...
     * @param  x        the parameter value
     * @exception  SQLException  if a database-access error occurs.
     */
    public void setByte( int index, byte x ) throws SQLException
    {
        setParam( index, new Integer( x ), Types.TINYINT, -1 );
    }

    /**
     * Set a parameter to a Java array of bytes. The driver converts this to a
     * SQL VARBINARY or LONGVARBINARY (depending on the argument's size
     * relative to the driver's limits on VARBINARYs) when it sends it to the
     * database.
     *
     * @param  parameterIndex    the first parameter is 1, the second is 2, ...
     * @param  x                 the parameter value
     * @exception  SQLException  if a database-access error occurs.
     */
    public void setBytes(int parameterIndex, byte x[]) throws SQLException {
        // when this method creates the parameter the formal type should
        // be a varbinary if the length of 'x' is <=255, image if length>255.
        if (x == null || x.length <= 255 || (x.length <= 8000
            && connection.getTdsVer() == Tds.TDS70)) {
            setParam(parameterIndex, x, Types.VARBINARY, -1);
        } else {
            setParam(parameterIndex, x, Types.LONGVARBINARY, -1);
        }
    }

    /**
     * Set a parameter to a Date value. The driver converts this to a
     * SQL DATE value when it sends it to the database.
     *
     * @param  parameterIndex    the first parameter is 1, the second is 2, ...
     * @param  value             the parameter value
     * @exception  SQLException  if a database-access error occurs.
     */
    public void setDate( int parameterIndex, Date value ) throws SQLException
    {
        setParam( parameterIndex, value, Types.DATE, -1 );
    }

    /**
     * Set a parameter to a Java double value. The driver converts this to a
     * SQL DOUBLE value when it sends it to the database.
     *
     * @param  parameterIndex    the first parameter is 1, the second is 2, ...
     * @param  value             the parameter value
     * @exception  SQLException  if a database-access error occurs.
     */
    public void setDouble( int parameterIndex, double value ) throws SQLException
    {
        setParam( parameterIndex, new Double( value ), Types.DOUBLE, -1 );
    }

    /**
     * Set a parameter to a Java float value. The driver converts this to a SQL
     * FLOAT value when it sends it to the database.
     *
     * @param  parameterIndex    the first parameter is 1, the second is 2, ...
     * @param  value             the parameter value
     * @exception  SQLException  if a database-access error occurs.
     */
    public void setFloat( int parameterIndex, float value ) throws SQLException
    {
        setParam( parameterIndex, new Float( value ), Types.REAL, -1 );
    }

    /**
     * Set a parameter to a Java int value. The driver converts this to a SQL
     * INTEGER value when it sends it to the database.
     *
     * @param  index    the first parameter is 1, the second is 2, ...
     * @param  value    the parameter value
     * @exception  SQLException  if a database-access error occurs.
     */
    public void setInt( int index, int value ) throws SQLException
    {
        setParam( index, new Integer( value ), Types.INTEGER, -1 );
    }

    /**
     * Set a parameter to a Java long value. The driver converts this to a SQL
     * BIGINT value when it sends it to the database.
     *
     * @param  parameterIndex    the first parameter is 1, the second is 2, ...
     * @param  value             the parameter value
     * @exception  SQLException  if a database-access error occurs.
     */
    public void setLong( int parameterIndex, long value ) throws SQLException
    {
        setParam(parameterIndex, new Long( value ), Types.BIGINT, -1);
    }

    /**
     * Set a parameter to SQL NULL. <P>
     *
     * <B>Note:</B> You must specify the parameter's SQL type.
     *
     * @param  index    the first parameter is 1, the second is 2, ...
     * @param  type     SQL type code defined by Types
     * @exception  SQLException  if a database-access error occurs.
     */
    public void setNull( int index, int type ) throws SQLException
    {
        setParam( index, null, type, -1 );
    }

    /**
     * Set the value of a parameter using an object; use the java.lang
     * equivalent objects for integral values. <p>
     *
     * The JDBC specification specifies a standard mapping from Java Object
     * types to SQL types. The given argument java object will be converted to
     * the corresponding SQL type before being sent to the database. <p>
     *
     * Note that this method may be used to pass datatabase specific abstract
     * data types, by using a Driver specific Java type.
     *
     * @param  parameterIndex    The first parameter is 1, the second is 2, ...
     * @param  x                 The object containing the input parameter value
     * @exception  SQLException  if a database-access error occurs.
     */
    public void setObject(int parameterIndex, Object x) throws SQLException {
        if( x == null )
            setNull( parameterIndex, Types.VARCHAR );
        else
            if( x instanceof java.lang.String )
                setString( parameterIndex, (String)x );
        else
            if( x instanceof java.math.BigDecimal )
                setBigDecimal( parameterIndex, (BigDecimal) x );
        else
            if( x instanceof java.lang.Integer )
                setInt( parameterIndex, ((Number)x).intValue() );
        else
            if( x instanceof java.lang.Long )
                setLong( parameterIndex, ((Number)x).longValue() );
        else
            if( x instanceof java.lang.Byte )
                setByte( parameterIndex, ((Number)x).byteValue() );
        else
            if( x instanceof java.lang.Short )
                setShort( parameterIndex, ((Number)x).shortValue() );
        else
            if( x instanceof java.lang.Boolean )
                setBoolean( parameterIndex, ((Boolean)x).booleanValue() );
        else
            if( x instanceof java.lang.Double )
                setDouble( parameterIndex, ((Number)x).doubleValue() );
        else
            if( x instanceof java.lang.Float )
                setFloat( parameterIndex, ((Number)x).floatValue() );
        else
            if (x instanceof Blob)
                setBlob(parameterIndex, (Blob) x);
        else
            if (x instanceof Clob)
                setClob(parameterIndex, (Clob) x);
        else
            if( x instanceof Date )
                setDate( parameterIndex, (Date)x );
        else
            if( x instanceof Time )
                setTime( parameterIndex, (Time)x );
        else
            if( x instanceof Timestamp )
                setTimestamp( parameterIndex, (Timestamp)x );
        else
        {
            Class c = x.getClass();

            if( c.isArray() && c.getComponentType().equals( byte.class ) )
                setBytes( parameterIndex, (byte[])x );
            else
                throw new SQLException("Not implemented");
        }
    }

    /**
     * This method is like setObject above, but assumes a scale of zero.
     *
     * @exception  SQLException  if a database-access error occurs.
     */
    public void setObject(int parameterIndex, Object x, int targetSqlType)
    throws SQLException {
        setObject(parameterIndex, x, targetSqlType, -1);
    }

    /**
     * Initialize one element in the parameter list
     *
     * @param  index  (in-only) index (first column is 1) of the parameter
     * @param  value  (in-only)
     * @param  type   (in-only) JDBC type
     */
    protected void setParam(
            int index,
            Object value,
            int type,
            int scale )
             throws SQLException
    {
        if ( index < 1 ) {
            throw new SQLException( "Invalid Parameter index "
                     + index + ".  JDBC indexes start at 1." );
        }
        if ( index > parameterList.length ) {
            throw new SQLException( "Invalid Parameter index "
                     + index + ".  This statement only has "
                     + parameterList.length + " parameters" );
        }

        // JDBC indexes start at 1, java array indexes start at 0 :-(
        index--;

        parameterList[index].type = type;
        parameterList[index].isSet = true;
        parameterList[index].value = value;

        parameterList[index].scale = scale;
    }

    protected static Map getTypemap()
    {
        if ( typemap != null ) {
            return typemap;
        }

        Map map = new java.util.HashMap( 15 );
        map.put( BigDecimal.class, new Integer( Types.NUMERIC ) );
        map.put( Boolean.class, new Integer( Types.BIT ) );
        map.put( Byte.class, new Integer( Types.TINYINT ) );
        map.put( byte[].class, new Integer( Types.VARBINARY ) );
        map.put( Date.class, new Integer( Types.DATE ) );
        map.put( Double.class, new Integer( Types.DOUBLE ) );
        map.put( float.class, new Integer( Types.REAL ) );
        map.put( Integer.class, new Integer( Types.INTEGER ) );
        map.put( Long.class, new Integer( Types.BIGINT ) );
        map.put( Short.class, new Integer( Types.SMALLINT ) );
        map.put( String.class, new Integer( Types.VARCHAR ) );
        map.put( Timestamp.class, new Integer( Types.TIMESTAMP ) );

        typemap = map;
        return typemap;
    }

    protected static int getType( Object o ) throws SQLException
    {
        if ( o == null ) {
            throw new SQLException( "You must specify a type for a null parameter" );
        }

        Map map = getTypemap();
        Object ot = map.get( o.getClass() );
        if ( ot == null ) {
            throw new SQLException( "Support for this type is not implemented" );
        }
        return ( ( Integer ) ot ).intValue();
    }

    //----------------------------------------------------------------------
    // Advanced features:

    /**
     * Set the value of a parameter using an object; use the java.lang
     * equivalent objects for integral values. <p>
     *
     * The given Java object will be converted to the targetSqlType before
     * being sent to the database. <p>
     *
     * Note that this method may be used to pass datatabase- specific abstract
     * data types. This is done by using a Driver- specific Java type and using
     * a targetSqlType of types.OTHER.
     *
     * @param  parameterIndex    The first parameter is 1, the second is 2, ...
     * @param  x                 The object containing the input parameter value
     * @param  targetSqlType     The SQL type (as defined in Types) to
     *      be sent to the database. The scale argument may further qualify this
     *      type.
     * @param  scale             For Types.DECIMAL or
     *      Types.NUMERIC types this is the number of digits after the
     *      decimal. For all other types this value will be ignored,
     * @exception  SQLException  if a database-access error occurs.
     */
    public void setObject( int parameterIndex, Object x, int targetSqlType, int scale )
             throws SQLException
    {
        if ( x == null ) {
            setParam( parameterIndex, x, targetSqlType, scale );
            return;
        }
        else {
            switch ( targetSqlType ) {
                case Types.CHAR:
                case Types.VARCHAR:
                    setString(parameterIndex, (String) x);
                    break;
                case Types.REAL:
                    setFloat(parameterIndex, ((Number) x).floatValue());
                    break;
                case Types.DOUBLE:
                    setDouble( parameterIndex, ((Number) x).doubleValue() );
                    break;
                case Types.INTEGER:
                    setInt( parameterIndex, ((Number) x).intValue() );
                    break;
                case Types.BIGINT:
                    setLong( parameterIndex, ((Number) x).longValue() );
                    break;
                case Types.BLOB:
                    Blob blob = (Blob) x;

                    setBinaryStream(parameterIndex, blob.getBinaryStream(), (int) blob.length());
                    break;
                case Types.CLOB:
                    Clob clob = (Clob) x;

                    setCharacterStream(parameterIndex, clob.getCharacterStream(), (int) clob.length());
                    break;
                default:
                    setParam( parameterIndex, x, targetSqlType, scale );
            }
        }
    }

    /**
     * Set a parameter to a Java short value. The driver converts this to a SQL
     * SMALLINT value when it sends it to the database.
     *
     * @param  index    the first parameter is 1, the second is 2, ...
     * @param  value    the parameter value
     * @exception  SQLException  if a database-access error occurs.
     */
    public void setShort( int index, short value ) throws SQLException
    {
        setParam( index, new Integer( value ), Types.SMALLINT, -1 );
    }

    /**
     * Set a parameter to a Java String value. The driver converts this to a
     * SQL VARCHAR or LONGVARCHAR value (depending on the arguments size
     * relative to the driver's limits on VARCHARs) when it sends it to the
     * database.
     *
     * @param  index    the first parameter is 1, the second is 2, ...
     * @param  str      the parameter value
     * @exception  SQLException  if a database-access error occurs.
     */
    public void setString( int index, String str ) throws SQLException
    {
        setParam(index, str, Types.VARCHAR, -1);
    }

    /**
     * Set a parameter to a Time value. The driver converts this to a
     * SQL TIME value when it sends it to the database.
     *
     * @param  parameterIndex    the first parameter is 1, the second is 2, ...
     * @param  value             the parameter value
     * @exception  SQLException  if a database-access error occurs.
     */
    public void setTime( int parameterIndex, Time value )
        throws SQLException
    {
        setParam( parameterIndex, value, Types.TIME, -1 );
    }

    /**
     * Set a parameter to a Timestamp value. The driver converts this
     * to a SQL TIMESTAMP value when it sends it to the database.
     *
     * @param  index    the first parameter is 1, the second is 2, ...
     * @param  value    the parameter value
     * @exception  SQLException  if a database-access error occurs.
     */
    public void setTimestamp( int index, Timestamp value )
             throws SQLException
    {
        setParam( index, value, Types.TIMESTAMP, -1 );
    }

    /**
     * When a very large UNICODE value is input to a LONGVARCHAR parameter, it
     * may be more practical to send it via a java.io.InputStream. JDBC will
     * read the data from the stream as needed, until it reaches end-of-file.
     * The JDBC driver will do any necessary conversion from UNICODE (UTF-8)
     * to the database char format. <P>
     *
     * <B>Note:</B> This stream object can either be a standard Java stream
     * object or your own subclass that implements the standard interface.
     *
     * @param  parameterIndex    the first parameter is 1, the second is 2, ...
     * @param  inputStream       the java input stream which contains the
     *                           UNICODE (UTF-8) parameter value
     * @param  length            the number of bytes in the stream
     * @exception  SQLException  if a database-access error occurs.
     */
    public void setUnicodeStream(int parameterIndex,
                                 java.io.InputStream inputStream,
                                 int length)
        throws SQLException {
        if (inputStream == null) {
            setCharacterStream(parameterIndex, null, 0);
        } else {
            try {
                setCharacterStream(parameterIndex,
                    new java.io.InputStreamReader(inputStream, "UTF-8"),
                    length);
            } catch (java.io.UnsupportedEncodingException e) {
                // This should never happen...
                throw TdsUtil.getSQLException("Unexpected encoding exception", null, e);
            }
        }
    }

    //--------------------------JDBC 2.0-----------------------------

    /**
     * JDBC 2.0 Adds a set of parameters to the batch.
     *
     * @exception  SQLException  if a database access error occurs
     * @see                      Statement#addBatch
     */
    public synchronized void addBatch() throws SQLException {
        if (batchValues == null) {
            batchValues = new ArrayList();
        }

        batchValues.add(parameterList);

        parameterList = new ParameterListItem[parameterList.length];

        for (int i = 0; i < parameterList.length; i++) {
            parameterList[i] = new ParameterListItem();
        }
    }

    /**
     * This method should be over-ridden by any sub-classes that place values other than
     * Strings into the batchValues list to handle execution properly.
     */
    protected int executeBatchOther(Object value) throws SQLException {
        if (value instanceof ParameterListItem[]) {
            ParameterListItem[] tmpParameterListItem = parameterList;

            try {
                parameterList = (ParameterListItem[]) value;

                return executeUpdate();
            } finally {
                parameterList = tmpParameterListItem;
            }
        } else {
            super.executeBatchOther(value);
        }

        return Integer.MIN_VALUE;
    }

    /**
     * JDBC 2.0 Sets the designated parameter to the given <code>Reader</code>
     * object, which is the given number of characters long. When a very large
     * UNICODE value is input to a LONGVARCHAR parameter, it may be more
     * practical to send it via a java.io.Reader. JDBC will read the data from
     * the stream as needed, until it reaches end-of-file. The JDBC driver will
     * do any necessary conversion from UNICODE to the database char format.
     * <P>
     *
     * <B>Note:</B> This stream object can either be a standard Java stream
     * object or your own subclass that implements the standard interface.
     *
     * @param  parameterIndex    the first parameter is 1, the second is 2, ...
     * @param  reader            the java reader which contains the UNICODE data
     * @param  length            the number of characters in the stream
     * @exception  SQLException  if a database access error occurs
     */
    public void setCharacterStream(int parameterIndex, java.io.Reader reader, int length)
        throws SQLException {
        if (reader == null || length == 0) {
            setParam(parameterIndex, null, Types.CLOB, -1);
        } else {
            setParam(parameterIndex, reader, Types.CLOB, length);
        }
    }

    /**
     * JDBC 2.0 Sets a REF(&lt;structured-type&gt;) parameter.
     *
     * @param  i                 the first parameter is 1, the second is 2, ...
     * @param  x                 an object representing data of an SQL REF Type
     * @exception  SQLException  if a database access error occurs
     */
    public void setRef( int i, Ref x ) throws SQLException
    {
        NotImplemented();
    }

    /**
     * JDBC 2.0 Sets a BLOB parameter.
     *
     * @param  parameterIndex    the first parameter is 1, the second is 2, ...
     * @param  blob              an object representing a BLOB
     * @exception  SQLException  if a database access error occurs
     */
    public void setBlob(int parameterIndex, Blob blob)
    throws SQLException {
        if (blob == null) {
            setBinaryStream(parameterIndex, null, 0);
        } else {
            long length = blob.length();

            if (length > Integer.MAX_VALUE) {
                throw new SQLException("Blob lengths greater than " + Integer.MAX_VALUE
                    + " are not suported.");
            }

            setBinaryStream(parameterIndex, blob.getBinaryStream(), (int) length);
        }
    }

    /**
     * JDBC 2.0 Sets a CLOB parameter.
     *
     * @param  parameterIndex    the first parameter is 1, the second is 2, ...
     * @param  clob              an object representing a CLOB
     * @exception  SQLException  if a database access error occurs
     */
    public void setClob(int parameterIndex, Clob clob)
        throws SQLException {
        if (clob == null) {
            setCharacterStream(parameterIndex, null, 0);
        } else {
            long length = clob.length();

            if (length > Integer.MAX_VALUE) {
                throw new SQLException("Clob lengths greater than " + Integer.MAX_VALUE
                    + " are not suported.");
            }

            setCharacterStream(parameterIndex, clob.getCharacterStream(), (int) length);
        }
    }

    /**
     * JDBC 2.0 Sets an Array parameter.
     *
     * @param  i                 the first parameter is 1, the second is 2, ...
     * @param  x                 an object representing an SQL array
     * @exception  SQLException  if a database access error occurs
     */
    public void setArray( int i, Array x ) throws SQLException
    {
        NotImplemented();
    }

    /**
     * JDBC 2.0 Gets the number, types and properties of a ResultSet's columns.
     *
     * @return                   the description of a ResultSet's columns
     * @exception  SQLException  if a database access error occurs
     */
    public ResultSetMetaData getMetaData() throws SQLException {
        ResultSet resultSet = null;

        try {
            resultSet = executeQuery();
            return resultSet.getMetaData();
        } catch (SQLException e) {
            // Ignore, the specification indicates that a null should be returned
            // if the ResultSetMetaData cannot be obtained.
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
        }

        return null;
    }

    /**
     * JDBC 2.0 Sets the designated parameter to a Date value, using
     * the given <code>Calendar</code> object. The driver uses the <code>Calendar</code>
     * object to construct an SQL DATE, which the driver then sends to the
     * database. With a a <code>Calendar</code> object, the driver can
     * calculate the date taking into account a custom timezone and locale. If
     * no <code>Calendar</code> object is specified, the driver uses the
     * default timezone and locale.
     *
     * @param  parameterIndex the first parameter is 1, the second is 2, ...
     * @param  date the parameter value
     * @param  calendar the <code>Calendar</code> object the driver will
     *      use to construct the date
     * @exception  SQLException  if a database access error occurs
     */
    public void setDate(int parameterIndex, Date date, Calendar calendar)
             throws SQLException {
        if (date != null && calendar != null) {
            TimeZone timeZone = TimeZone.getDefault();
            long newTime = date.getTime();

            newTime -= timeZone.getRawOffset();
            newTime += calendar.getTimeZone().getRawOffset();
            date = new Date(newTime);
        }

        setDate(parameterIndex, date);
    }

    /**
     * JDBC 2.0 Sets the designated parameter to a Time value, using
     * the given <code>Calendar</code> object. The driver uses the <code>Calendar</code>
     * object to construct an SQL TIME, which the driver then sends to the
     * database. With a a <code>Calendar</code> object, the driver can
     * calculate the time taking into account a custom timezone and locale. If
     * no <code>Calendar</code> object is specified, the driver uses the
     * default timezone and locale.
     *
     * @param  parameterIndex the first parameter is 1, the second is 2, ...
     * @param  time the parameter value
     * @param  calendar the <code>Calendar</code> object the driver will
     *      use to construct the time
     * @exception  SQLException  if a database access error occurs
     */
    public void setTime(int parameterIndex, Time time, Calendar calendar)
             throws SQLException {
        if (time != null && calendar != null) {
            TimeZone timeZone = TimeZone.getDefault();
            long newTime = time.getTime();

            newTime -= timeZone.getRawOffset();
            newTime += calendar.getTimeZone().getRawOffset();
            time = new Time(newTime);
        }

        setTime(parameterIndex, time);
    }

    /**
     * JDBC 2.0 Sets the designated parameter to a Timestamp value,
     * using the given <code>Calendar</code> object. The driver uses the <code>Calendar</code>
     * object to construct an SQL TIMESTAMP, which the driver then sends to the
     * database. With a a <code>Calendar</code> object, the driver can
     * calculate the timestamp taking into account a custom timezone and
     * locale. If no <code>Calendar</code> object is specified, the driver uses
     * the default timezone and locale.
     *
     * @param  parameterIndex    the first parameter is 1, the second is 2, ...
     * @param  timestamp the parameter value
     * @param  calendar the <code>Calendar</code> object the driver will
     *      use to construct the timestamp
     * @exception  SQLException  if a database access error occurs
     */
    public void setTimestamp(int parameterIndex,
                             Timestamp timestamp,
                             Calendar calendar)
             throws SQLException {
        if (timestamp != null && calendar != null) {
            TimeZone timeZone = TimeZone.getDefault();
            long newTime = timestamp.getTime();

            newTime -= timeZone.getRawOffset();
            newTime += calendar.getTimeZone().getRawOffset();
            timestamp = new Timestamp(newTime);
        }

        setTimestamp(parameterIndex, timestamp);
    }

    /**
     * JDBC 2.0 Sets the designated parameter to SQL NULL. This version of
     * setNull should be used for user-named types and REF type parameters.
     * Examples of user-named types include: STRUCT, DISTINCT, JAVA_OBJECT, and
     * named array types. <P>
     *
     * <B>Note:</B> To be portable, applications must give the SQL type code
     * and the fully-qualified SQL type name when specifying a NULL
     * user-defined or REF parameter. In the case of a user-named type the name
     * is the type name of the parameter itself. For a REF parameter the name
     * is the type name of the referenced type. If a JDBC driver does not need
     * the type code or type name information, it may ignore it. Although it is
     * intended for user-named and Ref parameters, this method may be used to
     * set a null parameter of any JDBC type. If the parameter does not have a
     * user-named or REF type, the given typeName is ignored.
     *
     * @param  paramIndex        the first parameter is 1, the second is 2, ...
     * @param  sqlType           a value from Types
     * @param  typeName          the fully-qualified name of an SQL user-named
     *      type, ignored if the parameter is not a user-named type or REF
     * @exception  SQLException  if a database access error occurs
     */
    public void setNull( int paramIndex, int sqlType, String typeName )
             throws SQLException
    {
        NotImplemented();
    }

    public ParameterMetaData getParameterMetaData() throws SQLException {
        try {
            Class pmdClass = Class.forName("net.sourceforge.jtds.jdbc.ParameterMetaDataImpl");
            Class[] parameterTypes = new Class[] {ParameterListItem[].class};
            Object[] arguments = new Object[] {parameterList};
            Constructor pmdConstructor = pmdClass.getConstructor(parameterTypes);

            return (ParameterMetaData) pmdConstructor.newInstance(arguments);
        } catch (Exception e) {
            NotImplemented();
        }

        return null;
    }

    public void setURL(int param, java.net.URL url) throws SQLException
    {
        NotImplemented();
    }

}
