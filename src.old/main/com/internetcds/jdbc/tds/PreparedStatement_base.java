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


package com.internetcds.jdbc.tds;

import java.sql.*;
import java.math.BigDecimal;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.Calendar;
import java.util.Map;

/**
 *  <P>
 *
 *  A SQL statement is pre-compiled and stored in a PreparedStatement object.
 *  This object can then be used to efficiently execute this statement multiple
 *  times. <P>
 *
 *  <B>Note:</B> The setXXX methods for setting IN parameter values must specify
 *  types that are compatible with the defined SQL type of the input parameter.
 *  For instance, if the IN parameter has SQL type Integer then setInt should be
 *  used. <p>
 *
 *  If arbitrary parameter type conversions are required then the setObject
 *  method should be used with a target SQL type.
 *
 *@author     Craig Spannring
 *@author     The FreeTDS project
 *@version    $Id: PreparedStatement_base.java,v 1.8 2001/09/24 08:45:10
 *      aschoerk Exp $
 *@see        Connection#prepareStatement
 *@see        ResultSet
 */
public class PreparedStatement_base
         extends TdsStatement
         implements PreparedStatementHelper, java.sql.PreparedStatement {
    public final static String cvsVersion = "$Id: PreparedStatement_base.java,v 1.20 2002-09-26 14:10:31 alin_sinpalean Exp $";

    String rawQueryString = null;
    // Vector               procedureCache     = null;  put it in tds
    ParameterListItem[] parameterList = null;
    static Map typemap = null;

    public PreparedStatement_base(
            TdsConnection conn_,
            String sql )
             throws SQLException
    {
        this( conn_, sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY );
    }


    public PreparedStatement_base(
            TdsConnection conn_,
            String sql,
            int type,
            int concurrency )
             throws SQLException
    {
        super( conn_, type, concurrency );

        rawQueryString = conn_.nativeSQL(sql);

        int i;
        int numberOfParameters = ParameterUtils.countParameters( rawQueryString );

        parameterList = new ParameterListItem[numberOfParameters];
        for ( i = 0; i < numberOfParameters; i++ ) {
            parameterList[i] = new ParameterListItem();
        }

        // procedureCache = new Vector();
    }


    protected void NotImplemented() throws java.sql.SQLException
    {
        throw new SQLException( "Not Implemented" );
    }


    /**
     *  <P>
     *
     *  In general, parameter values remain in force for repeated use of a
     *  Statement. Setting a parameter value automatically clears its previous
     *  value. However, in some cases it is useful to immediately release the
     *  resources used by the current parameter values; this can be done by
     *  calling clearParameters.
     *
     *@exception  SQLException  if a database-access error occurs.
     */
    public void clearParameters() throws SQLException
    {
        int i;
        for ( i = 0; i < parameterList.length; i++ ) {
            parameterList[i].clear();
        }
    }


    /* Cache connected to tds
   public void dropAllProcedures()
   {
      procedureCache = null;
      procedureCache = new Vector();
   }
    */
    public boolean execute() throws SQLException
    {
        Tds tds = getTds( rawQueryString );
        return execute( tds );
    }


    /**
     *  Some prepared statements return multiple results; the execute method
     *  handles these complex statements as well as the simpler form of
     *  statements handled by executeQuery and executeUpdate.
     *
     *@exception  SQLException  if a database-access error occurs.
     *@see                      Statement#execute
     */
    public boolean execute( Tds tds ) throws SQLException
    {
        //
        // TDS can handle prepared statements by creating a temporary
        // procedure.  Since procedure must have the datatype specified
        // in the procedure declaration we will have to defer creating
        // the actual procedure until the statement is executed.  By
        // that time we know all the types of all of the parameters.
        //

        Procedure procedure = null;
        boolean result = false;

        // SAfe No need for this either. We'll have to consume all input, nut
        //      just the last ResultSet (if one exists).
//        closeResults(false);
        // SAfe No need for this. getMoreResults sets it to -1 at start, anyway
//        updateCount = -2;

        // First make sure the caller has filled in all the parameters.
        ParameterUtils.verifyThatParametersAreSet( parameterList );

        // Find a stored procedure that is compatible with this set of
        // parameters if one exists.
        procedure = findCompatibleStoredProcedure( tds, rawQueryString );
        // now look in tds

        // if we don't have a suitable match then create a new
        // temporary stored procedure
        if ( procedure == null ) {
            // create the stored procedure
            procedure = new Procedure( rawQueryString,
                    tds.getUniqueProcedureName(),
                    parameterList, tds );

            // store it in the procedureCache
            // procedureCache.addElement(procedure);
            // store it in the procedureCache
            tds.procedureCache.put( rawQueryString, procedure );
            // MJH Only record the proc name if in manual commit mode
            if( !getConnection().getAutoCommit() ) // MJH
                tds.proceduresOfTra.add( procedure );

            // create it on the SQLServer.
            submitProcedure( tds, procedure );
        }
        result = executeCall( tds,
                procedure.getProcedureName(),
                procedure.getParameterList(),
        // formal params
        parameterList );
        // actual params

        return result;
    }


    protected boolean executeCall(
            String name,
            ParameterListItem[] formalParameterList,
            ParameterListItem[] actualParameterList )
             throws SQLException
    {
        Tds tds = getTds( "UPDATE " );
        return executeCall( tds, name, formalParameterList, actualParameterList );
    }


    protected boolean executeCall(
            Tds tds,
            String name,
            ParameterListItem[] formalParameterList,
            ParameterListItem[] actualParameterList )
             throws SQLException
    {
        // SAfe This is where all outstanding results must be skipped, to make
        //      sure they don't interfere with the the current ones.
        skipToEnd();

        boolean result;

        try {
            SQLException exception = null;
            PacketResult tmp = null;

            // execute the stored procedure.
            tds.executeProcedure( name,
                    formalParameterList,
                    actualParameterList,
                    this,
                    getQueryTimeout() );

            result = getMoreResults(tds, true);
        }
        catch ( TdsException e ) {
            throw new SQLException( e.toString() );
        }
        catch ( java.io.IOException e ) {
            throw new SQLException( e.toString() );
        }
        finally {
            tds.comm.packetType = 0;
        }

        return result;
    }



    private Procedure findCompatibleStoredProcedure( Tds tds, String rawQueryString )
             throws SQLException
    {
        Procedure procedure = null;
        int i;

        procedure = ( Procedure ) tds.procedureCache.get( rawQueryString );
        return procedure;
    }


    private void submitProcedure( Tds tds, Procedure proc )
             throws SQLException
    {
        String sql = proc.getPreparedSqlString();
        tds.submitProcedure( sql, warningChain );
    }


    /**
     *  A prepared SQL query is executed and its ResultSet is returned.
     *
     *@return                   a ResultSet that contains the data produced by
     *      the query; never null
     *@exception  SQLException  if a database-access error occurs.
     */
    public java.sql.ResultSet executeQuery() throws SQLException
    {
//        closeResults(false);

        Tds tds = getTds( rawQueryString );

        if( !execute(tds) )
        {
            skipToEnd();
            releaseTds();
            throw new SQLException( "Was expecting a result set" );
        }

        return results;
    }


    /**
     *  Execute a SQL INSERT, UPDATE or DELETE statement. In addition, SQL
     *  statements that return nothing such as SQL DDL statements can be
     *  executed.
     *
     *@return                   either the row count for INSERT, UPDATE or
     *      DELETE; or 0 for SQL statements that return nothing
     *@exception  SQLException  if a database-access error occurs.
     */
    public int executeUpdate() throws SQLException
    {
//        closeResults(false);

        Tds tds = getTds( "UPDATE " );
        if ( execute( tds ) ) {
            skipToEnd();
            releaseTds();
            throw new SQLException( "executeUpdate can't return a result set" );
        }
        else {
            int res = getUpdateCount();
            releaseTds();
            return res;
        }
    }


    /**
     *  When a very large ASCII value is input to a LONGVARCHAR parameter, it
     *  may be more practical to send it via a java.io.InputStream. JDBC will
     *  read the data from the stream as needed, until it reaches end-of-file.
     *  The JDBC driver will do any necessary conversion from ASCII to the
     *  database char format. <P>
     *
     *  <B>Note:</B> This stream object can either be a standard Java stream
     *  object or your own subclass that implements the standard interface.
     *
     *@param  parameterIndex    the first parameter is 1, the second is 2, ...
     *@param  x                 the java input stream which contains the ASCII
     *      parameter value
     *@param  length            the number of bytes in the stream
     *@exception  SQLException  if a database-access error occurs.
     */
    public void setAsciiStream( int parameterIndex,
            java.io.InputStream x,
            int length )
             throws SQLException
    {
        NotImplemented();
    }


    /**
     *  Set a parameter to a java.lang.BigDecimal value. The driver converts
     *  this to a SQL NUMERIC value when it sends it to the database.
     *
     *@param  parameterIndex    the first parameter is 1, the second is 2, ...
     *@param  x                 the parameter value
     *@exception  SQLException  if a database-access error occurs.
     */
    public void setBigDecimal( int parameterIndex, BigDecimal x ) throws SQLException
    {
        setParam( parameterIndex, x, java.sql.Types.DECIMAL, -1 );
    }


    /**
     *  When a very large binary value is input to a LONGVARBINARY parameter, it
     *  may be more practical to send it via a java.io.InputStream. JDBC will
     *  read the data from the stream as needed, until it reaches end-of-file.
     *  <P>
     *
     *  <B>Note:</B> This stream object can either be a standard Java stream
     *  object or your own subclass that implements the standard interface.
     *
     *@param  parameterIndex    the first parameter is 1, the second is 2, ...
     *@param  x                 the java input stream which contains the binary
     *      parameter value
     *@param  length            the number of bytes in the stream
     *@exception  SQLException  if a database-access error occurs.
     */
    public void setBinaryStream( int parameterIndex,
            java.io.InputStream x,
            int length )
             throws SQLException
    {
        if ( length == 0 ) {
            setBytes( parameterIndex, null );
        }
        byte[] bs = new byte[length];
        int actlen;
        try {
            actlen = x.read( bs );
        }
        catch ( java.io.IOException e ) {
            SQLException newE = new SQLException( "setBinaryStream: IO-Exception occured reading Stream" + e.toString() );
            throw newE;
        }
        if ( actlen != length ) {
            throw new SQLException( "SetBinaryStream parameterized Length: " + Integer.toString( length ) + " got length: " + Integer.toString( actlen ) );
        }
        else {
            try {
                actlen = x.read( bs );
            }
            catch ( java.io.IOException e ) {
                SQLException newE = new SQLException( "setBinaryStream: IO-Exception occured reading Stream" + e.toString() );
                throw newE;
            }
            if ( actlen != -1 ) {
                throw new SQLException( "SetBinaryStream parameterized Length: " + Integer.toString( length ) + " got more than that " );
            }
        }
        this.setBytes( parameterIndex, bs );
    }


    /**
     *  Set a parameter to a Java boolean value. The driver converts this to a
     *  SQL BIT value when it sends it to the database.
     *
     *@param  parameterIndex    the first parameter is 1, the second is 2, ...
     *@param  x                 the parameter value
     *@exception  SQLException  if a database-access error occurs.
     */
    public void setBoolean( int parameterIndex, boolean x ) throws SQLException
    {
        setParam( parameterIndex, new Boolean( x ), java.sql.Types.BIT, -1 );
    }


    /**
     *  Set a parameter to a Java byte value. The driver converts this to a SQL
     *  TINYINT value when it sends it to the database.
     *
     *@param  parameterIndex    the first parameter is 1, the second is 2, ...
     *@param  x                 the parameter value
     *@exception  SQLException  if a database-access error occurs.
     */
    public void setByte( int index, byte x ) throws SQLException
    {
        setParam( index, new Integer( x ), java.sql.Types.SMALLINT, -1 );
    }


    /**
     *  Set a parameter to a Java array of bytes. The driver converts this to a
     *  SQL VARBINARY or LONGVARBINARY (depending on the argument's size
     *  relative to the driver's limits on VARBINARYs) when it sends it to the
     *  database.
     *
     *@param  parameterIndex    the first parameter is 1, the second is 2, ...
     *@param  x                 the parameter value
     *@exception  SQLException  if a database-access error occurs.
     */
    public void setBytes(int parameterIndex, byte x[]) throws SQLException
    {
        // when this method creates the parameter the formal type should
        // be a varbinary if the length of 'x' is <=255, image if length>255.
        if( x==null || x.length<=255 || (x.length<=8000 &&
            ((TdsConnection)getConnection()).getTdsVer()==Tds.TDS70) )
            setParam( parameterIndex, x, java.sql.Types.VARBINARY, -1 );
        else
            setParam( parameterIndex, x, java.sql.Types.LONGVARBINARY, -1 );
    }


    /**
     *  Set a parameter to a java.sql.Date value. The driver converts this to a
     *  SQL DATE value when it sends it to the database.
     *
     *@param  parameterIndex    the first parameter is 1, the second is 2, ...
     *@param  x                 the parameter value
     *@exception  SQLException  if a database-access error occurs.
     */
    public void setDate( int parameterIndex, java.sql.Date value )
             throws SQLException
    {
        setParam( parameterIndex, value, java.sql.Types.DATE, -1 );
    }


    /**
     *  Set a parameter to a Java double value. The driver converts this to a
     *  SQL DOUBLE value when it sends it to the database.
     *
     *@param  parameterIndex    the first parameter is 1, the second is 2, ...
     *@param  x                 the parameter value
     *@exception  SQLException  if a database-access error occurs.
     */
    public void setDouble( int parameterIndex, double value ) throws SQLException
    {
        setParam( parameterIndex, new Double( value ), java.sql.Types.DOUBLE, -1 );
    }


    /**
     *  Set a parameter to a Java float value. The driver converts this to a SQL
     *  FLOAT value when it sends it to the database.
     *
     *@param  parameterIndex    the first parameter is 1, the second is 2, ...
     *@param  x                 the parameter value
     *@exception  SQLException  if a database-access error occurs.
     */
    public void setFloat( int parameterIndex, float value ) throws SQLException
    {
        setParam( parameterIndex, new Float( value ), java.sql.Types.REAL, -1 );
    }


    /**
     *  Set a parameter to a Java int value. The driver converts this to a SQL
     *  INTEGER value when it sends it to the database.
     *
     *@param  parameterIndex    the first parameter is 1, the second is 2, ...
     *@param  x                 the parameter value
     *@exception  SQLException  if a database-access error occurs.
     */
    public void setInt( int index, int value ) throws SQLException
    {
        setParam( index, new Integer( value ), java.sql.Types.INTEGER, -1 );
    }


    /**
     *  Set a parameter to a Java long value. The driver converts this to a SQL
     *  BIGINT value when it sends it to the database.
     *
     *@param  parameterIndex    the first parameter is 1, the second is 2, ...
     *@param  x                 the parameter value
     *@exception  SQLException  if a database-access error occurs.
     */
    public void setLong( int parameterIndex, long value ) throws SQLException
    {
        if ( value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE ) {
            setParam( parameterIndex, new Integer( ( int ) value ), java.sql.Types.INTEGER, -1 );
        }
        else {
            setParam( parameterIndex, new Long( value ), java.sql.Types.NUMERIC, 0 );
        }
    }


    /**
     *  Set a parameter to SQL NULL. <P>
     *
     *  <B>Note:</B> You must specify the parameter's SQL type.
     *
     *@param  parameterIndex    the first parameter is 1, the second is 2, ...
     *@param  sqlType           SQL type code defined by java.sql.Types
     *@exception  SQLException  if a database-access error occurs.
     */
    public void setNull( int index, int type ) throws SQLException
    {
        setParam( index, null, type, -1 );
    }


    /**
     *  <p>
     *
     *  Set the value of a parameter using an object; use the java.lang
     *  equivalent objects for integral values. <p>
     *
     *  The JDBC specification specifies a standard mapping from Java Object
     *  types to SQL types. The given argument java object will be converted to
     *  the corresponding SQL type before being sent to the database. <p>
     *
     *  Note that this method may be used to pass datatabase specific abstract
     *  data types, by using a Driver specific Java type.
     *
     *@param  parameterIndex    The first parameter is 1, the second is 2, ...
     *@param  x                 The object containing the input parameter value
     *@exception  SQLException  if a database-access error occurs.
     */
    public void setObject( int parameterIndex, Object x ) throws SQLException
    {
        if ( x == null ) {
            setNull( parameterIndex, java.sql.Types.VARCHAR );
        }
        else
                if ( x instanceof java.lang.String ) {
            setString( parameterIndex, ( String ) x );
        }
        else
                if ( x instanceof java.math.BigDecimal ) {
            setBigDecimal( parameterIndex, ( BigDecimal ) x );
        }
        else
                if ( x instanceof java.lang.Integer ) {
            setInt( parameterIndex, ( ( Number ) x ).intValue() );
        }
        else
                if ( x instanceof java.lang.Long ) {
            setLong( parameterIndex, ( ( Number ) x ).longValue() );
        }
        else
                if ( x instanceof java.lang.Byte ) {
            setByte( parameterIndex, ( ( Number ) x ).byteValue() );
        }
        else
                if ( x instanceof java.lang.Short ) {
            setShort( parameterIndex, ( ( Number ) x ).shortValue() );
        }
        else
                if ( x instanceof java.lang.Boolean ) {
            setBoolean( parameterIndex, ( ( Boolean ) x ).booleanValue() );
        }
        else
                if ( x instanceof java.lang.Double ) {
            setDouble( parameterIndex, ( ( Number ) x ).doubleValue() );
        }
        else
                if ( x instanceof java.lang.Float ) {
            setFloat( parameterIndex, ( ( Number ) x ).floatValue() );
        }
        else
                if ( x instanceof java.sql.Date ) {
            setDate( parameterIndex, ( java.sql.Date ) x );
        }
        else
                if ( x instanceof java.util.Date ) {
            setTimestamp( parameterIndex, new Timestamp( ( ( java.util.Date ) x ).getTime() ) );
        }
        else {
            Class c = x.getClass();
            if ( c.isArray() && c.getComponentType().equals( byte.class ) ) {
                setBytes( parameterIndex, ( byte[] ) x );
            }
            else {
                throw new SQLException( "Not implemented" );
            }
        }
    }


    /**
     *  This method is like setObject above, but assumes a scale of zero.
     *
     *@exception  SQLException  if a database-access error occurs.
     */
    public void setObject( int parameterIndex, Object x, int targetSqlType ) throws SQLException
    {
        setObject( parameterIndex, x, targetSqlType, 0 );
    }


    /**
     *  initialize one element in the parameter list
     *
     *@param  index  (in-only) index (first column is 1) of the parameter
     *@param  value  (in-only)
     *@param  type   (in-only) JDBC type
     */
    protected void setParam(
            int index,
            Object value,
            int type,
            int strLength )
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

        parameterList[index].maxLength = strLength;
    }
    // setParam()


    protected static Map getTypemap()
    {
        if ( typemap != null ) {
            return typemap;
        }

        Map map = new java.util.HashMap( 15 );
        map.put( BigDecimal.class, new Integer( java.sql.Types.NUMERIC ) );
        map.put( Boolean.class, new Integer( java.sql.Types.BIT ) );
        map.put( Byte.class, new Integer( java.sql.Types.TINYINT ) );
        map.put( byte[].class, new Integer( java.sql.Types.VARBINARY ) );
        map.put( java.sql.Date.class, new Integer( java.sql.Types.DATE ) );
        map.put( Double.class, new Integer( java.sql.Types.DOUBLE ) );
        map.put( float.class, new Integer( java.sql.Types.REAL ) );
        map.put( Integer.class, new Integer( java.sql.Types.INTEGER ) );
        map.put( Long.class, new Integer( java.sql.Types.NUMERIC ) );
        map.put( Short.class, new Integer( java.sql.Types.SMALLINT ) );
        map.put( String.class, new Integer( java.sql.Types.VARCHAR ) );
        map.put( java.sql.Timestamp.class, new Integer( java.sql.Types.TIMESTAMP ) );

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
     *  <p>
     *
     *  Set the value of a parameter using an object; use the java.lang
     *  equivalent objects for integral values. <p>
     *
     *  The given Java object will be converted to the targetSqlType before
     *  being sent to the database. <p>
     *
     *  Note that this method may be used to pass datatabase- specific abstract
     *  data types. This is done by using a Driver- specific Java type and using
     *  a targetSqlType of java.sql.types.OTHER.
     *
     *@param  parameterIndex    The first parameter is 1, the second is 2, ...
     *@param  x                 The object containing the input parameter value
     *@param  targetSqlType     The SQL type (as defined in java.sql.Types) to
     *      be sent to the database. The scale argument may further qualify this
     *      type.
     *@param  scale             For java.sql.Types.DECIMAL or
     *      java.sql.Types.NUMERIC types this is the number of digits after the
     *      decimal. For all other types this value will be ignored,
     *@exception  SQLException  if a database-access error occurs.
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
                case java.sql.Types.CHAR:
                case java.sql.Types.VARCHAR:
                    setString( parameterIndex, ( String ) x );
                    break;
                case java.sql.Types.REAL:
                    setFloat( parameterIndex, ( ( Float ) x ).floatValue() );
                    break;
                case java.sql.Types.DOUBLE:
                    setDouble( parameterIndex, ( ( Double ) x ).doubleValue() );
                    break;
                case java.sql.Types.INTEGER:
                    setInt( parameterIndex, ( ( Integer ) x ).intValue() );
                    break;
                case java.sql.Types.BIGINT:
                    setLong( parameterIndex, ( ( Long ) x ).longValue() );
                    break;
                default:
                    setParam( parameterIndex, x, targetSqlType, scale );
            }
        }
    }


    /**
     *  Set a parameter to a Java short value. The driver converts this to a SQL
     *  SMALLINT value when it sends it to the database.
     *
     *@param  parameterIndex    the first parameter is 1, the second is 2, ...
     *@param  x                 the parameter value
     *@exception  SQLException  if a database-access error occurs.
     */
    public void setShort( int index, short value ) throws SQLException
    {
        setParam( index, new Integer( value ), java.sql.Types.SMALLINT, -1 );
    }


    /**
     *  Set a parameter to a Java String value. The driver converts this to a
     *  SQL VARCHAR or LONGVARCHAR value (depending on the arguments size
     *  relative to the driver's limits on VARCHARs) when it sends it to the
     *  database.
     *
     *@param  parameterIndex    the first parameter is 1, the second is 2, ...
     *@param  x                 the parameter value
     *@exception  SQLException  if a database-access error occurs.
     */
    public void setString( int index, String str ) throws SQLException
    {
        setParam(index, str, java.sql.Types.VARCHAR, str==null ? -1 : str.length());
    }


    /**
     *  Set a parameter to a java.sql.Time value. The driver converts this to a
     *  SQL TIME value when it sends it to the database.
     *
     *@param  parameterIndex    the first parameter is 1, the second is 2, ...
     *@param  x                 the parameter value
     *@exception  SQLException  if a database-access error occurs.
     */
    public void setTime( int parameterIndex, java.sql.Time value )
        throws SQLException
    {
        setParam( parameterIndex, value, java.sql.Types.TIME, -1 );
    }


    /**
     *  Set a parameter to a java.sql.Timestamp value. The driver converts this
     *  to a SQL TIMESTAMP value when it sends it to the database.
     *
     *@param  parameterIndex    the first parameter is 1, the second is 2, ...
     *@param  x                 the parameter value
     *@exception  SQLException  if a database-access error occurs.
     */
    public void setTimestamp( int index, java.sql.Timestamp value )
             throws SQLException
    {
        setParam( index, value, java.sql.Types.TIMESTAMP, -1 );
    }


    /**
     *  When a very large UNICODE value is input to a LONGVARCHAR parameter, it
     *  may be more practical to send it via a java.io.InputStream. JDBC will
     *  read the data from the stream as needed, until it reaches end-of-file.
     *  The JDBC driver will do any necessary conversion from UNICODE to the
     *  database char format. <P>
     *
     *  <B>Note:</B> This stream object can either be a standard Java stream
     *  object or your own subclass that implements the standard interface.
     *
     *@param  parameterIndex    the first parameter is 1, the second is 2, ...
     *@param  x                 the java input stream which contains the UNICODE
     *      parameter value
     *@param  length            the number of bytes in the stream
     *@exception  SQLException  if a database-access error occurs.
     */
    public void setUnicodeStream( int parameterIndex, java.io.InputStream x, int length )
             throws SQLException
    {
        throw new SQLException( "Not implemented" );
    }


    //--------------------------JDBC 2.0-----------------------------

    /**
     *  JDBC 2.0 Adds a set of parameters to the batch.
     *
     *@exception  SQLException  if a database access error occurs
     *@see                      Statement#addBatch
     */
    public void addBatch() throws java.sql.SQLException
    {
        NotImplemented();
    }


    /**
     *  JDBC 2.0 Sets the designated parameter to the given <code>Reader</code>
     *  object, which is the given number of characters long. When a very large
     *  UNICODE value is input to a LONGVARCHAR parameter, it may be more
     *  practical to send it via a java.io.Reader. JDBC will read the data from
     *  the stream as needed, until it reaches end-of-file. The JDBC driver will
     *  do any necessary conversion from UNICODE to the database char format.
     *  <P>
     *
     *  <B>Note:</B> This stream object can either be a standard Java stream
     *  object or your own subclass that implements the standard interface.
     *
     *@param  parameterIndex    the first parameter is 1, the second is 2, ...
     *@param  x                 the java reader which contains the UNICODE data
     *@param  length            the number of characters in the stream
     *@exception  SQLException  if a database access error occurs
     */
    public void setCharacterStream( int parameterIndex,
            java.io.Reader reader,
            int length ) throws java.sql.SQLException
    {
        NotImplemented();
    }


    /**
     *  JDBC 2.0 Sets a REF(&lt;structured-type&gt;) parameter.
     *
     *@param  i                 the first parameter is 1, the second is 2, ...
     *@param  x                 an object representing data of an SQL REF Type
     *@exception  SQLException  if a database access error occurs
     */
    public void setRef( int i, java.sql.Ref x ) throws java.sql.SQLException
    {
        NotImplemented();
    }


    /**
     *  JDBC 2.0 Sets a BLOB parameter.
     *
     *@param  i                 the first parameter is 1, the second is 2, ...
     *@param  x                 an object representing a BLOB
     *@exception  SQLException  if a database access error occurs
     */
    public void setBlob( int i, java.sql.Blob x ) throws java.sql.SQLException
    {
        NotImplemented();
    }


    /**
     *  JDBC 2.0 Sets a CLOB parameter.
     *
     *@param  i                 the first parameter is 1, the second is 2, ...
     *@param  x                 an object representing a CLOB
     *@exception  SQLException  if a database access error occurs
     */
    public void setClob( int i, java.sql.Clob x ) throws java.sql.SQLException
    {
        NotImplemented();
    }


    /**
     *  JDBC 2.0 Sets an Array parameter.
     *
     *@param  i                 the first parameter is 1, the second is 2, ...
     *@param  x                 an object representing an SQL array
     *@exception  SQLException  if a database access error occurs
     */
    public void setArray( int i, java.sql.Array x ) throws java.sql.SQLException
    {
        NotImplemented();
    }


    /**
     *  JDBC 2.0 Gets the number, types and properties of a ResultSet's columns.
     *
     *@return                   the description of a ResultSet's columns
     *@exception  SQLException  if a database access error occurs
     */
    public java.sql.ResultSetMetaData getMetaData() throws java.sql.SQLException
    {
        NotImplemented();
        return null;
    }


    /**
     *  JDBC 2.0 Sets the designated parameter to a java.sql.Date value, using
     *  the given <code>Calendar</code> object. The driver uses the <code>Calendar</code>
     *  object to construct an SQL DATE, which the driver then sends to the
     *  database. With a a <code>Calendar</code> object, the driver can
     *  calculate the date taking into account a custom timezone and locale. If
     *  no <code>Calendar</code> object is specified, the driver uses the
     *  default timezone and locale.
     *
     *@param  parameterIndex    the first parameter is 1, the second is 2, ...
     *@param  x                 the parameter value
     *@param  cal               the <code>Calendar</code> object the driver will
     *      use to construct the date
     *@exception  SQLException  if a database access error occurs
     */
    public void setDate( int parameterIndex, java.sql.Date x, java.util.Calendar cal )
             throws java.sql.SQLException
    {
        NotImplemented();
    }


    /**
     *  JDBC 2.0 Sets the designated parameter to a java.sql.Time value, using
     *  the given <code>Calendar</code> object. The driver uses the <code>Calendar</code>
     *  object to construct an SQL TIME, which the driver then sends to the
     *  database. With a a <code>Calendar</code> object, the driver can
     *  calculate the time taking into account a custom timezone and locale. If
     *  no <code>Calendar</code> object is specified, the driver uses the
     *  default timezone and locale.
     *
     *@param  parameterIndex    the first parameter is 1, the second is 2, ...
     *@param  x                 the parameter value
     *@param  cal               the <code>Calendar</code> object the driver will
     *      use to construct the time
     *@exception  SQLException  if a database access error occurs
     */
    public void setTime( int parameterIndex, java.sql.Time x, java.util.Calendar cal )
             throws java.sql.SQLException
    {
        NotImplemented();
    }


    /**
     *  JDBC 2.0 Sets the designated parameter to a java.sql.Timestamp value,
     *  using the given <code>Calendar</code> object. The driver uses the <code>Calendar</code>
     *  object to construct an SQL TIMESTAMP, which the driver then sends to the
     *  database. With a a <code>Calendar</code> object, the driver can
     *  calculate the timestamp taking into account a custom timezone and
     *  locale. If no <code>Calendar</code> object is specified, the driver uses
     *  the default timezone and locale.
     *
     *@param  parameterIndex    the first parameter is 1, the second is 2, ...
     *@param  x                 the parameter value
     *@param  cal               the <code>Calendar</code> object the driver will
     *      use to construct the timestamp
     *@exception  SQLException  if a database access error occurs
     */
    public void setTimestamp( int parameterIndex,
            java.sql.Timestamp x,
            java.util.Calendar cal )
             throws java.sql.SQLException
    {
        NotImplemented();
    }


    /**
     *  JDBC 2.0 Sets the designated parameter to SQL NULL. This version of
     *  setNull should be used for user-named types and REF type parameters.
     *  Examples of user-named types include: STRUCT, DISTINCT, JAVA_OBJECT, and
     *  named array types. <P>
     *
     *  <B>Note:</B> To be portable, applications must give the SQL type code
     *  and the fully-qualified SQL type name when specifying a NULL
     *  user-defined or REF parameter. In the case of a user-named type the name
     *  is the type name of the parameter itself. For a REF parameter the name
     *  is the type name of the referenced type. If a JDBC driver does not need
     *  the type code or type name information, it may ignore it. Although it is
     *  intended for user-named and Ref parameters, this method may be used to
     *  set a null parameter of any JDBC type. If the parameter does not have a
     *  user-named or REF type, the given typeName is ignored.
     *
     *@param  parameterIndex    the first parameter is 1, the second is 2, ...
     *@param  sqlType           a value from java.sql.Types
     *@param  typeName          the fully-qualified name of an SQL user-named
     *      type, ignored if the parameter is not a user-named type or REF
     *@exception  SQLException  if a database access error occurs
     */
    public void setNull( int paramIndex, int sqlType, String typeName )
             throws java.sql.SQLException
    {
        NotImplemented();
    }

    public java.sql.ParameterMetaData getParameterMetaData() throws java.sql.SQLException
    {
        NotImplemented();
        return null;
    }

    public void setURL(int param, java.net.URL url) throws java.sql.SQLException
    {
        NotImplemented();
    }

}
