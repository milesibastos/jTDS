//
// Copyright 1998, 1999 CDS Networks, Inc., Medford Oregon
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
import java.util.Calendar;

/**
 *  <P>
 *
 *  CallableStatement is used to execute SQL stored procedures. <P>
 *
 *  JDBC provides a stored procedure SQL escape that allows stored procedures to
 *  be called in a standard way for all RDBMS's. This escape syntax has one form
 *  that includes a result parameter and one that does not. If used, the result
 *  parameter must be registered as an OUT parameter. The other parameters may
 *  be used for input, output or both. Parameters are refered to sequentially,
 *  by number. The first parameter is 1. <P>
 *
 *  <CODE>
 * {?= call <procedure-name>
 *
 *  [<arg1>,<arg2>, ...]}<BR>
 *  {call <procedure-name>
 *
 *  [<arg1>,<arg2>, ...]} </CODE> <P>
 *
 *  IN parameter values are set using the set methods inherited from
 *  PreparedStatement. The type of all OUT parameters must be registered prior
 *  to executing the stored procedure; their values are retrieved after
 *  execution via the get methods provided here. <P>
 *
 *  A Callable statement may return a ResultSet or multiple ResultSets. Multiple
 *  ResultSets are handled using operations inherited from Statement. <P>
 *
 *  For maximum portability, a call's ResultSets and update counts should be
 *  processed prior to getting the values of output parameters.
 *
 *@see    Connection#prepareCall
 *@see    ResultSet
 */
public class CallableStatement_base
         extends com.internetcds.jdbc.tds.PreparedStatement_base
         implements java.sql.CallableStatement {
    public final static String cvsVersion = "$Id: CallableStatement_base.java,v 1.13 2002-09-18 16:27:01 alin_sinpalean Exp $";

    private String procedureName = null;
    private boolean lastWasNull = false;
    private int lastOutParam = -1;


    public CallableStatement_base( TdsConnection conn_, String sql )
             throws SQLException
    {
        this( conn_, sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY );
    }


    public CallableStatement_base(
            TdsConnection conn_, String sql, int type, int concurrency )
             throws SQLException
    {
        super( conn_, sql, type, concurrency );
        int i;

        procedureName = "";
        i = 0;
        int pos = sql.indexOf( "{call " );
        if ( pos >= 0 ) {
            i = pos + 6;
        }
        while ( i < sql.length()
                 && ( !
                ( Character.isLetterOrDigit( sql.charAt( i ) )
                 || sql.charAt( i ) == '#' ) ) ) {
            i++;
        }

        while ( i < sql.length()
                 && ( Character.isLetterOrDigit( sql.charAt( i ) )
                 || sql.charAt( i ) == '#'
                 || sql.charAt( i ) == '_' ) ) {
            procedureName = procedureName + sql.charAt( i );
            i++;
        }

        if ( procedureName.length() == 0 ) {
            throw new SQLException( "Did not find name in sql string" );
        }
    }


    /**
     *  read one element from the parameter list
     *
     *@param  index  (in-only) index (first column is 1) of the parameter
     *@param  value  (in-only)
     *@param  type   (in-only) JDBC type
     */
    private Object getParam(
            int index )
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
        lastWasNull = ( parameterList[index] == null );
        return parameterList[index].value;
    }


    /**
     *  Get the value of a NUMERIC parameter as a java.math.BigDecimal object.
     *
     *@param  parameterIndex    the first parameter is 1, the second is 2, ...
     *@param  scale             a value greater than or equal to zero
     *      representing the desired number of digits to the right of the
     *      decimal point
     *@return                   the parameter value; if the value is SQL NULL,
     *      the result is null
     *@exception  SQLException  if a database-access error occurs.
     */
    public BigDecimal getBigDecimal( int parameterIndex, int scale )
             throws SQLException
    {
        throw new SQLException( "Not implemented" );
    }


    /**
     *  Get the value of a BIT parameter as a Java boolean.
     *
     *@param  parameterIndex    the first parameter is 1, the second is 2, ...
     *@return                   the parameter value; if the value is SQL NULL,
     *      the result is false
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean getBoolean( int parameterIndex ) throws SQLException
    {
        Object value = getParam( parameterIndex );
        if ( value == null ) {
            return false;
        }
        try {
            return ( ( Boolean ) value ).booleanValue();
        }
        catch ( Exception e ) {
            throw new SQLException( "Unable to convert parameter" );
        }
    }


    /**
     *  Get the value of a TINYINT parameter as a Java byte.
     *
     *@param  parameterIndex    the first parameter is 1, the second is 2, ...
     *@return                   the parameter value; if the value is SQL NULL,
     *      the result is 0
     *@exception  SQLException  if a database-access error occurs.
     */
    public byte getByte( int parameterIndex ) throws SQLException
    {
        Object value = getParam( parameterIndex );
        if ( value == null ) {
            return 0;
        }
        try {
            return ( ( Number ) value ).byteValue();
        }
        catch ( Exception e ) {
            throw new SQLException( "Unable to convert parameter" );
        }
    }


    /**
     *  Get the value of a SQL BINARY or VARBINARY parameter as a Java byte[]
     *
     *@param  parameterIndex    the first parameter is 1, the second is 2, ...
     *@return                   the parameter value; if the value is SQL NULL,
     *      the result is null
     *@exception  SQLException  if a database-access error occurs.
     */
    public byte[] getBytes( int parameterIndex ) throws SQLException
    {
        Object value = getParam( parameterIndex );
        if ( value == null ) {
            return null;
        }
        try {
            return ( byte[] ) value;
        }
        catch ( Exception e ) {
            throw new SQLException( "Unable to convert parameter" );
        }
    }


    /**
     *  Get the value of a SQL DATE parameter as a java.sql.Date object
     *
     *@param  parameterIndex    the first parameter is 1, the second is 2, ...
     *@return                   the parameter value; if the value is SQL NULL,
     *      the result is null
     *@exception  SQLException  if a database-access error occurs.
     */
    public java.sql.Date getDate( int parameterIndex ) throws SQLException
    {
        throw new SQLException( "Not implemented" );
    }


    /**
     *  Get the value of a DOUBLE parameter as a Java double.
     *
     *@param  parameterIndex    the first parameter is 1, the second is 2, ...
     *@return                   the parameter value; if the value is SQL NULL,
     *      the result is 0
     *@exception  SQLException  if a database-access error occurs.
     */
    public double getDouble( int parameterIndex ) throws SQLException
    {
        Object value = getParam( parameterIndex );
        if ( value == null ) {
            return 0;
        }
        try {
            return ( ( Number ) value ).doubleValue();
        }
        catch ( Exception e ) {
            throw new SQLException( "Unable to convert parameter" );
        }
    }


    /**
     *  Get the value of a FLOAT parameter as a Java float.
     *
     *@param  parameterIndex    the first parameter is 1, the second is 2, ...
     *@return                   the parameter value; if the value is SQL NULL,
     *      the result is 0
     *@exception  SQLException  if a database-access error occurs.
     */
    public float getFloat( int parameterIndex ) throws SQLException
    {
        Object value = getParam( parameterIndex );
        if ( value == null ) {
            return 0;
        }
        try {
            return ( ( Number ) value ).floatValue();
        }
        catch ( Exception e ) {
            throw new SQLException( "Unable to convert parameter" );
        }
    }


    /**
     *  Get the value of an INTEGER parameter as a Java int.
     *
     *@param  parameterIndex    the first parameter is 1, the second is 2, ...
     *@return                   the parameter value; if the value is SQL NULL,
     *      the result is 0
     *@exception  SQLException  if a database-access error occurs.
     */
    public int getInt( int parameterIndex ) throws SQLException
    {
        Object value = getParam( parameterIndex );
        if ( value == null ) {
            return 0;
        }
        try {
            return ( ( Number ) value ).intValue();
        }
        catch ( Exception e ) {
            throw new SQLException( "Unable to convert parameter" );
        }
    }


    /**
     *  Get the value of a BIGINT parameter as a Java long.
     *
     *@param  parameterIndex    the first parameter is 1, the second is 2, ...
     *@return                   the parameter value; if the value is SQL NULL,
     *      the result is 0
     *@exception  SQLException  if a database-access error occurs.
     */
    public long getLong( int parameterIndex ) throws SQLException
    {
        Object value = getParam( parameterIndex );
        if ( value == null ) {
            return 0;
        }
        try {
            return ( ( Number ) value ).longValue();
        }
        catch ( Exception e ) {
            throw new SQLException( "Unable to convert parameter" );
        }
    }


    //----------------------------------------------------------------------
    // Advanced features:


    /**
     *  Get the value of a parameter as a Java object. <p>
     *
     *  This method returns a Java object whose type coresponds to the SQL type
     *  that was registered for this parameter using registerOutParameter. <p>
     *
     *  Note that this method may be used to read datatabase-specific, abstract
     *  data types. This is done by specifying a targetSqlType of
     *  java.sql.types.OTHER, which allows the driver to return a
     *  database-specific Java type.
     *
     *@param  parameterIndex    The first parameter is 1, the second is 2, ...
     *@return                   A java.lang.Object holding the OUT parameter
     *      value.
     *@exception  SQLException  if a database-access error occurs.
     */
    public Object getObject( int parameterIndex ) throws SQLException
    {
        return getParam( parameterIndex );
    }


    /**
     *  Get the value of a SMALLINT parameter as a Java short.
     *
     *@param  parameterIndex    the first parameter is 1, the second is 2, ...
     *@return                   the parameter value; if the value is SQL NULL,
     *      the result is 0
     *@exception  SQLException  if a database-access error occurs.
     */
    public short getShort( int parameterIndex ) throws SQLException
    {
        Object value = getParam( parameterIndex );
        if ( value == null ) {
            return 0;
        }
        try {
            return ( ( Number ) value ).shortValue();
        }
        catch ( Exception e ) {
            throw new SQLException( "Unable to convert parameter" );
        }
    }


    /**
     *  Get the value of a CHAR, VARCHAR, or LONGVARCHAR parameter as a Java
     *  String.
     *
     *@param  parameterIndex    the first parameter is 1, the second is 2, ...
     *@return                   the parameter value; if the value is SQL NULL,
     *      the result is null
     *@exception  SQLException  if a database-access error occurs.
     */
    public String getString( int parameterIndex ) throws SQLException
    {
        Object value = getParam( parameterIndex );
        try {
            return ( String ) value;
        }
        catch ( Exception e ) {
            throw new SQLException( "Unable to convert parameter" );
        }
    }


    /**
     *  Get the value of a SQL TIME parameter as a java.sql.Time object.
     *
     *@param  parameterIndex    the first parameter is 1, the second is 2, ...
     *@return                   the parameter value; if the value is SQL NULL,
     *      the result is null
     *@exception  SQLException  if a database-access error occurs.
     */
    public java.sql.Time getTime( int parameterIndex ) throws SQLException
    {
        throw new SQLException( "Not implemented" );
    }


    /**
     *  Get the value of a SQL TIMESTAMP parameter as a java.sql.Timestamp
     *  object.
     *
     *@param  parameterIndex    the first parameter is 1, the second is 2, ...
     *@return                   the parameter value; if the value is SQL NULL,
     *      the result is null
     *@exception  SQLException  if a database-access error occurs.
     */
    public java.sql.Timestamp getTimestamp( int parameterIndex )
             throws SQLException
    {
        throw new SQLException( "Not implemented" );
    }


    /**
     *  Before executing a stored procedure call, you must explicitly call
     *  registerOutParameter to register the java.sql.Type of each out
     *  parameter. <P>
     *
     *  <B>Note:</B> When reading the value of an out parameter, you must use
     *  the getXXX method whose Java type XXX corresponds to the parameter's
     *  registered SQL type.
     *
     *@param  parameterIndex    the first parameter is 1, the second is 2,...
     *@param  sqlType           SQL type code defined by java.sql.Types; for
     *      parameters of type Numeric or Decimal use the version of
     *      registerOutParameter that accepts a scale value
     *@exception  SQLException  if a database-access error occurs.
     */
    public void registerOutParameter( int parameterIndex, int sqlType )
             throws SQLException
    {
        registerOutParameter( parameterIndex, sqlType, -1 );
    }


    /**
     *  Use this version of registerOutParameter for registering Numeric or
     *  Decimal out parameters. <P>
     *
     *  <B>Note:</B> When reading the value of an out parameter, you must use
     *  the getXXX method whose Java type XXX corresponds to the parameter's
     *  registered SQL type.
     *
     *@param  parameterIndex    the first parameter is 1, the second is 2, ...
     *@param  sqlType           use either java.sql.Type.NUMERIC or
     *      java.sql.Type.DECIMAL
     *@param  scale             a value greater than or equal to zero
     *      representing the desired number of digits to the right of the
     *      decimal point
     *@exception  SQLException  if a database-access error occurs.
     */
    public void registerOutParameter( int parameterIndex, int sqlType, int scale )
             throws SQLException
    {
        setParam( parameterIndex, null, sqlType, scale );
        parameterList[parameterIndex - 1].isOutput = true;
    }


    /**
     *  An OUT parameter may have the value of SQL NULL; wasNull reports whether
     *  the last value read has this special value. <P>
     *
     *  <B>Note:</B> You must first call getXXX on a parameter to read its value
     *  and then call wasNull() to see if the value was SQL NULL.
     *
     *@return                   true if the last parameter read was SQL NULL
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean wasNull() throws SQLException
    {
        return lastWasNull;
    }


    public boolean execute()
             throws SQLException
    {
        boolean result;

        closeResults(false);
        // SAfe No need for this. getMoreResults sets it to -1 at start, anyway
//        updateCount = -2;
        lastOutParam = -1;

        // First make sure the caller has filled in all the parameters.
        ParameterUtils.verifyThatParametersAreSet( parameterList );

        // execute the stored procedure
        result = executeCall( procedureName,
                parameterList,
                parameterList );

        return result;
    }



    //--------------------------JDBC 2.0-----------------------------

    /**
     *  JDBC 2.0 Gets the value of a JDBC <code>NUMERIC</code> parameter as a
     *  <code>java.math.BigDecimal</code> object with as many digits to the
     *  right of the decimal point as the value contains.
     *
     *@param  parameterIndex    the first parameter is 1, the second is 2, and
     *      so on
     *@return                   the parameter value in full precision. If the
     *      value is SQL NULL, the result is <code>null</code>.
     *@exception  SQLException  if a database access error occurs
     */
    public BigDecimal getBigDecimal( int parameterIndex ) throws SQLException
    {
        NotImplemented();
        return null;
    }



    /**
     *  Gets the value of a JDBC <code>DATE</code> parameter as a <code>java.sql.Date</code>
     *  object, using the given <code>Calendar</code> object to construct the
     *  date. With a <code>Calendar</code> object, the driver can calculate the
     *  date taking into account a custom timezone and locale. If no <code>Calendar</code>
     *  object is specified, the driver uses the default timezone and locale.
     *
     *@param  parameterIndex    the first parameter is 1, the second is 2, and
     *      so on
     *@param  cal               the <code>Calendar</code> object the driver will
     *      use to construct the date
     *@return                   the parameter value. If the value is SQL NULL,
     *      the result is <code>null</code>.
     *@exception  SQLException  if a database access error occurs
     */
    public java.sql.Date getDate( int parameterIndex, Calendar cal )
             throws SQLException
    {
        NotImplemented();
        return null;
    }


    /**
     *  Gets the value of a JDBC <code>TIME</code> parameter as a <code>java.sql.Time</code>
     *  object, using the given <code>Calendar</code> object to construct the
     *  time. With a <code>Calendar</code> object, the driver can calculate the
     *  time taking into account a custom timezone and locale. If no <code>Calendar</code>
     *  object is specified, the driver uses the default timezone and locale.
     *
     *@param  parameterIndex    the first parameter is 1, the second is 2, and
     *      so on
     *@param  cal               the <code>Calendar</code> object the driver will
     *      use to construct the time
     *@return                   the parameter value; if the value is SQL NULL,
     *      the result is <code>null</code>.
     *@exception  SQLException  if a database access error occurs
     */
    public java.sql.Time getTime( int parameterIndex, Calendar cal )
             throws SQLException
    {
        NotImplemented();
        return null;
    }


    /**
     *  Gets the value of a JDBC <code>TIMESTAMP</code> parameter as a <code>java.sql.Timestamp</code>
     *  object, using the given <code>Calendar</code> object to construct the
     *  <code>Timestamp</code> object. With a <code>Calendar</code> object, the
     *  driver can calculate the timestamp taking into account a custom timezone
     *  and locale. If no <code>Calendar</code> object is specified, the driver
     *  uses the default timezone and locale.
     *
     *@param  parameterIndex    the first parameter is 1, the second is 2, and
     *      so on
     *@param  cal               the <code>Calendar</code> object the driver will
     *      use to construct the timestamp
     *@return                   the parameter value. If the value is SQL NULL,
     *      the result is <code>null</code>.
     *@exception  SQLException  if a database access error occurs
     */
    public java.sql.Timestamp getTimestamp( int parameterIndex, Calendar cal )
             throws SQLException
    {
        NotImplemented();
        return null;
    }


    protected void addOutputParam( Object value ) throws SQLException
    {
        for ( lastOutParam++; lastOutParam < parameterList.length; lastOutParam++ ) {
            if ( parameterList[lastOutParam].isOutput ) {
                parameterList[lastOutParam].value = value;
                return;
            }
        }
        throw new SQLException( "protocol returns too many output params" );
        // ERROR: More parameters received than expected
    }


    /**
     *  JDBC 2.0 Registers the designated output parameter. This version of the
     *  method <code>registerOutParameter</code> should be used for a user-named
     *  or REF output parameter. Examples of user-named types include: STRUCT,
     *  DISTINCT, JAVA_OBJECT, and named array types. Before executing a stored
     *  procedure call, you must explicitly call <code>registerOutParameter</code>
     *  to register the type from <code>java.sql.Types</code> for each OUT
     *  parameter. For a user-named parameter the fully-qualified SQL type name
     *  of the parameter should also be given, while a REF parameter requires
     *  that the fully-qualified type name of the referenced type be given. A
     *  JDBC driver that does not need the type code and type name information
     *  may ignore it. To be portable, however, applications should always
     *  provide these values for user-named and REF parameters. Although it is
     *  intended for user-named and REF parameters, this method may be used to
     *  register a parameter of any JDBC type. If the parameter does not have a
     *  user-named or REF type, the typeName parameter is ignored. <P>
     *
     *  <B>Note:</B> When reading the value of an out parameter, you must use
     *  the <code>getXXX</code> method whose Java type XXX corresponds to the
     *  parameter's registered SQL type.
     *
     *@param  parameterIndex    the first parameter is 1, the second is 2,...
     *@param  sqlType           a value from <code>java.sql.Types</code>
     *@param  typeName          the fully-qualified name of an SQL structured
     *      type
     *@exception  SQLException  if a database-access error occurs
     */
    public void registerOutParameter( int paramIndex, int sqlType, String typeName )
             throws SQLException
    {
        NotImplemented();
    }


    /**
     *  JDBC 2.0 Gets the value of a JDBC <code>BLOB</code> parameter as a
     *  {@link Blob} object in the Java programming language.
     *
     *@param  i                 the first parameter is 1, the second is 2, and
     *      so on
     *@return                   the parameter value as a <code>Blob</code>
     *      object in the Java programming language. If the value was SQL NULL,
     *      the value <code>null</code> is returned.
     *@exception  SQLException  if a database access error occurs
     */
    public Blob getBlob( int i ) throws SQLException
    {
        NotImplemented();
        return null;
    }


    /**
     *  JDBC 2.0 Gets the value of a JDBC <code>CLOB</code> parameter as a
     *  <code>Clob</code> object in the Java programming language.
     *
     *@param  i                 the first parameter is 1, the second is 2, and
     *      so on
     *@return                   the parameter value as a <code>Clob</code>
     *      object in the Java programming language. If the value was SQL NULL,
     *      the value <code>null</code> is returned.
     *@exception  SQLException  if a database access error occurs
     */
    public Clob getClob( int i ) throws SQLException
    {
        NotImplemented();
        return null;
    }


    /**
     *  JDBC 2.0 Returns an object representing the value of OUT parameter
     *  <code>i</code> and uses <code>map</code> for the custom mapping of the
     *  parameter value. <p>
     *
     *  This method returns a Java object whose type corresponds to the JDBC
     *  type that was registered for this parameter using the method <code>registerOutParameter</code>
     *  . By registering the target JDBC type as <code>java.sql.Types.OTHER</code>
     *  , this method can be used to read database-specific abstract data types.
     *
     *@param  i                 the first parameter is 1, the second is 2, and
     *      so on
     *@param  map               the mapping from SQL type names to Java classes
     *@return                   a java.lang.Object holding the OUT parameter
     *      value.
     *@exception  SQLException  if a database access error occurs
     */
    public Object getObject( int i, java.util.Map map ) throws SQLException
    {
        NotImplemented();
        return null;
    }


    /**
     *  JDBC 2.0 Gets the value of a JDBC <code>REF(&lt;structured-type&gt;)</code>
     *  parameter as a {@link Ref} object in the Java programming language.
     *
     *@param  i                 the first parameter is 1, the second is 2, and
     *      so on
     *@return                   the parameter value as a <code>Ref</code> object
     *      in the Java programming language. If the value was SQL NULL, the
     *      value <code>null</code> is returned.
     *@exception  SQLException  if a database access error occurs
     */
    public Ref getRef( int i ) throws SQLException
    {
        NotImplemented();
        return null;
    }


    /**
     *  JDBC 2.0 Gets the value of a JDBC <code>ARRAY</code> parameter as an
     *  {@link Array} object in the Java programming language.
     *
     *@param  i                 the first parameter is 1, the second is 2, and
     *      so on
     *@return                   the parameter value as an <code>Array</code>
     *      object in the Java programming language. If the value was SQL NULL,
     *      the value <code>null</code> is returned.
     *@exception  SQLException  if a database access error occurs
     */
    public Array getArray( int i ) throws SQLException
    {
        NotImplemented();
        return null;
    }


    public void setClob( int i, java.sql.Clob x ) throws java.sql.SQLException
    {
        //  XXX should be inherited PreparedStatement_2_0
        NotImplemented();
    }


    public void addBatch() throws java.sql.SQLException
    {
        //  XXX should be inherited PreparedStatement_2_0
        NotImplemented();
    }


    public void setBlob( int i, java.sql.Blob x ) throws java.sql.SQLException
    {
        //  XXX should be inherited PreparedStatement_2_0
        NotImplemented();
    }


    public void setArray( int i, java.sql.Array x ) throws java.sql.SQLException
    {
        //  XXX should be inherited PreparedStatement_2_0
        NotImplemented();
    }


    public void setRef( int i, java.sql.Ref x ) throws java.sql.SQLException
    {
        //  XXX should be inherited PreparedStatement_2_0
        NotImplemented();
    }


    public void setCharacterStream( int parameterIndex,
            java.io.Reader reader,
            int length ) throws java.sql.SQLException
    {
        //  XXX should be inherited PreparedStatement_2_0
        NotImplemented();
    }


    public java.sql.ResultSetMetaData getMetaData() throws java.sql.SQLException
    {
        //  XXX should be inherited PreparedStatement_2_0
        NotImplemented();
        return null;
    }


    public void setNull( int paramIndex, int sqlType, String typeName )
             throws java.sql.SQLException
    {
        //  XXX should be inherited PreparedStatement_2_0
        NotImplemented();
    }


    public void setTimestamp( int parameterIndex, java.sql.Timestamp x, java.util.Calendar cal )
             throws java.sql.SQLException
    {
        //  XXX should be inherited PreparedStatement_2_0
        NotImplemented();
    }


    public void setDate( int parameterIndex, java.sql.Date x, java.util.Calendar cal )
             throws java.sql.SQLException
    {
        //  XXX should be inherited PreparedStatement_2_0
        NotImplemented();
    }


    public void setTime( int parameterIndex, java.sql.Time x, java.util.Calendar cal )
             throws java.sql.SQLException
    {
        NotImplemented();
    }


    // called by TdsStatement.moreResults
    public void handleRetStat( PacketRetStatResult packet ) { }


    public void handleParamResult( PacketOutputParamResult packet ) throws SQLException
    {
        addOutputParam( packet.value );
    }

    public void registerOutParameter(String str, int param, String str2) throws java.sql.SQLException
    {
        NotImplemented();
    }

    public void registerOutParameter(String str, int param, int param2) throws java.sql.SQLException
    {
        NotImplemented();
    }

    public void registerOutParameter(String str, int param) throws java.sql.SQLException
    {
        NotImplemented();
    }

    public java.sql.Array getArray(String str) throws java.sql.SQLException
    {
        NotImplemented();
        return null;
    }

    public java.math.BigDecimal getBigDecimal(String str) throws java.sql.SQLException
    {
        NotImplemented();
        return null;
    }

    public java.sql.Blob getBlob(String str) throws java.sql.SQLException
    {
        NotImplemented();
        return null;
    }

    public boolean getBoolean(String str) throws java.sql.SQLException
    {
        NotImplemented();
        return false;
    }

    public byte getByte(String str) throws java.sql.SQLException
    {
        NotImplemented();
        return 0;
    }

    public byte[] getBytes(String str) throws java.sql.SQLException
    {
        NotImplemented();
        return null;
    }

    public java.sql.Clob getClob(String str) throws java.sql.SQLException
    {
        NotImplemented();
        return null;
    }

    public java.sql.Date getDate(String str) throws java.sql.SQLException
    {
        NotImplemented();
        return null;
    }

    public java.sql.Date getDate(String str, java.util.Calendar calendar) throws java.sql.SQLException
    {
        NotImplemented();
        return null;
    }

    public double getDouble(String str) throws java.sql.SQLException
    {
        NotImplemented();
        return 0;
    }

    public float getFloat(String str) throws java.sql.SQLException
    {
        NotImplemented();
        return 0;
    }

    public int getInt(String str) throws java.sql.SQLException
    {
        NotImplemented();
        return 0;
    }

    public long getLong(String str) throws java.sql.SQLException
    {
        NotImplemented();
        return 0;
    }

    public Object getObject(String str) throws java.sql.SQLException
    {
        NotImplemented();
        return null;
    }

    public Object getObject(String str, java.util.Map map) throws java.sql.SQLException
    {
        NotImplemented();
        return null;
    }

    public java.sql.Ref getRef(String str) throws java.sql.SQLException
    {
        NotImplemented();
        return null;
    }

    public short getShort(String str) throws java.sql.SQLException
    {
        NotImplemented();
        return 0;
    }

    public String getString(String str) throws java.sql.SQLException
    {
        NotImplemented();
        return null;
    }

    public java.sql.Time getTime(String str) throws java.sql.SQLException
    {
        NotImplemented();
        return null;
    }

    public java.sql.Time getTime(String str, java.util.Calendar calendar) throws java.sql.SQLException
    {
        NotImplemented();
        return null;
    }

    public java.sql.Timestamp getTimestamp(String str) throws java.sql.SQLException
    {
        NotImplemented();
        return null;
    }

    public java.sql.Timestamp getTimestamp(String str, java.util.Calendar calendar) throws java.sql.SQLException
    {
        NotImplemented();
        return null;
    }

    public java.net.URL getURL(int param) throws java.sql.SQLException
    {
        NotImplemented();
        return null;
    }

    public java.net.URL getURL(String str) throws java.sql.SQLException
    {
        NotImplemented();
        return null;
    }

    public void setAsciiStream(String str, java.io.InputStream inputStream, int param) throws java.sql.SQLException
    {
        NotImplemented();
    }

    public void setBigDecimal(String str, java.math.BigDecimal bigDecimal) throws java.sql.SQLException
    {
        NotImplemented();
    }

    public void setBinaryStream(String str, java.io.InputStream inputStream, int param) throws java.sql.SQLException
    {
        NotImplemented();
    }

    public void setBoolean(String str, boolean param) throws java.sql.SQLException
    {
        NotImplemented();
    }

    public void setByte(String str, byte param) throws java.sql.SQLException
    {
        NotImplemented();
    }

    public void setBytes(String str, byte[] values) throws java.sql.SQLException
    {
        NotImplemented();
    }

    public void setCharacterStream(String str, java.io.Reader reader, int param) throws java.sql.SQLException
    {
        NotImplemented();
    }

    public void setDate(String str, java.sql.Date date) throws java.sql.SQLException
    {
        NotImplemented();
    }

    public void setDate(String str, java.sql.Date date, java.util.Calendar calendar) throws java.sql.SQLException
    {
        NotImplemented();
    }

    public void setDouble(String str, double param) throws java.sql.SQLException
    {
        NotImplemented();
    }

    public void setFloat(String str, float param) throws java.sql.SQLException
    {
        NotImplemented();
    }

    public void setInt(String str, int param) throws java.sql.SQLException
    {
        NotImplemented();
    }

    public void setLong(String str, long param) throws java.sql.SQLException
    {
        NotImplemented();
    }

    public void setNull(String str, int param) throws java.sql.SQLException
    {
        NotImplemented();
    }

    public void setNull(String str, int param, String str2) throws java.sql.SQLException
    {
        NotImplemented();
    }

    public void setObject(String str, Object obj) throws java.sql.SQLException
    {
        NotImplemented();
    }

    public void setObject(String str, Object obj, int param) throws java.sql.SQLException
    {
        NotImplemented();
    }

    public void setObject(String str, Object obj, int param, int param3) throws java.sql.SQLException
    {
        NotImplemented();
    }

    public void setShort(String str, short param) throws java.sql.SQLException
    {
        NotImplemented();
    }

    public void setString(String str, String str1) throws java.sql.SQLException
    {
        NotImplemented();
    }

    public void setTime(String str, java.sql.Time time) throws java.sql.SQLException
    {
        NotImplemented();
    }

    public void setTime(String str, java.sql.Time time, java.util.Calendar calendar) throws java.sql.SQLException
    {
        NotImplemented();
    }

    public void setTimestamp(String str, java.sql.Timestamp timestamp) throws java.sql.SQLException
    {
        NotImplemented();
    }

    public void setTimestamp(String str, java.sql.Timestamp timestamp, java.util.Calendar calendar) throws java.sql.SQLException
    {
        NotImplemented();
    }

    public void setURL(String str, java.net.URL url) throws java.sql.SQLException
    {
        NotImplemented();
    }
}
