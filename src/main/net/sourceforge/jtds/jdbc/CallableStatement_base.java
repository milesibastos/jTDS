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

package net.sourceforge.jtds.jdbc;

import java.sql.*;
import java.math.BigDecimal;
import java.util.Calendar;

/**
 * CallableStatement is used to execute SQL stored procedures.
 * <p>
 * JDBC provides a stored procedure SQL escape that allows stored procedures to
 * be called in a standard way for all RDBMS's. This escape syntax has one form
 * that includes a result parameter and one that does not. If used, the result
 * parameter must be registered as an OUT parameter. The other parameters may
 * be used for input, output or both. Parameters are refered to sequentially,
 * by number. The first parameter is 1.
 * <p>
 * <code>
 * {?= call &lt;procedure-name&gt; [<arg1>,<arg2>, ...]}<br>
 * {call &lt;procedure-name&gt; [<arg1>,<arg2>, ...]}
 * </code>
 * <p>
 * IN parameter values are set using the set methods inherited from
 * PreparedStatement. The type of all OUT parameters must be registered prior
 * to executing the stored procedure; their values are retrieved after
 * execution via the get methods provided here.
 * <p>
 * A Callable statement may return a ResultSet or multiple ResultSets. Multiple
 * ResultSets are handled using operations inherited from Statement.
 * <p>
 * For maximum portability, a call's ResultSets and update counts should be
 * processed prior to getting the values of output parameters.
 *
 * @see  Connection#prepareCall
 * @see  ResultSet
 * @version  $Id: CallableStatement_base.java,v 1.3 2002-10-24 12:27:55 alin_sinpalean Exp $
 */
public class CallableStatement_base extends PreparedStatement_base
    implements java.sql.CallableStatement
{
    public final static String cvsVersion = "$Id: CallableStatement_base.java,v 1.3 2002-10-24 12:27:55 alin_sinpalean Exp $";

    private String procedureName = null;
    private boolean lastWasNull = false;
    private int lastOutParam = -1;

    public CallableStatement_base(TdsConnection conn_, String sql) throws SQLException
    {
        this(conn_, sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    public CallableStatement_base(TdsConnection conn_, String sql, int type, int concurrency)
        throws SQLException
    {
        super(conn_, sql, type, concurrency);
        procedureName = "";

        int i = 0;
        int pos = sql.indexOf("{call ");
        if( pos >= 0 )
            i = pos + 6;

        // Find the start of the procedure name
        while( i<sql.length() && !Character.isLetterOrDigit(sql.charAt(i)) && sql.charAt(i)!='#' )
            i++;

        pos = i;
        // Find the end of the procedure name
        while( i<sql.length() && (Character.isLetterOrDigit(sql.charAt(i)) || sql.charAt(i)=='#' ||
            sql.charAt(i)=='_' || sql.charAt(i)=='.') )
            i++;

        procedureName = sql.substring(pos, i);

        if( procedureName.length() == 0 )
            throw new SQLException( "Did not find name in sql string" );
    }

    private Object getParam(int index) throws SQLException
    {
        if( index < 1 )
            throw new SQLException("Invalid parameter index "
                     + index + ". JDBC indexes start at 1.");
        if( index > parameterList.length )
            throw new SQLException("Invalid parameter index "
                     + index + ". This statement only has "
                     + parameterList.length + " parameters.");

        // JDBC indexes start at 1, java array indexes start at 0 :-(
        index--;
        lastWasNull = ( parameterList[index] == null );
        return parameterList[index].value;
    }

    protected void addOutputParam(Object value) throws SQLException
    {
        for( lastOutParam++; lastOutParam<parameterList.length; lastOutParam++ )
            if( parameterList[lastOutParam].isOutput )
            {
                parameterList[lastOutParam].value = value;
                return;
            }

        throw new SQLException("More output params than expected.");
    }

    // called by TdsStatement.moreResults
    /** @todo Should implement this method and the {?=call...} escape sequence. */
    protected void handleRetStat(PacketRetStatResult packet)
    {
    }

    protected void handleParamResult(PacketOutputParamResult packet) throws SQLException
    {
        addOutputParam(packet.value);
    }

    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException
    {
        NotImplemented();
        return null;
    }

    public boolean getBoolean(int parameterIndex) throws SQLException
    {
        Object value = getParam(parameterIndex);
        if( value == null )
            return false;

        try
        {
            return ((Boolean)value).booleanValue();
        }
        catch( Exception e )
        {
            throw new SQLException("Unable to convert parameter");
        }
    }

    public byte getByte(int parameterIndex) throws SQLException
    {
        Object value = getParam(parameterIndex);
        if( value == null )
            return 0;

        try
        {
            return ((Number)value).byteValue();
        }
        catch( Exception e )
        {
            throw new SQLException("Unable to convert parameter");
        }
    }

    public byte[] getBytes(int parameterIndex) throws SQLException
    {
        Object value = getParam(parameterIndex);
        if( value == null )
            return null;

        try
        {
            return (byte[])value;
        }
        catch( Exception e )
        {
            throw new SQLException("Unable to convert parameter");
        }
    }

    public java.sql.Date getDate(int parameterIndex) throws SQLException
    {
        NotImplemented();
        return null;
    }

    public double getDouble(int parameterIndex) throws SQLException
    {
        Object value = getParam(parameterIndex);
        if( value == null )
            return 0;

        try
        {
            return ((Number)value).doubleValue();
        }
        catch( Exception e )
        {
            throw new SQLException("Unable to convert parameter");
        }
    }

    public float getFloat(int parameterIndex) throws SQLException
    {
        Object value = getParam(parameterIndex);
        if( value == null )
            return 0;

        try
        {
            return ((Number)value).floatValue();
        }
        catch( Exception e )
        {
            throw new SQLException("Unable to convert parameter");
        }
    }

    public int getInt(int parameterIndex) throws SQLException
    {
        Object value = getParam(parameterIndex);
        if( value == null )
            return 0;

        try
        {
            return ((Number)value).intValue();
        }
        catch( Exception e )
        {
            throw new SQLException("Unable to convert parameter");
        }
    }

    public long getLong(int parameterIndex) throws SQLException
    {
        Object value = getParam(parameterIndex);
        if( value == null )
            return 0;

        try
        {
            return ((Number)value).longValue();
        }
        catch( Exception e )
        {
            throw new SQLException("Unable to convert parameter");
        }
    }

    //----------------------------------------------------------------------
    // Advanced features:

    public Object getObject(int parameterIndex) throws SQLException
    {
        return getParam(parameterIndex);
    }

    public short getShort(int parameterIndex) throws SQLException
    {
        Object value = getParam(parameterIndex);
        if( value == null )
            return 0;

        try
        {
            return ((Number)value).shortValue();
        }
        catch( Exception e )
        {
            throw new SQLException("Unable to convert parameter");
        }
    }

    public String getString(int parameterIndex) throws SQLException
    {
        Object value = getParam(parameterIndex);
        try
        {
            return (String)value;
        }
        catch( Exception e )
        {
            throw new SQLException("Unable to convert parameter");
        }
    }

    public java.sql.Time getTime(int parameterIndex) throws SQLException
    {
        NotImplemented();
        return null;
    }

    public java.sql.Timestamp getTimestamp(int parameterIndex) throws SQLException
    {
        NotImplemented();
        return null;
    }

    public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException
    {
        registerOutParameter(parameterIndex, sqlType, -1);
    }

    public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException
    {
        setParam(parameterIndex, null, sqlType, scale);
        parameterList[parameterIndex-1].isOutput = true;
    }

    public boolean wasNull() throws SQLException
    {
        return lastWasNull;
    }

    public boolean execute() throws SQLException
    {
        closeResults(false);
        lastOutParam = -1;

        // First make sure the caller has filled in all the parameters.
        ParameterUtils.verifyThatParametersAreSet(parameterList);

        // Execute the stored procedure
        return internalExecuteCall(procedureName, parameterList, parameterList, getTds(false), warningChain);
    }

    //--------------------------JDBC 2.0-----------------------------

    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException
    {
        NotImplemented();
        return null;
    }

    public java.sql.Date getDate(int parameterIndex, Calendar cal) throws SQLException
    {
        NotImplemented();
        return null;
    }

    public java.sql.Time getTime(int parameterIndex, Calendar cal) throws SQLException
    {
        NotImplemented();
        return null;
    }

    public java.sql.Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException
    {
        NotImplemented();
        return null;
    }

    public void registerOutParameter(int paramIndex, int sqlType, String typeName)
        throws SQLException
    {
        NotImplemented();
    }

    public Blob getBlob(int i) throws SQLException
    {
        NotImplemented();
        return null;
    }

    public Clob getClob(int i) throws SQLException
    {
        NotImplemented();
        return null;
    }

    public Object getObject(int i, java.util.Map map) throws SQLException
    {
        NotImplemented();
        return null;
    }

    public Ref getRef(int i) throws SQLException
    {
        NotImplemented();
        return null;
    }

    public Array getArray(int i) throws SQLException
    {
        NotImplemented();
        return null;
    }

    public void setClob(int i, java.sql.Clob x) throws java.sql.SQLException
    {
        //  XXX should be inherited PreparedStatement_2_0
        NotImplemented();
    }

    public void addBatch() throws java.sql.SQLException
    {
        //  XXX should be inherited PreparedStatement_2_0
        NotImplemented();
    }

    public void setBlob(int i, java.sql.Blob x) throws java.sql.SQLException
    {
        //  XXX should be inherited PreparedStatement_2_0
        NotImplemented();
    }

    public void setArray(int i, java.sql.Array x) throws java.sql.SQLException
    {
        //  XXX should be inherited PreparedStatement_2_0
        NotImplemented();
    }

    public void setRef(int i, java.sql.Ref x) throws java.sql.SQLException
    {
        //  XXX should be inherited PreparedStatement_2_0
        NotImplemented();
    }

    public void setCharacterStream(int parameterIndex, java.io.Reader reader, int length)
        throws java.sql.SQLException
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

    public void setNull(int paramIndex, int sqlType, String typeName) throws java.sql.SQLException
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

    public void setDate(int parameterIndex, java.sql.Date x, java.util.Calendar cal)
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
