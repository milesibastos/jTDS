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
 * @version  $Id: CallableStatement_base.java,v 1.20 2004-03-27 17:24:58 bheineman Exp $
 */
public class CallableStatement_base extends PreparedStatement_base
implements java.sql.CallableStatement {
    public final static String cvsVersion = "$Id: CallableStatement_base.java,v 1.20 2004-03-27 17:24:58 bheineman Exp $";

    private String procedureName = null;
    private boolean lastWasNull = false;
    private int lastOutParam = -1;

    /**
     * Return value of the procedure call.
     */
    private Integer retVal = null;

    /**
     * Set if the procedure call should return a value (i.e the SQL string is
     * of the form &quot;{&#63=call &#133}&quot;.
     */
    private boolean haveRetVal = false;

    public CallableStatement_base(TdsConnection conn_, String sql) throws SQLException {
        this(conn_, sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

    public CallableStatement_base(TdsConnection conn_, String sql, int type, int concurrency)
    throws SQLException {
        super(conn_, type, concurrency);

        // Remove any extra whitespace for comparisons.
        sql = sql.trim();

        StringBuffer result = new StringBuffer(sql.length());
        int numberOfParameters = EscapeProcessor.parameterNativeSQL(sql, result);

        rawQueryString = result.toString();

        int length = rawQueryString.length();
        int i = 0;

        // Skip non-whitespace characters.
        for (; i < length && !Character.isWhitespace(rawQueryString.charAt(i)); i++);

        String call = rawQueryString.substring(0, i);

        if (!call.equalsIgnoreCase("exec") && !call.equalsIgnoreCase("execute")) {
            rawQueryString = "exec " + rawQueryString;
            length = rawQueryString.length();
            i = 4;
        }

        boolean inString = false;

        // Find start of stored procedure name or return variable.
        for (; i < length; i++) {
            final char ch = rawQueryString.charAt(i);

            if (ch == '[' || ch == '"') {
                inString = true;
                break;
            } else if (ch != ' ' // space
                       && ch != '\t' // tab
                       && ch != '\n' // newline
                       && ch != '\r' // carriage-return
                       && ch != '\f') { // form-feed
                // A non ANSI SQL-92 whitespace character was found
                break;
            }
        }

        int pos = i;

        if (rawQueryString.charAt(i) == '?') {
            int retPos = i;

            // Skip non-whitespace characters.
            for (; i < length && rawQueryString.charAt(i) != '='; i++);

            if (rawQueryString.charAt(i) == '=') {
                rawQueryString = rawQueryString.substring(0, retPos) + rawQueryString.substring(i + 1);
                numberOfParameters--;
                haveRetVal = true;
            } else {
                throw new SQLException("Invalid return value syntax; expected to find '=': "
                                       + sql);
            }
        }

        // Find the end of the procedure name
        for (i++; i < length; i++) {
            final char ch = rawQueryString.charAt(i);

            // While this parsing does not validate that a quoted identifier ends
            // with the the same style as it was opened, it does detect the procedure
            // name properly and lets the database validate the identifiers
            // (which it does anyway).
            if (inString) {
                if (ch == ']' || ch == '"') {
                    inString = false;
                }
            } else {
                if (ch == '[' || ch == '"') {
                    inString = true;
                } else if (ch == ' ' // space
                           || ch == '\t' // tab
                           || ch == '\n' // newline
                           || ch == '\r' // carriage-return
                           || ch == '\f') { // form-feed
                    // An ANSI SQL-92 whitespace character was found
                    break;
                }
            }
        }

        if (inString) {
            throw new SQLException("Invalid procedure name detected; expected "
                                   + "idendifer to be closed properly by \" or ]: "
                                   + sql);
        }

        // Skip non-whitespace characters.
        for (; i < length && !Character.isWhitespace(rawQueryString.charAt(i)); i++);

        procedureName = rawQueryString.substring(pos, i);

        if (procedureName.length() == 0) {
            throw new SQLException("Unable to locate procedure name in: " + sql);
        }

        parameterList = new ParameterListItem[numberOfParameters];

        for (int x = 0; x < numberOfParameters; x++) {
            parameterList[x] = new ParameterListItem();
        }
    }

    private Object getParam(int index) throws SQLException {
        // If the originally provided SQL string started with ?= then the
        // actual positions are shifted to the left and 1 is the return value
        if (haveRetVal) {
            if (index == 1) {
                return retVal;
            }

            index--;
        }

        if (index < 1) {
            throw new SQLException("Invalid parameter index "
                                   + index + ". JDBC indexes start at 1.");
        }

        if (index > parameterList.length) {
            throw new SQLException("Invalid parameter index "
                                   + index + ". This statement only has "
                                   + parameterList.length + " parameters.");
        }

        // JDBC indexes start at 1, java array indexes start at 0 :-(
        index--;
        lastWasNull = (parameterList[index] == null);

        return parameterList[index].value;
    }

    /**
     * Override <code>PreparedStatement_base.setParam</code> to take into
     * account an eventual output parameter (if there is one, the parameters
     * are actually shifted one position).
     */
    protected void setParam(int index, Object value, int type, int scale)
    throws SQLException {
        super.setParam(haveRetVal ? index - 1 : index, value, type, scale);
    }

    protected void addOutputParam(Object value) throws SQLException {
        for (lastOutParam++; lastOutParam<parameterList.length; lastOutParam++) {
            if (parameterList[lastOutParam].isOutput) {
                parameterList[lastOutParam].value = value;
                return;
            }
        }

        throw new SQLException("More output params than expected.");
    }

    // called by TdsStatement.moreResults
    protected boolean handleRetStat(PacketRetStatResult packet) {
        if (!super.handleRetStat(packet)) {
            retVal = new Integer(packet.getRetStat());
        }

        return true;
    }

    protected boolean handleParamResult(PacketOutputParamResult packet) throws SQLException {
        if (!super.handleParamResult(packet)) {
            addOutputParam(packet.value);
        }

        return true;
    }

    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        NotImplemented();
        return null;
    }

    public boolean getBoolean(int parameterIndex) throws SQLException {
        Object value = getParam(parameterIndex);

        if (value == null) {
            return false;
        }

        try {
            return ((Boolean) value).booleanValue();
        } catch (Exception e) {
            throw new SQLException("Unable to convert parameter");
        }
    }

    public byte getByte(int parameterIndex) throws SQLException {
        Object value = getParam(parameterIndex);

        if (value == null) {
            return 0;
        }

        try {
            return ((Number) value).byteValue();
        } catch (Exception e) {
            throw new SQLException("Unable to convert parameter");
        }
    }

    public byte[] getBytes(int parameterIndex) throws SQLException {
        Object value = getParam(parameterIndex);

        if (value == null) {
            return null;
        }

        try {
            return (byte[]) value;
        } catch (Exception e) {
            throw new SQLException("Unable to convert parameter");
        }
    }

    public java.sql.Date getDate(int parameterIndex) throws SQLException {
        NotImplemented();
        return null;
    }

    public double getDouble(int parameterIndex) throws SQLException {
        Object value = getParam(parameterIndex);

        if (value == null) {
            return 0;
        }

        try {
            return ((Number) value).doubleValue();
        } catch (Exception e) {
            throw new SQLException("Unable to convert parameter");
        }
    }

    public float getFloat(int parameterIndex) throws SQLException {
        Object value = getParam(parameterIndex);

        if (value == null) {
            return 0;
        }

        try {
            return ((Number) value).floatValue();
        } catch (Exception e) {
            throw new SQLException("Unable to convert parameter");
        }
    }

    public int getInt(int parameterIndex) throws SQLException {
        Object value = getParam(parameterIndex);

        if (value == null) {
            return 0;
        }

        try {
            return ((Number) value).intValue();
        } catch (Exception e) {
            throw new SQLException("Unable to convert parameter");
        }
    }

    public long getLong(int parameterIndex) throws SQLException {
        Object value = getParam(parameterIndex);

        if (value == null) {
            return 0;
        }

        try {
            return ((Number) value).longValue();
        } catch (Exception e) {
            throw new SQLException("Unable to convert parameter");
        }
    }

    //----------------------------------------------------------------------
    // Advanced features:

    public Object getObject(int parameterIndex) throws SQLException {
        return getParam(parameterIndex);
    }

    public short getShort(int parameterIndex) throws SQLException {
        Object value = getParam(parameterIndex);

        if (value == null) {
            return 0;
        }

        try {
            return ((Number) value).shortValue();
        } catch (Exception e) {
            throw new SQLException("Unable to convert parameter");
        }
    }

    public String getString(int parameterIndex) throws SQLException {
        Object value = getParam(parameterIndex);

        try {
            return (String) value;
        } catch (Exception e) {
            throw new SQLException("Unable to convert parameter");
        }
    }

    public java.sql.Time getTime(int parameterIndex) throws SQLException {
        NotImplemented();
        return null;
    }

    public java.sql.Timestamp getTimestamp(int parameterIndex) throws SQLException {
        NotImplemented();
        return null;
    }

    public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
        registerOutParameter(parameterIndex, sqlType, -1);
    }

    public void registerOutParameter(int parameterIndex, int sqlType, int scale)
    throws SQLException {
        // SAfe If there is a return value, decrement the parameter index or
        //      simply ignore the call if it's for the return value
        if (haveRetVal) {
            if (parameterIndex == 1) {
                // If it's the return value, it can only be an integral value
                // of some kind
                if (sqlType != Types.INTEGER && sqlType != Types.NUMERIC
                    && sqlType != Types.DECIMAL && sqlType != Types.BIGINT) {
                    throw new SQLException("Procedure return value is integer.");
                }

                return;
            }

            parameterIndex -= 1;
        }

        Object value = parameterList[parameterIndex - 1].value;
        // Call directly the method from PreparedStatement_base, otherwise the
        // index will be decremented one more time.
        super.setParam(parameterIndex, null, sqlType, scale);
        parameterList[parameterIndex - 1].isOutput = true;
        // Restore the original value; setParam has set it to null.
        parameterList[parameterIndex - 1].value = value;
    }

    public boolean wasNull() throws SQLException {
        return lastWasNull;
    }

    public boolean execute(Tds tds) throws SQLException {
        closeResults(false);
        lastOutParam = -1;

        // Setup the parameter native types and maximum sizes. Don't assign
        // names to parameters, it's going to crash.
        ParameterUtils.createParameterMapping(
                null, parameterList, tds, false);

        // Execute the stored procedure
        return internalExecuteCall(procedureName, parameterList, parameterList,
                                   tds, warningChain);
    }

    //--------------------------JDBC 2.0-----------------------------

    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
        NotImplemented();
        return null;
    }

    public java.sql.Date getDate(int parameterIndex, Calendar cal) throws SQLException {
        NotImplemented();
        return null;
    }

    public java.sql.Time getTime(int parameterIndex, Calendar cal) throws SQLException {
        NotImplemented();
        return null;
    }

    public java.sql.Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
        NotImplemented();
        return null;
    }

    public void registerOutParameter(int paramIndex, int sqlType, String typeName)
    throws SQLException {
        NotImplemented();
    }

    public Blob getBlob(int i) throws SQLException {
        NotImplemented();
        return null;
    }

    public Clob getClob(int i) throws SQLException {
        NotImplemented();
        return null;
    }

    public Object getObject(int i, java.util.Map map) throws SQLException {
        NotImplemented();
        return null;
    }

    public Ref getRef(int i) throws SQLException {
        NotImplemented();
        return null;
    }

    public Array getArray(int i) throws SQLException {
        NotImplemented();
        return null;
    }

    public void registerOutParameter(String str, int param, String str2) throws SQLException {
        NotImplemented();
    }

    public void registerOutParameter(String str, int param, int param2) throws SQLException {
        NotImplemented();
    }

    public void registerOutParameter(String str, int param) throws SQLException {
        NotImplemented();
    }

    public java.sql.Array getArray(String str) throws SQLException {
        NotImplemented();
        return null;
    }

    public java.math.BigDecimal getBigDecimal(String str) throws SQLException {
        NotImplemented();
        return null;
    }

    public java.sql.Blob getBlob(String str) throws SQLException {
        NotImplemented();
        return null;
    }

    public boolean getBoolean(String str) throws SQLException {
        NotImplemented();
        return false;
    }

    public byte getByte(String str) throws SQLException {
        NotImplemented();
        return 0;
    }

    public byte[] getBytes(String str) throws SQLException {
        NotImplemented();
        return null;
    }

    public java.sql.Clob getClob(String str) throws SQLException {
        NotImplemented();
        return null;
    }

    public java.sql.Date getDate(String str) throws SQLException {
        NotImplemented();
        return null;
    }

    public java.sql.Date getDate(String str, java.util.Calendar calendar) throws SQLException {
        NotImplemented();
        return null;
    }

    public double getDouble(String str) throws SQLException {
        NotImplemented();
        return 0;
    }

    public float getFloat(String str) throws SQLException {
        NotImplemented();
        return 0;
    }

    public int getInt(String str) throws SQLException {
        NotImplemented();
        return 0;
    }

    public long getLong(String str) throws SQLException {
        NotImplemented();
        return 0;
    }

    public Object getObject(String str) throws SQLException {
        NotImplemented();
        return null;
    }

    public Object getObject(String str, java.util.Map map) throws SQLException {
        NotImplemented();
        return null;
    }

    public java.sql.Ref getRef(String str) throws SQLException {
        NotImplemented();
        return null;
    }

    public short getShort(String str) throws SQLException {
        NotImplemented();
        return 0;
    }

    public String getString(String str) throws SQLException {
        NotImplemented();
        return null;
    }

    public java.sql.Time getTime(String str) throws SQLException {
        NotImplemented();
        return null;
    }

    public java.sql.Time getTime(String str, java.util.Calendar calendar) throws SQLException {
        NotImplemented();
        return null;
    }

    public java.sql.Timestamp getTimestamp(String str) throws SQLException {
        NotImplemented();
        return null;
    }

    public java.sql.Timestamp getTimestamp(String str, java.util.Calendar calendar) throws SQLException {
        NotImplemented();
        return null;
    }

    public java.net.URL getURL(int param) throws SQLException {
        NotImplemented();
        return null;
    }

    public java.net.URL getURL(String str) throws SQLException {
        NotImplemented();
        return null;
    }

    public void setAsciiStream(String str, java.io.InputStream inputStream, int param) throws SQLException {
        NotImplemented();
    }

    public void setBigDecimal(String str, java.math.BigDecimal bigDecimal) throws SQLException {
        NotImplemented();
    }

    public void setBinaryStream(String str, java.io.InputStream inputStream, int param) throws SQLException {
        NotImplemented();
    }

    public void setBoolean(String str, boolean param) throws SQLException {
        NotImplemented();
    }

    public void setByte(String str, byte param) throws SQLException {
        NotImplemented();
    }

    public void setBytes(String str, byte[] values) throws SQLException {
        NotImplemented();
    }

    public void setCharacterStream(String str, java.io.Reader reader, int param) throws SQLException {
        NotImplemented();
    }

    public void setDate(String str, java.sql.Date date) throws SQLException {
        NotImplemented();
    }

    public void setDate(String str, java.sql.Date date, java.util.Calendar calendar) throws SQLException {
        NotImplemented();
    }

    public void setDouble(String str, double param) throws SQLException {
        NotImplemented();
    }

    public void setFloat(String str, float param) throws SQLException {
        NotImplemented();
    }

    public void setInt(String str, int param) throws SQLException {
        NotImplemented();
    }

    public void setLong(String str, long param) throws SQLException {
        NotImplemented();
    }

    public void setNull(String str, int param) throws SQLException {
        NotImplemented();
    }

    public void setNull(String str, int param, String str2) throws SQLException {
        NotImplemented();
    }

    public void setObject(String str, Object obj) throws SQLException {
        NotImplemented();
    }

    public void setObject(String str, Object obj, int param) throws SQLException {
        NotImplemented();
    }

    public void setObject(String str, Object obj, int param, int param3) throws SQLException {
        NotImplemented();
    }

    public void setShort(String str, short param) throws SQLException {
        NotImplemented();
    }

    public void setString(String str, String str1) throws SQLException {
        NotImplemented();
    }

    public void setTime(String str, java.sql.Time time) throws SQLException {
        NotImplemented();
    }

    public void setTime(String str, java.sql.Time time, java.util.Calendar calendar) throws SQLException {
        NotImplemented();
    }

    public void setTimestamp(String str, java.sql.Timestamp timestamp) throws SQLException {
        NotImplemented();
    }

    public void setTimestamp(String str, java.sql.Timestamp timestamp, java.util.Calendar calendar) throws SQLException {
        NotImplemented();
    }

    public void setURL(String str, java.net.URL url) throws SQLException {
        NotImplemented();
    }

    public String getProcedureName() {
        return procedureName;
    }
}
