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
import java.util.TimeZone;

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
 * @version  $Id: CallableStatement_base.java,v 1.23 2004-05-02 04:08:08 bheineman Exp $
 */
public class CallableStatement_base extends PreparedStatement_base
implements CallableStatement {
    public final static String cvsVersion = "$Id: CallableStatement_base.java,v 1.23 2004-05-02 04:08:08 bheineman Exp $";

    private final static int ACCESS_UNKNOWN = 0;
    private final static int ACCESS_ORDINAL = 1;
    private final static int ACCESS_NAMED   = 2;

    private String procedureName = null;
    private boolean lastWasNull = false;
    private int lastOutParam = -1;

    /**
     * Set to <code>ACCESS_NAMED</code> if this
     * <code>CallableStatement</code> is using named parameters,
     * <code>ACCESS_ORDINAL</code> if using ordinal
     * parameters and <code>ACCESS_UNKNOWN</code> if no parameter has
     * been set yet (and any of the two access types is still possible).
     */
    private int accessType = ACCESS_UNKNOWN;

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
                length = rawQueryString.length();
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

    /**
     * Check that the <code>CallableStatement</code> is using the expected
     * access type. There are two possible access types, ordinal (as in
     * <code>setString(1, "value")</code>) and named (as in
     * <code>setString("@param", "value")</code>), and access to parameters
     * must be consistent in using only one of them. Mixing access types will
     * generate an <code>SQLException</code> and that is what this method is
     * enforcing.
     *
     * @param expected      one of <code>ACCESS_ORDINAL</code> and
     *                      <code>ACCESS_NAMED</code>; if the current access
     *                      type is <code>ACCESS_UNKNOWN</code>, then it is set
     *                      to this value; otherwise if doesn't match this value
     *                      an <code>SQLException</code> is thrown
     * @throws SQLException if the current access type is not
     *                      <code>ACCESS_UNKNOWN</code> and is different from
     *                      <code>expected</code>
     */
    private void checkAccessType(int expected) throws SQLException {
        if (accessType != expected) {
            if (accessType == ACCESS_UNKNOWN) {
                accessType = expected;
            } else {
                throw new SQLException(
                        "Named and ordinal parameter access cannot be mixed.",
                        "HY000");
            }
        }
    }

    private Object getParam(int index, int sqlType) throws SQLException {
        // TODO We should check for access type, too. We can't do it in the
        //      current implementation but we should be able to if we move the
        //      whole conversion for getters and setters in a single place, say
        //      ParameterUtils
//        checkAccessType(ACCESS_ORDINAL);

        // If the originally provided SQL string started with ?= then the
        // actual positions are shifted to the left and 1 is the return value
        if (haveRetVal) {
            if (index == 1) {
                return retVal;
            }

            --index;
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
        --index;
        lastWasNull = (parameterList[index] == null);

        // If the type is unknown just return the value...
        if (sqlType == Integer.MIN_VALUE) {
            return parameterList[index].value;
        }

        return ParameterUtils.getObject(sqlType,
                                        parameterList[index].value,
                                        connection.getEncoder());
    }

    /**
     * Override <code>PreparedStatement_base.setParam</code> to take into
     * account an eventual output parameter (if there is one, the parameters
     * are actually shifted one position).
     */
    protected void setParam(int index, Object value, int type, int scale)
            throws SQLException {
        // TODO We should check for access type, too. We can't do it in the
        //      current implementation but we should be able to if we move the
        //      whole conversion for getters and setters in a single place, say
        //      ParameterUtils
//        checkAccessType(ACCESS_ORDINAL);
        super.setParam(haveRetVal ? index - 1 : index, value, type, scale);
    }

    private void addOutputValue(Object value) throws SQLException {
        for (lastOutParam++; lastOutParam<parameterList.length; lastOutParam++) {
            if (parameterList[lastOutParam].isOutput) {
                parameterList[lastOutParam].value = value;
                return;
            }
        }

        throw new SQLException("More output params than expected.", "HY000");
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
            addOutputValue(packet.value);
        }

        return true;
    }

    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        BigDecimal result = getBigDecimal(parameterIndex);

        if (result == null) {
            return null;
        }

        return result.setScale(scale);
    }

    public boolean getBoolean(int parameterIndex) throws SQLException {
        Boolean result = (Boolean) getParam(parameterIndex, Types.BIT);

        if (result == null) {
            return false;
        }

        return result.booleanValue();
    }

    public byte getByte(int parameterIndex) throws SQLException {
        Byte result = (Byte) getParam(parameterIndex, Types.TINYINT);

        if (result == null) {
            return 0;
        }

        return result.byteValue();
    }

    public byte[] getBytes(int parameterIndex) throws SQLException {
        return (byte[]) getParam(parameterIndex, Types.BINARY);
    }

    public Date getDate(int parameterIndex) throws SQLException {
        return (Date) getParam(parameterIndex, Types.DATE);
    }

    public double getDouble(int parameterIndex) throws SQLException {
        Double result = (Double) getParam(parameterIndex, Types.DOUBLE);

        if (result == null) {
            return 0;
        }

        return result.doubleValue();
    }

    public float getFloat(int parameterIndex) throws SQLException {
        Float result = (Float) getParam(parameterIndex, Types.FLOAT);

        if (result == null) {
            return 0;
        }

        return result.floatValue();
    }

    public short getShort(int parameterIndex) throws SQLException {
        Short result = (Short) getParam(parameterIndex, Types.SMALLINT);

        if (result == null) {
            return 0;
        }

        return result.shortValue();
    }

    public int getInt(int parameterIndex) throws SQLException {
        Integer result = (Integer) getParam(parameterIndex, Types.INTEGER);

        if (result == null) {
            return 0;
        }

        return result.intValue();
    }

    public long getLong(int parameterIndex) throws SQLException {
        Long result = (Long) getParam(parameterIndex, Types.BIGINT);

        if (result == null) {
            return 0;
        }

        return result.longValue();
    }

    //----------------------------------------------------------------------
    // Advanced features:

    public Object getObject(int parameterIndex) throws SQLException {
        return getParam(parameterIndex, Integer.MIN_VALUE);
    }

    public String getString(int parameterIndex) throws SQLException {
        return (String) getParam(parameterIndex, Types.CHAR);
    }

    public Time getTime(int parameterIndex) throws SQLException {
        return (Time) getParam(parameterIndex, Types.TIME);
    }

    public Timestamp getTimestamp(int parameterIndex) throws SQLException {
        return (Timestamp) getParam(parameterIndex, Types.TIMESTAMP);
    }

    public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
        registerOutParameter(parameterIndex, sqlType, -1);
    }

    public void registerOutParameter(int parameterIndex, int sqlType, int scale)
            throws SQLException {
        checkAccessType(ACCESS_ORDINAL);
        internalRegisterOutParameter(parameterIndex, sqlType, scale);
    }

    public void internalRegisterOutParameter(int parameterIndex, int sqlType, int scale)
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
        return (BigDecimal) getParam(parameterIndex, Types.NUMERIC);
    }

    public Date getDate(int parameterIndex, Calendar calendar) throws SQLException {
        Date date = getDate(parameterIndex);

        if (date != null && calendar != null) {
            TimeZone timeZone = TimeZone.getDefault();
            long newTime = date.getTime();

            newTime -= timeZone.getRawOffset();
            newTime += calendar.getTimeZone().getRawOffset();
            date = new Date(newTime);
        }

        return date;
    }

    public Time getTime(int parameterIndex, Calendar calendar) throws SQLException {
        Time time = getTime(parameterIndex);

        if (time != null && calendar != null) {
            TimeZone timeZone = TimeZone.getDefault();
            long newTime = time.getTime();

            newTime -= timeZone.getRawOffset();
            newTime += calendar.getTimeZone().getRawOffset();
            time = new Time(newTime);
        }

        return time;
    }

    public Timestamp getTimestamp(int parameterIndex, Calendar calendar) throws SQLException {
        Timestamp timestamp = getTimestamp(parameterIndex);

        if (timestamp != null && calendar != null) {
            TimeZone timeZone = TimeZone.getDefault();
            long newTime = timestamp.getTime();

            newTime -= timeZone.getRawOffset();
            newTime += calendar.getTimeZone().getRawOffset();
            timestamp = new Timestamp(newTime);
        }

        return timestamp;
    }

    public void registerOutParameter(int paramIndex, int sqlType, String typeName)
    throws SQLException {
        NotImplemented();
    }

    public Blob getBlob(int paramIndex) throws SQLException {
        return (Blob) getParam(paramIndex, Types.BLOB);
    }

    public Clob getClob(int paramIndex) throws SQLException {
        return (Clob) getParam(paramIndex, Types.BLOB);
    }

    public Object getObject(int paramIndex, java.util.Map map) throws SQLException {
        NotImplemented();
        return null;
    }

    public Ref getRef(int paramIndex) throws SQLException {
        NotImplemented();
        return null;
    }

    public Array getArray(int paramIndex) throws SQLException {
        NotImplemented();
        return null;
    }

    protected int getParamNo(String parameterName, boolean checkIsOut, boolean assign)
            throws SQLException {
        checkAccessType(ACCESS_NAMED);

        // Check if it's the return status (its name is @RETURN_STATUS because
        // that's how getColumns returns it and that's what spec says should be
        // used as parameter name).
        if (parameterName.equalsIgnoreCase("@return_status")) {
//            if (assign) {
//                throw new SQLException("Parameter type is OUT: " + parameterName,
//                        "HY000");
//            }

            if (haveRetVal) {
                return 1;
            } else {
                // We could maybe provide the return status even if it was not
                // registered as output parameter
                throw new SQLException("No such parameter: " + parameterName,
                        "HY000");
            }
        }

        // Not the return status, look for it through the list and if not found
        // and assign is true, then assign it
        // TODO Improve the search by using a HashMap instead?
    	for (int i = 0; i < parameterList.length; ++i) {
            String formalName = parameterList[i].formalName;

            // If the parameter name not found, we should add it
            if (formalName == null) {
                // If not assigning parameter names, break and throw the
                // exception
                if (!assign) {
                    break;
                }
                // Ok to assign the parameter name and return the position
                parameterList[i].formalName = parameterName;
                return i + (haveRetVal ? 2 : 1);
            }

    		if (formalName.equalsIgnoreCase(parameterName)) {
                // TODO Should we keep this check? It works with ordinals.
    			if (checkIsOut) {
    				if (!parameterList[i].isOutput) {
    					throw new SQLException(
                                "Parameter type is IN: " + parameterName, "HY000");
    				}
    			}
                return i + (haveRetVal ? 2 : 1);
    		}
    	}

		throw new SQLException("No such parameter: " + parameterName, "HY000");
	}

    public void registerOutParameter(String parameterName,
                                     int sqlType,
                                     String typeName)
            throws SQLException {
        NotImplemented();
    }

    public void registerOutParameter(String parameterName,
                                     int sqlType,
                                     int scale)
            throws SQLException {
        internalRegisterOutParameter(
                getParamNo(parameterName, false, true), sqlType, scale);
    }

    public void registerOutParameter(String parameterName, int sqlType)
            throws SQLException {
    	registerOutParameter(parameterName, sqlType, -1);
    }

    public Array getArray(String parameterName) throws SQLException {
        return getArray(getParamNo(parameterName, true, false));
    }

    public java.math.BigDecimal getBigDecimal(String parameterName)
            throws SQLException {
        return getBigDecimal(getParamNo(parameterName, true, false));
    }

    public Blob getBlob(String parameterName) throws SQLException {
        return getBlob(getParamNo(parameterName, true, false));
    }

    public boolean getBoolean(String parameterName) throws SQLException {
        return getBoolean(getParamNo(parameterName, true, false));
    }

    public byte getByte(String parameterName) throws SQLException {
        return getByte(getParamNo(parameterName, true, false));
    }

    public byte[] getBytes(String parameterName) throws SQLException {
        return getBytes(getParamNo(parameterName, true, false));
    }

    public Clob getClob(String parameterName) throws SQLException {
        return getClob(getParamNo(parameterName, true, false));
    }

    public Date getDate(String parameterName) throws SQLException {
        return getDate(getParamNo(parameterName, true, false));
    }

    public Date getDate(String parameterName, java.util.Calendar cal)
            throws SQLException {
        return getDate(getParamNo(parameterName, true, false), cal);
    }

    public double getDouble(String parameterName) throws SQLException {
        return getDouble(getParamNo(parameterName, true, false));
    }

    public float getFloat(String parameterName) throws SQLException {
        return getFloat(getParamNo(parameterName, true, false));
    }

    public int getInt(String parameterName) throws SQLException {
    	return getInt(getParamNo(parameterName, true, false));
    }

    public long getLong(String parameterName) throws SQLException {
        return getLong(getParamNo(parameterName, true, false));
    }

    public Object getObject(String parameterName) throws SQLException {
        return getObject(getParamNo(parameterName, true, false));
    }

    public Object getObject(String parameterName, java.util.Map map)
            throws SQLException {
        return getObject(getParamNo(parameterName, true, false), map);
    }

    public Ref getRef(String parameterName) throws SQLException {
        return getRef(getParamNo(parameterName, true, false));
    }

    public short getShort(String parameterName) throws SQLException {
        return getShort(getParamNo(parameterName, true, false));
    }

    public String getString(String parameterName) throws SQLException {
    	return getString(getParamNo(parameterName, true, false));
    }

    public Time getTime(String parameterName) throws SQLException {
        return getTime(getParamNo(parameterName, true, false));
    }

    public Time getTime(String parameterName, java.util.Calendar cal)
            throws SQLException {
        return getTime(getParamNo(parameterName, true, false), cal);
    }

    public Timestamp getTimestamp(String parameterName)
            throws SQLException {
        return getTimestamp(getParamNo(parameterName, true, false));
    }

    public Timestamp getTimestamp(String parameterName, java.util.Calendar cal)
            throws SQLException {
        return getTimestamp(getParamNo(parameterName, true, false), cal);
    }

    public java.net.URL getURL(int parameterIndex) throws SQLException {
        NotImplemented();
        return null;
    }

    public java.net.URL getURL(String parameterName) throws SQLException {
        return getURL(getParamNo(parameterName, true, false));
    }

    public void setAsciiStream(String parameterName, java.io.InputStream x, int length)
            throws SQLException {
        setAsciiStream(getParamNo(parameterName, false, true), x, length);
    }

    public void setBigDecimal(String parameterName, java.math.BigDecimal x)
            throws SQLException {
        setBigDecimal(getParamNo(parameterName, false, true), x);
    }

    public void setBinaryStream(String parameterName, java.io.InputStream x, int length)
            throws SQLException {
        setBinaryStream(getParamNo(parameterName, false, true), x, length);
    }

    public void setBoolean(String parameterName, boolean x)
            throws SQLException {
        setBoolean(getParamNo(parameterName, false, true), x);
    }

    public void setByte(String parameterName, byte x)
            throws SQLException {
        setByte(getParamNo(parameterName, false, true), x);
    }

    public void setBytes(String parameterName, byte[] x)
            throws SQLException {
        setBytes(getParamNo(parameterName, false, true), x);
    }

    public void setCharacterStream(String parameterName, java.io.Reader x, int length)
            throws SQLException {
        setCharacterStream(getParamNo(parameterName, false, true), x, length);
    }

    public void setDate(String parameterName, Date x)
            throws SQLException {
        setDate(getParamNo(parameterName, false, true), x);
    }

    public void setDate(String parameterName, Date x, java.util.Calendar cal)
            throws SQLException {
        setDate(getParamNo(parameterName, false, true), x, cal);
    }

    public void setDouble(String parameterName, double x) throws SQLException {
        setDouble(getParamNo(parameterName, false, true), x);
    }

    public void setFloat(String parameterName, float x) throws SQLException {
        setFloat(getParamNo(parameterName, false, true), x);
    }

    public void setInt(String parameterName, int x) throws SQLException {
    	setInt(getParamNo(parameterName, false, true), x);
    }

    public void setLong(String parameterName, long x) throws SQLException {
        setLong(getParamNo(parameterName, false, true), x);
    }

    public void setNull(String parameterName, int sqlType) throws SQLException {
        setNull(getParamNo(parameterName, false, true), sqlType);
    }

    public void setNull(String parameterName, int sqlType, String typeName)
            throws SQLException {
        setNull(getParamNo(parameterName, false, true), sqlType, typeName);
    }

    public void setObject(String parameterName, Object x) throws SQLException {
        setObject(getParamNo(parameterName, false, true), x);
    }

    public void setObject(String parameterName, Object x, int targetSqlType)
            throws SQLException {
        setObject(getParamNo(parameterName, false, true), x, targetSqlType);
    }

    public void setObject(String parameterName, Object x, int targetSqlType, int scale)
            throws SQLException {
        setObject(getParamNo(parameterName, false, true), x, targetSqlType, scale);
    }

    public void setShort(String parameterName, short x) throws SQLException {
        setShort(getParamNo(parameterName, false, true), x);
    }

    public void setString(String parameterName, String x) throws SQLException {
        setString(getParamNo(parameterName, false, true), x);
    }

    public void setTime(String parameterName, Time x)
            throws SQLException {
        setTime(getParamNo(parameterName, false, true), x);
    }

    public void setTime(String parameterName, Time x, java.util.Calendar cal)
            throws SQLException {
        setTime(getParamNo(parameterName, false, true), x, cal);
    }

    public void setTimestamp(String parameterName, Timestamp x)
            throws SQLException {
        setTimestamp(getParamNo(parameterName, false, true), x);
    }

    public void setTimestamp(String parameterName, Timestamp x, java.util.Calendar cal)
            throws SQLException {
        setTimestamp(getParamNo(parameterName, false, true), x, cal);
    }

    public void setURL(String parameterName, java.net.URL x)
            throws SQLException {
        setURL(getParamNo(parameterName, false, true), x);
    }

    public String getProcedureName() {
        return procedureName;
    }
}
