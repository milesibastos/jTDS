package com.internetcds.jdbc.tds;

import java.io.InputStream;
import java.sql.*;
import java.math.BigDecimal;
import java.util.Calendar;
import java.io.Reader;
import java.sql.SQLException;
import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Time;
import java.text.*;

/**
 * This implements all of the get and update methods,
 * which are delegated to the row object.
 *
 * This is so we can easily subclass this object to
 * provide say cached Recordsets, or Cursor based recordsets.
 *
 *@author     chris
 *@created    17 March 2001
 */
public abstract class AbstractResultSet
         implements ResultSet {

    /**
     *  Description of the Field
     */
    protected int fetchSize = 100;

    /**
     *  Description of the Field
     */
    protected SQLWarningChain warningChain = null;


    ResultSetMetaData metaData = null;

    /**
     *  The number, types and properties of a ResultSet's columns are provided
     *  by the getMetaData method.
     *
     *@return                   the description of a ResultSet's columns
     *@exception  SQLException  if a database-access error occurs.
     */
    public ResultSetMetaData getMetaData() throws SQLException
    {
        if (metaData == null) {
            metaData = new TdsResultSetMetaData( getContext().getColumnInfo() );
        }
        return metaData;
    }

    public abstract Context getContext();

    public java.io.InputStream getAsciiStream(String columnName) throws SQLException
    {
        return getAsciiStream(findColumn(columnName));
    }


    public BigDecimal getBigDecimal(String columnName, int scale) throws SQLException
    {
        return getBigDecimal(findColumn(columnName), scale);
    }


    public java.io.InputStream getBinaryStream(String columnName)
             throws SQLException
    {
        return getBinaryStream(findColumn(columnName));
    }


    public boolean getBoolean(String columnName) throws SQLException
    {
        return getBoolean(findColumn(columnName));
    }


    public byte getByte(String columnName) throws SQLException
    {
        return getByte(findColumn(columnName));
    }


    public byte[] getBytes(String columnName) throws SQLException
    {
        return getBytes(findColumn(columnName));
    }


    public java.sql.Date getDate(String columnName) throws SQLException
    {
        return getDate(findColumn(columnName));
    }


    public double getDouble(String columnName) throws SQLException
    {
        return getDouble(findColumn(columnName));
    }


    public float getFloat(String columnName) throws SQLException
    {
        return getFloat(findColumn(columnName));
    }


    public int getInt(String columnName) throws SQLException
    {
        return getInt(findColumn(columnName));
    }


    public long getLong(String columnName) throws SQLException
    {
        return getLong(findColumn(columnName));
    }


    public Object getObject(String columnName) throws SQLException
    {
        return getObject(findColumn(columnName));
    }


    public short getShort(String columnName) throws SQLException
    {
        return getShort(findColumn(columnName));
    }


    public String getString(String columnName) throws SQLException
    {
        return getString(findColumn(columnName));
    }


    public java.sql.Time getTime(String columnName) throws SQLException
    {
        return getTime(findColumn(columnName));
    }


    public java.sql.Timestamp getTimestamp(String columnName) throws SQLException
    {
        return getTimestamp(findColumn(columnName));
    }


    public java.io.InputStream getUnicodeStream(String columnName) throws SQLException
    {
        return getUnicodeStream(findColumn(columnName));
    }


    public Ref getRef(String colName) throws SQLException
    {
        return getRef(findColumn(colName));
    }


    public java.sql.Timestamp getTimestamp(String columnName, Calendar cal)
             throws SQLException
    {
        return getTimestamp(findColumn(columnName), cal);
    }


    public java.sql.Date getDate(String columnName, Calendar cal) throws SQLException
    {
        return getDate(findColumn(columnName), cal);
    }


    public java.sql.Time getTime(String columnName, Calendar cal)
             throws SQLException
    {
        return getTime(findColumn(columnName), cal);
    }


    public short getShort(int index) throws SQLException
    {
        return (short) getLong(index);
    }


    public Object getObject(String colName, java.util.Map map) throws SQLException
    {
        return getObject(findColumn(colName), map);
    }


    public Blob getBlob(String colName) throws SQLException
    {
        return getBlob(findColumn(colName));
    }


    public Clob getClob(String colName) throws SQLException
    {
        return getClob(findColumn(colName));
    }


    public Array getArray(String colName) throws SQLException
    {
        return getArray(findColumn(colName));
    }


    public float getFloat(int index) throws SQLException
    {
        return (float) getDouble(index);
    }


    public int getInt(int index) throws SQLException
    {
        return (int) getLong(index);
    }


    /**
     *  A column value can be retrieved as a stream of ASCII characters and then
     *  read in chunks from the stream. This method is particularly suitable for
     *  retrieving large LONGVARCHAR values. The JDBC driver will do any
     *  necessary conversion from the database format into ASCII. <P>
     *
     *  <B>Note:</B> All the data in the returned stream must be read prior to
     *  getting the value of any other column. The next call to a get method
     *  implicitly closes the stream. . Also, a stream may return 0 for
     *  available() whether there is data available or not.
     *
     *@param  index       the first column is 1, the second is 2, ...
     *@return                   a Java input stream that delivers the database
     *      column value as a stream of one byte ASCII characters. If the value
     *      is SQL NULL then the result is null.
     *@exception  SQLException  if a database-access error occurs.
     */
    public java.io.InputStream getAsciiStream(int index) throws SQLException
    {
        String val = getString(index);
        if (val == null) {
            return null;
        }
        try {
            return new ByteArrayInputStream(val.getBytes("ASCII"));
        }
        catch (UnsupportedEncodingException ue) {
            // plain impossible with encoding ASCII
            return null;
        }
    }


    /**
     *  Get the value of a column in the current row as a java.lang.BigDecimal
     *  object.
     *
     *@param  index       the first column is 1, the second is 2, ...
     *@param  scale             the number of digits to the right of the decimal
     *@return                   the column value; if the value is SQL NULL, the
     *      result is null
     *@exception  SQLException  if a database-access error occurs.
     */
    public BigDecimal getBigDecimal(int index, int scale)
             throws SQLException
    {
        return currentRow().getBigDecimal(index, scale);
    }


    /**
     *  A column value can be retrieved as a stream of uninterpreted bytes and
     *  then read in chunks from the stream. This method is particularly
     *  suitable for retrieving large LONGVARBINARY values. <P>
     *
     *  <B>Note:</B> All the data in the returned stream must be read prior to
     *  getting the value of any other column. The next call to a get method
     *  implicitly closes the stream. Also, a stream may return 0 for
     *  available() whether there is data available or not.
     *
     *@param  index       the first column is 1, the second is 2, ...
     *@return                   a Java input stream that delivers the database
     *      column value as a stream of uninterpreted bytes. If the value is SQL
     *      NULL then the result is null.
     *@exception  SQLException  if a database-access error occurs.
     */
    public java.io.InputStream getBinaryStream(int index)
             throws SQLException
    {
        byte[] bytes = getBytes(index);
        if (bytes != null) {
            return new ByteArrayInputStream(bytes);
        }
        return null;
    }


    /**
     *  Get the value of a column in the current row as a Java boolean.
     *
     *@param  index       the first column is 1, the second is 2, ...
     *@return                   the column value; if the value is SQL NULL, the
     *      result is false
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean getBoolean(int index) throws SQLException
    {
        return currentRow().getBoolean(index);
    }


    // getBoolean()

    /**
     *  Get the value of a column in the current row as a Java byte.
     *
     *@param  index       the first column is 1, the second is 2, ...
     *@return                   the column value; if the value is SQL NULL, the
     *      result is 0
     *@exception  SQLException  if a database-access error occurs.
     */
    public byte getByte(int index) throws SQLException
    {
        return (byte) getLong(index);
    }


    /**
     *  Get the value of a column in the current row as a Java byte array. The
     *  bytes represent the raw values returned by the driver.
     *
     *@param  index       the first column is 1, the second is 2, ...
     *@return                   the column value; if the value is SQL NULL, the
     *      result is null
     *@exception  SQLException  if a database-access error occurs.
     */
    public byte[] getBytes(int index) throws SQLException
    {
        return currentRow().getBytes(index);
    }


    /**
     *  Get the value of a column in the current row as a java.sql.Date object.
     *
     *@param  index       the first column is 1, the second is 2, ...
     *@return                   the column value; if the value is SQL NULL, the
     *      result is null
     *@exception  SQLException  if a database-access error occurs.
     */
    public java.sql.Date getDate(int index) throws SQLException
    {
        java.sql.Date result = null;
        java.sql.Timestamp tmp = getTimestamp(index);

        if (tmp != null) {
            result = new java.sql.Date(tmp.getTime());
        }
        return result;
    }


    /**
     *  Get the value of a column in the current row as a Java double.
     *
     *@param  index       the first column is 1, the second is 2, ...
     *@return                   the column value; if the value is SQL NULL, the
     *      result is 0
     *@exception  SQLException  if a database-access error occurs.
     */
    public double getDouble(int index) throws SQLException
    {
        return currentRow().getDouble(index);
    }


    public long getLong(int index) throws SQLException
    {
        return currentRow().getLong(index);
    }


    /**
     *  <p>
     *
     *  Get the value of a column in the current row as a Java object. <p>
     *
     *  This method will return the value of the given column as a Java object.
     *  The type of the Java object will be the default Java Object type
     *  corresponding to the column's SQL type, following the mapping specified
     *  in the JDBC spec. <p>
     *
     *  This method may also be used to read datatabase specific abstract data
     *  types. JDBC 2.0 In the JDBC 2.0 API, the behavior of method <code>
     *  getObject</code> is extended to materialize data of SQL user-defined
     *  types. When the a column contains a structured or distinct value, the
     *  behavior of this method is as if it were a call to:
     *  getObject(index, this.getStatement().getConnection().getTypeMap()).
     *
     *@param  index       the first column is 1, the second is 2, ...
     *@return                   A java.lang.Object holding the column value.
     *@exception  SQLException  if a database-access error occurs.
     */
    public Object getObject(int index) throws SQLException
    {
        if (currentRow() == null) {
            throw new SQLException("No current row in the result set.  " +
                    "Did you call ResultSet.next()?");
        }
        return currentRow().getObject(index);
    }


    public String getString(int index) throws SQLException
    {
        Object tmp = getObject(index);

        if (tmp == null) {
            return null;
        }
        else if (tmp instanceof byte[]) {
            byte[] b = (byte[])tmp;
            StringBuffer buf = new StringBuffer(2*b.length);
            for( int i=0; i<b.length; i++ )
            {
                int n=((int)b[i])&0xFF, v=n/16;
                buf.append((char)(v<10 ? '0'+v : 'A'+v-10));
                v = n%16;
                buf.append((char)(v<10 ? '0'+v : 'A'+v-10));
            }
            return buf.toString();
        }
        else {
            return tmp.toString();
        }
    }


    public java.sql.Time getTime(int index) throws SQLException
    {
        java.sql.Time result = null;
        java.sql.Timestamp tmp = getTimestamp(index);

        if (tmp != null) {
            result = new java.sql.Time(tmp.getTime());
        }
        return result;
    }


    public java.io.Reader getCharacterStream(int index) throws SQLException
    {
        String val = getString(index);
        if (val == null) {
            return null;
        }

        return new java.io.StringReader(val);
    }


    public java.io.Reader getCharacterStream(String columnName) throws SQLException
    {
        return getCharacterStream(findColumn(columnName));
    }


    public BigDecimal getBigDecimal(int index) throws SQLException
    {
        return currentRow().getBigDecimal(index);
    }


    public BigDecimal getBigDecimal(String columnName) throws SQLException
    {
        return currentRow().getBigDecimal(findColumn(columnName));
    }


    public Object getObject(int i, java.util.Map map) throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Not Implemented");
    }


    public Ref getRef(int i) throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Not Implemented");
    }


    public Blob getBlob(int i) throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Not Implemented");
    }


    public Clob getClob(int i) throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Not Implemented");
    }


    public Array getArray(int i) throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Not Implemented");
    }


    public java.sql.Timestamp getTimestamp(int index, Calendar cal)
             throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Not Implemented");
    }


    public java.sql.Date getDate(int index, Calendar cal) throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Not Implemented");
    }


    public java.sql.Time getTime(int index, Calendar cal)
             throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Not Implemented");
    }



    public java.sql.Timestamp getTimestamp(int index) throws SQLException
    {
        return currentRow().getTimestamp(index);
    }


    public java.io.InputStream getUnicodeStream(int index) throws SQLException
    {
        String val = getString(index);
        if (val == null) {
            return null;
        }
        try {
            return new ByteArrayInputStream(val.getBytes("UTF8"));
        }
        catch (UnsupportedEncodingException e) {
            // plain impossible with UTF-8
            return null;
        }
    }


    public int findColumn(String columnName) throws SQLException
    {
        int i;

        Columns info = getContext().getColumnInfo();

        for (i = 1; i <= info.fakeColumnCount(); i++) {
            if ( info.getName(i).equalsIgnoreCase( columnName ) ) {
                return i;
            }
            // XXX also need to look at the fully qualified name ie. table.column
        }
        throw new SQLException("No such column " + columnName);
    }

    public void updateNull(int index) throws SQLException
    {
        updateObject( index, null );
    }


    public void updateNull(String columnName) throws SQLException
    {
        updateNull(findColumn(columnName));
    }


    public void updateBoolean(String columnName, boolean x) throws SQLException
    {
        updateBoolean(findColumn(columnName), x);
    }


    public void updateByte(String columnName, byte x) throws SQLException
    {
        updateByte(findColumn(columnName), x);
    }


    public void updateShort(String columnName, short x) throws SQLException
    {
        updateShort(findColumn(columnName), x);
    }


    public void updateInt(String columnName, int x) throws SQLException
    {
        updateInt(findColumn(columnName), x);
    }


    public void updateLong(String columnName, long x) throws SQLException
    {
        updateLong(findColumn(columnName), x);
    }


    public void updateFloat(String columnName, float x) throws SQLException
    {
        updateFloat(findColumn(columnName), x);
    }


    public void updateDouble(String columnName, double x) throws SQLException
    {
        updateDouble(findColumn(columnName), x);
    }


    public void updateBigDecimal(String columnName, BigDecimal x) throws SQLException
    {
        updateBigDecimal(findColumn(columnName), x);
    }


    public void updateString(String columnName, String x) throws SQLException
    {
        updateString(findColumn(columnName), x);
    }


    public void updateBytes(String columnName, byte x[]) throws SQLException
    {
        updateBytes(findColumn(columnName), x);
    }


    public void updateDate(String columnName, java.sql.Date x) throws SQLException
    {
        updateDate(findColumn(columnName), x);
    }


    public void updateTime(String columnName, java.sql.Time x) throws SQLException
    {
        updateTime(findColumn(columnName), x);
    }


    public void updateTimestamp(String columnName, java.sql.Timestamp x) throws SQLException
    {
        updateTimestamp(findColumn(columnName), x);
    }


    public void updateAsciiStream(String columnName,
            java.io.InputStream x,
            int length) throws SQLException
    {
        updateAsciiStream(findColumn(columnName), x, length);
    }


    public void updateBinaryStream(String columnName,
            java.io.InputStream x,
            int length) throws SQLException
    {
        updateBinaryStream(findColumn(columnName), x, length);
    }


    public void updateCharacterStream(String columnName,
            java.io.Reader reader,
            int length) throws SQLException
    {
        updateCharacterStream(findColumn(columnName), reader, length);
    }


    public void updateObject(String columnName, Object x, int scale) throws SQLException
    {
        updateObject(findColumn(columnName), x, scale);
    }


    public void updateObject(String columnName, Object x) throws SQLException
    {
        updateObject(findColumn(columnName), x);
    }


    /**
     *  A column may have the value of SQL NULL; wasNull reports whether the
     *  last column read had this special value. Note that you must first call
     *  getXXX on a column to try to read its value and then call wasNull() to
     *  find if the value was the SQL NULL.
     *
     *@return                   true if last column read was SQL NULL
     *@exception  SQLException  if a database-access error occurs.
     */
    public boolean wasNull() throws SQLException
    {
        return currentRow().wasNull();
    }


    /**
     *  JDBC 2.0 Updates a column with a boolean value. The <code>updateXXX
     *  </code>methods are used to update column values in the current row, or
     *  the insert row. The <code>updateXXX</code> methods do not update the
     *  underlying database; instead the <code>updateRow</code> or <code>
     *  insertRow</code> methods are called to update the database.
     *
     *@param  index       the first column is 1, the second is 2, ...
     *@param  x                 the new column value
     *@exception  SQLException  if a database access error occurs
     */
    public void updateBoolean(int index, boolean x) throws SQLException
    {
        updateObject( index, new Boolean( x ) );
    }


    /**
     *  JDBC 2.0 Updates a column with a byte value. The <code>updateXXX</code>
     *  methods are used to update column values in the current row, or the
     *  insert row. The <code>updateXXX</code> methods do not update the
     *  underlying database; instead the <code>updateRow</code> or <code>
     *  insertRow</code> methods are called to update the database.
     *
     *@param  index       the first column is 1, the second is 2, ...
     *@param  x                 the new column value
     *@exception  SQLException  if a database access error occurs
     */
    public void updateByte(int index, byte x) throws SQLException
    {
        updateObject( index, new Byte( x ) );
    }


    /**
     *  JDBC 2.0 Updates a column with a short value. The <code>updateXXX
     *  </code>methods are used to update column values in the current row, or
     *  the insert row. The <code>updateXXX</code> methods do not update the
     *  underlying database; instead the <code>updateRow</code> or <code>
     *  insertRow</code> methods are called to update the database.
     *
     *@param  index       the first column is 1, the second is 2, ...
     *@param  x                 the new column value
     *@exception  SQLException  if a database access error occurs
     */
    public void updateShort(int index, short x) throws SQLException
    {
        updateObject( index, new Short( x ) );
    }


    /**
     *  JDBC 2.0 Updates a column with an integer value. The <code>updateXXX
     *  </code>methods are used to update column values in the current row, or
     *  the insert row. The <code>updateXXX</code> methods do not update the
     *  underlying database; instead the <code>updateRow</code> or <code>
     *  insertRow</code> methods are called to update the database.
     *
     *@param  index       the first column is 1, the second is 2, ...
     *@param  x                 the new column value
     *@exception  SQLException  if a database access error occurs
     */
    public void updateInt(int index, int x) throws SQLException
    {
        updateObject( index, new Integer( x ) );
    }


    /**
     *  JDBC 2.0 Updates a column with a long value. The <code>updateXXX</code>
     *  methods are used to update column values in the current row, or the
     *  insert row. The <code>updateXXX</code> methods do not update the
     *  underlying database; instead the <code>updateRow</code> or <code>
     *  insertRow</code> methods are called to update the database.
     *
     *@param  index       the first column is 1, the second is 2, ...
     *@param  x                 the new column value
     *@exception  SQLException  if a database access error occurs
     */
    public void updateLong(int index, long x) throws SQLException
    {
        updateObject( index, new Long( x ) );
    }


    /**
     *  JDBC 2.0 Updates a column with a float value. The <code>updateXXX
     *  </code>methods are used to update column values in the current row, or
     *  the insert row. The <code>updateXXX</code> methods do not update the
     *  underlying database; instead the <code>updateRow</code> or <code>
     *  insertRow</code> methods are called to update the database.
     *
     *@param  index       the first column is 1, the second is 2, ...
     *@param  x                 the new column value
     *@exception  SQLException  if a database access error occurs
     */
    public void updateFloat(int index, float x) throws SQLException
    {
        updateObject( index, new Float( x ) );
    }


    /**
     *  JDBC 2.0 Updates a column with a Double value. The <code>updateXXX
     *  </code>methods are used to update column values in the current row, or
     *  the insert row. The <code>updateXXX</code> methods do not update the
     *  underlying database; instead the <code>updateRow</code> or <code>
     *  insertRow</code> methods are called to update the database.
     *
     *@param  index       the first column is 1, the second is 2, ...
     *@param  x                 the new column value
     *@exception  SQLException  if a database access error occurs
     */
    public void updateDouble(int index, double x) throws SQLException
    {
        updateObject( index, new Double( x ) );
    }


    /**
     *  JDBC 2.0 Updates a column with a BigDecimal value. The <code>updateXXX
     *  </code>methods are used to update column values in the current row, or
     *  the insert row. The <code>updateXXX</code> methods do not update the
     *  underlying database; instead the <code>updateRow</code> or <code>
     *  insertRow</code> methods are called to update the database.
     *
     *@param  index       the first column is 1, the second is 2, ...
     *@param  x                 the new column value
     *@exception  SQLException  if a database access error occurs
     */
    public void updateBigDecimal(int index, BigDecimal x) throws SQLException
    {
        updateObject( index, x );
    }


    /**
     *  JDBC 2.0 Updates a column with a String value. The <code>updateXXX
     *  </code>methods are used to update column values in the current row, or
     *  the insert row. The <code>updateXXX</code> methods do not update the
     *  underlying database; instead the <code>updateRow</code> or <code>
     *  insertRow</code> methods are called to update the database.
     *
     *@param  index       the first column is 1, the second is 2, ...
     *@param  x                 the new column value
     *@exception  SQLException  if a database access error occurs
     */
    public void updateString(int index, String x) throws SQLException
    {
        updateObject( index, x );
    }


    /**
     *  JDBC 2.0 Updates a column with a byte array value. The <code>updateXXX
     *  </code>methods are used to update column values in the current row, or
     *  the insert row. The <code>updateXXX</code> methods do not update the
     *  underlying database; instead the <code>updateRow</code> or <code>
     *  insertRow</code> methods are called to update the database.
     *
     *@param  index       the first column is 1, the second is 2, ...
     *@param  x                 the new column value
     *@exception  SQLException  if a database access error occurs
     */
    public void updateBytes(int index, byte x[]) throws SQLException
    {
        updateObject( index, x );
    }


    /**
     *  JDBC 2.0 Updates a column with a Date value. The <code>updateXXX</code>
     *  methods are used to update column values in the current row, or the
     *  insert row. The <code>updateXXX</code> methods do not update the
     *  underlying database; instead the <code>updateRow</code> or <code>
     *  insertRow</code> methods are called to update the database.
     *
     *@param  index       the first column is 1, the second is 2, ...
     *@param  x                 the new column value
     *@exception  SQLException  if a database access error occurs
     */
    public void updateDate(int index, java.sql.Date x) throws SQLException
    {
        updateObject( index, x );
    }


    /**
     *  JDBC 2.0 Updates a column with a Time value. The <code>updateXXX</code>
     *  methods are used to update column values in the current row, or the
     *  insert row. The <code>updateXXX</code> methods do not update the
     *  underlying database; instead the <code>updateRow</code> or <code>
     *  insertRow</code> methods are called to update the database.
     *
     *@param  index       the first column is 1, the second is 2, ...
     *@param  x                 the new column value
     *@exception  SQLException  if a database access error occurs
     */
    public void updateTime(int index, java.sql.Time x) throws SQLException
    {
        updateObject( index, x );
    }


    /**
     *  JDBC 2.0 Updates a column with a Timestamp value. The <code>updateXXX
     *  </code>methods are used to update column values in the current row, or
     *  the insert row. The <code>updateXXX</code> methods do not update the
     *  underlying database; instead the <code>updateRow</code> or <code>
     *  insertRow</code> methods are called to update the database.
     *
     *@param  index       the first column is 1, the second is 2, ...
     *@param  x                 the new column value
     *@exception  SQLException  if a database access error occurs
     */
    public void updateTimestamp(int index, java.sql.Timestamp x) throws SQLException
    {
        updateObject( index, x );
    }


    /**
     *  JDBC 2.0 Updates a column with an ascii stream value. The <code>
     *  updateXXX</code> methods are used to update column values in the current
     *  row, or the insert row. The <code>updateXXX</code> methods do not update
     *  the underlying database; instead the <code>updateRow</code> or <code>
     *  insertRow</code> methods are called to update the database.
     *
     *@param  index       the first column is 1, the second is 2, ...
     *@param  x                 the new column value
     *@param  length            the length of the stream
     *@exception  SQLException  if a database access error occurs
     */
    public void updateAsciiStream(int index,
            java.io.InputStream x,
            int length) throws SQLException
    {
        throw new SQLException("Not Implemented");
    }


    /**
     *  JDBC 2.0 Updates a column with a binary stream value. The <code>
     *  updateXXX</code> methods are used to update column values in the current
     *  row, or the insert row. The <code>updateXXX</code> methods do not update
     *  the underlying database; instead the <code>updateRow</code> or <code>
     *  insertRow</code> methods are called to update the database.
     *
     *@param  index       the first column is 1, the second is 2, ...
     *@param  x                 the new column value
     *@param  length            the length of the stream
     *@exception  SQLException  if a database access error occurs
     */
    public void updateBinaryStream(int index,
            java.io.InputStream x,
            int length) throws SQLException
    {
        throw new SQLException("Not Implemented");
    }


    /**
     *  JDBC 2.0 Updates a column with a character stream value. The <code>
     *  updateXXX</code> methods are used to update column values in the current
     *  row, or the insert row. The <code>updateXXX</code> methods do not update
     *  the underlying database; instead the <code>updateRow</code> or <code>
     *  insertRow</code> methods are called to update the database.
     *
     *@param  index       the first column is 1, the second is 2, ...
     *@param  x                 the new column value
     *@param  length            the length of the stream
     *@exception  SQLException  if a database access error occurs
     */
    public void updateCharacterStream(int index,
            java.io.Reader x,
            int length) throws SQLException
    {
        throw new SQLException("Not Implemented");
    }

    private static NumberFormat f = NumberFormat.getInstance();

    /**
     *  JDBC 2.0 Updates a column with an Object value. The <code>updateXXX
     *  </code>methods are used to update column values in the current row, or
     *  the insert row. The <code>updateXXX</code> methods do not update the
     *  underlying database; instead the <code>updateRow</code> or <code>
     *  insertRow</code> methods are called to update the database.
     *
     *@param  index       the first column is 1, the second is 2, ...
     *@param  x                 the new column value
     *@param  scale             For java.sql.Types.DECIMAL or
     *      java.sql.Types.NUMERIC types this is the number of digits after the
     *      decimal. For all other types this value will be ignored.
     *@exception  SQLException  if a database access error occurs
     */
    public void updateObject(int index, Object x, int scale) throws SQLException
    {
        if ( x instanceof Number ) {
            f.setMaximumFractionDigits( scale );
            updateObject( index, f.format( ((Number)x).doubleValue() ) );
        }
        else if ( x instanceof BigDecimal ) {
            f.setMaximumFractionDigits( scale );
            updateObject( index, ((BigDecimal)x).setScale( scale ) );
        }
        else {
            updateObject( index, x );
        }
    }


    /**
     *  JDBC 2.0 Updates a column with an Object value. The <code>updateXXX
     *  </code>methods are used to update column values in the current row, or
     *  the insert row. The <code>updateXXX</code> methods do not update the
     *  underlying database; instead the <code>updateRow</code> or <code>
     *  insertRow</code> methods are called to update the database.
     *
     *@param  index       the first column is 1, the second is 2, ...
     *@param  x                 the new column value
     *@exception  SQLException  if a database access error occurs
     */
    public void updateObject(int index, Object x) throws SQLException
    {
        currentRow().setElementAt( index, x );
    }



    public abstract PacketRowResult currentRow() throws SQLException;

    public void updateRef(int param, java.sql.Ref ref) throws java.sql.SQLException
    {
        throw new SQLException("Not Implemented");
    }

    public void updateRef(String columnName, java.sql.Ref ref) throws java.sql.SQLException
    {
        updateRef(findColumn(columnName), ref);
    }

    public void updateClob(int param, java.sql.Clob clob) throws java.sql.SQLException
    {
        throw new SQLException("Not Implemented");
    }

    public void updateClob(String columnName, java.sql.Clob clob) throws java.sql.SQLException
    {
        updateClob(findColumn(columnName), clob);
    }

    public void updateBlob(String columnName, java.sql.Blob blob) throws java.sql.SQLException
    {
        updateBlob(findColumn(columnName), blob);
    }

    public void updateBlob(int param, java.sql.Blob blob) throws java.sql.SQLException
    {
        throw new SQLException("Not Implemented");
    }

    public void updateArray(String columnName, java.sql.Array array) throws java.sql.SQLException
    {
        updateArray(findColumn(columnName), array);
    }

    public void updateArray(int param, java.sql.Array array) throws java.sql.SQLException
    {
        throw new SQLException("Not Implemented");
    }

    public java.net.URL getURL(String columnName) throws java.sql.SQLException
    {
        return getURL(findColumn(columnName));
    }

    public java.net.URL getURL(int param) throws java.sql.SQLException
    {
        throw new SQLException("Not Implemented");
    }

}
