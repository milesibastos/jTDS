package net.sourceforge.jtds.jdbc;

import java.io.*;
import java.math.BigDecimal;
import java.sql.*;
import java.text.*;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

/**
 * This class implements all of the get and update methods, which are delegated to the row object.
 * <p>
 * This is so we can easily subclass this object to provide say cached result sets, or cursor-based
 * result sets.
 *
 * @author   chris
 * @author   Alin Sinpalean
 * @created  17 March 2001
 * @version  $Id: AbstractResultSet.java,v 1.16 2004-05-02 22:45:20 bheineman Exp $
 */
public abstract class AbstractResultSet implements ResultSet {
    public final static String cvsVersion = "$Id: AbstractResultSet.java,v 1.16 2004-05-02 22:45:20 bheineman Exp $";

    public final static int DEFAULT_FETCH_SIZE = 100;
    public static final long HOUR_CONSTANT = 3600000;

    /**
     * Number of rows to fetch once. An implementation may ignore this.
     */
    protected int fetchSize = DEFAULT_FETCH_SIZE;

    /**
     * The <code>ResultSet</code>'s warning chain.
     */
    protected SQLWarningChain warningChain = null;

    /**
     * The <code>ResultSet</code>'s meta data.
     */
    ResultSetMetaData metaData = null;

    /**
     * Used to format numeric values when scale is specified.
     */
    private static NumberFormat f = NumberFormat.getInstance();

    /**
     * Returns the <code>Context</code> of the <code>ResultSet</code> instance. A
     * <code>Context</code> holds information about the <code>ResultSet</code>'s columns.
     */
    public abstract Context getContext();

    /**
     * Returns the current row in the <code>ResultSet</code>, or <code>null</code> if there is no
     * current row.
     *
     * @exception  SQLException  if an SQL error occurs or there is no current row
     */
    public abstract PacketRowResult currentRow() throws SQLException;

    public ResultSetMetaData getMetaData() throws SQLException {
        if (metaData == null) {
            metaData = new TdsResultSetMetaData(getContext().getColumnInfo());
        }

        return metaData;
    }

    public java.io.InputStream getAsciiStream(String columnName) throws SQLException {
        return getAsciiStream(findColumn(columnName));
    }

    public BigDecimal getBigDecimal(String columnName, int scale) throws SQLException {
        return getBigDecimal(findColumn(columnName), scale);
    }

    public java.io.InputStream getBinaryStream(String columnName) throws SQLException {
        return getBinaryStream(findColumn(columnName));
    }

    public boolean getBoolean(String columnName) throws SQLException {
        return getBoolean(findColumn(columnName));
    }

    public byte getByte(String columnName) throws SQLException {
        return getByte(findColumn(columnName));
    }

    public byte[] getBytes(String columnName) throws SQLException {
        return getBytes(findColumn(columnName));
    }

    public Date getDate(String columnName) throws SQLException {
        return getDate(findColumn(columnName));
    }

    public double getDouble(String columnName) throws SQLException {
        return getDouble(findColumn(columnName));
    }

    public float getFloat(String columnName) throws SQLException {
        return getFloat(findColumn(columnName));
    }

    public int getInt(String columnName) throws SQLException {
        return getInt(findColumn(columnName));
    }

    public long getLong(String columnName) throws SQLException {
        return getLong(findColumn(columnName));
    }

    public Object getObject(String columnName) throws SQLException {
        return getObject(findColumn(columnName));
    }

    public short getShort(String columnName) throws SQLException {
        return getShort(findColumn(columnName));
    }

    public String getString(String columnName) throws SQLException {
        return getString(findColumn(columnName));
    }

    public Time getTime(String columnName) throws SQLException {
        return getTime(findColumn(columnName));
    }

    public Timestamp getTimestamp(String columnName) throws SQLException {
        return getTimestamp(findColumn(columnName));
    }

    public java.io.InputStream getUnicodeStream(String columnName) throws SQLException {
        return getUnicodeStream(findColumn(columnName));
    }

    public Ref getRef(String colName) throws SQLException {
        return getRef(findColumn(colName));
    }

    public Timestamp getTimestamp(String columnName, Calendar calendar) throws SQLException {
        return getTimestamp(findColumn(columnName), calendar);
    }

    public Date getDate(String columnName, Calendar calendar) throws SQLException {
        return getDate(findColumn(columnName), calendar);
    }

    public Time getTime(String columnName, Calendar calendar) throws SQLException {
        return getTime(findColumn(columnName), calendar);
    }

    public short getShort(int index) throws SQLException {
        return(short)getLong(index);
    }

    public Object getObject(String colName, java.util.Map map) throws SQLException {
        return getObject(findColumn(colName), map);
    }

    public Blob getBlob(String colName) throws SQLException {
        return getBlob(findColumn(colName));
    }

    public Clob getClob(String colName) throws SQLException {
        return getClob(findColumn(colName));
    }

    public Array getArray(String colName) throws SQLException {
        return getArray(findColumn(colName));
    }

    public float getFloat(int index) throws SQLException {
        return (float) getDouble(index);
    }

    public int getInt(int index) throws SQLException {
        return (int) getLong(index);
    }

    public java.io.InputStream getAsciiStream(int index) throws SQLException {
        String val = getString(index);

        if (val == null) {
            return null;
        }

        try {
            return new ByteArrayInputStream(val.getBytes("ASCII"));
        } catch (UnsupportedEncodingException e) {
            // This should never happen...
            throw TdsUtil.getSQLException("Unexpected encoding exception", null, e);
        }
    }

    public BigDecimal getBigDecimal(int index, int scale) throws SQLException {
        BigDecimal result = getBigDecimal(index);

        if (result == null) {
            return null;
        }

        return result.setScale(scale);
    }

    public java.io.InputStream getBinaryStream(int index) throws SQLException {
        byte[] bytes = getBytes(index);

        if (bytes != null) {
            return new ByteArrayInputStream(bytes);
        }

        return null;
    }

    public boolean getBoolean(int index) throws SQLException {
        Boolean result = (Boolean) currentRow().getObjectAs(index, Types.BIT);

        if (result == null) {
            return false;
        }

        return result.booleanValue();
    }

    public byte getByte(int index) throws SQLException {
        return (byte) getLong(index);
    }

    public byte[] getBytes(int index) throws SQLException {
        return (byte[]) currentRow().getObjectAs(index, Types.BINARY);
    }

    public Date getDate(int index) throws SQLException {
        return (Date) currentRow().getObjectAs(index, Types.DATE);
    }

    public double getDouble(int index) throws SQLException {
        Double result = (Double) currentRow().getObjectAs(index, Types.DOUBLE);

        if (result == null) {
            return 0;
        }

        return result.doubleValue();
    }

    public long getLong(int index) throws SQLException {
        Long result = (Long) currentRow().getObjectAs(index, Types.BIGINT);

        if (result == null) {
            return 0;
        }

        return result.longValue();
    }

    public Object getObject(int index) throws SQLException {
        PacketRowResult row = currentRow();

        if (row == null) {
            throw new SQLException("No current row in the result set.");
        }

        return row.getObject(index);
    }

    public String getString(int index) throws SQLException {
        return (String) currentRow().getObjectAs(index, Types.CHAR);
    }

    public Time getTime(int index) throws SQLException {
        return (Time) currentRow().getObjectAs(index, Types.TIME);
    }

    public java.io.Reader getCharacterStream(int index) throws SQLException {
        String val = getString(index);

        if (val == null) {
            return null;
        }

        return new java.io.StringReader(val);
    }

    public java.io.Reader getCharacterStream(String columnName) throws SQLException {
        return getCharacterStream(findColumn(columnName));
    }

    public BigDecimal getBigDecimal(int index) throws SQLException {
        return (BigDecimal) currentRow().getObjectAs(index, Types.NUMERIC);
    }

    public BigDecimal getBigDecimal(String columnName) throws SQLException {
        return getBigDecimal(findColumn(columnName));
    }

    public Object getObject(int i, java.util.Map map) throws SQLException {
        throw new java.lang.UnsupportedOperationException("Not Implemented");
    }

    public Ref getRef(int i) throws SQLException {
        throw new java.lang.UnsupportedOperationException("Not Implemented");
    }

    public Blob getBlob(int index) throws SQLException {
        return (Blob) currentRow().getObjectAs(index, Types.BLOB);
    }

    public Clob getClob(int index) throws SQLException {
        return (Clob) currentRow().getObjectAs(index, Types.CLOB);
    }

    public Array getArray(int index) throws SQLException {
        throw new java.lang.UnsupportedOperationException("Not Implemented");
    }

    public Timestamp getTimestamp(int index, Calendar calendar)
    throws SQLException {
        Timestamp timestamp = getTimestamp(index);

        if (timestamp != null && calendar != null) {
            TimeZone timeZone = TimeZone.getDefault();
            long newTime = timestamp.getTime();

            newTime -= timeZone.getRawOffset();
            newTime += calendar.getTimeZone().getRawOffset();
            timestamp = new Timestamp(newTime);
        }

        return timestamp;
    }

    public Date getDate(int index, Calendar calendar) throws SQLException {
        Date date = getDate(index);

        if (date != null && calendar != null) {
            TimeZone timeZone = TimeZone.getDefault();
            long newTime = date.getTime();

            newTime -= timeZone.getRawOffset();
            newTime += calendar.getTimeZone().getRawOffset();
            date = new Date(newTime);
        }

        return date;
    }

    public Time getTime(int index, Calendar calendar)
    throws SQLException {
        Time time = getTime(index);

        if (time != null && calendar != null) {
            TimeZone timeZone = TimeZone.getDefault();
            long newTime = time.getTime();

            newTime -= timeZone.getRawOffset();
            newTime += calendar.getTimeZone().getRawOffset();
            time = new Time(newTime);
        }

        return time;
    }

    public Timestamp getTimestamp(int index) throws SQLException {
        return (Timestamp) currentRow().getObjectAs(index, Types.TIMESTAMP);
    }

    public java.io.InputStream getUnicodeStream(int index) throws SQLException {
        String val = getString(index);

        if (val == null) {
            return null;
        }

        try {
            return new ByteArrayInputStream(val.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            // This should never happen...
            throw TdsUtil.getSQLException("Unexpected encoding exception", null, e);
        }
    }

    public int findColumn(String columnName) throws SQLException {
        Columns info = getContext().getColumnInfo();

        for (int i = 1; i <= info.fakeColumnCount(); i++) {
            /** @todo Also need to look at the fully qualified name, i.e. table.column */
            if (info.getName(i).equalsIgnoreCase(columnName)
                    || info.getLabel(i).equalsIgnoreCase(columnName)) {
                return i;
            }
        }

        throw new SQLException("No such column " + columnName);
    }

    public void updateNull(int index) throws SQLException {
        updateObject(index, null);
    }

    public void updateNull(String columnName) throws SQLException {
        updateNull(findColumn(columnName));
    }

    public void updateBoolean(String columnName, boolean x) throws SQLException {
        updateBoolean(findColumn(columnName), x);
    }

    public void updateByte(String columnName, byte x) throws SQLException {
        updateByte(findColumn(columnName), x);
    }

    public void updateShort(String columnName, short x) throws SQLException {
        updateShort(findColumn(columnName), x);
    }

    public void updateInt(String columnName, int x) throws SQLException {
        updateInt(findColumn(columnName), x);
    }

    public void updateLong(String columnName, long x) throws SQLException {
        updateLong(findColumn(columnName), x);
    }

    public void updateFloat(String columnName, float x) throws SQLException {
        updateFloat(findColumn(columnName), x);
    }

    public void updateDouble(String columnName, double x) throws SQLException {
        updateDouble(findColumn(columnName), x);
    }

    public void updateBigDecimal(String columnName, BigDecimal x) throws SQLException {
        updateBigDecimal(findColumn(columnName), x);
    }

    public void updateString(String columnName, String x) throws SQLException {
        updateString(findColumn(columnName), x);
    }

    public void updateBytes(String columnName, byte x[]) throws SQLException {
        updateBytes(findColumn(columnName), x);
    }

    public void updateDate(String columnName, Date x) throws SQLException {
        updateDate(findColumn(columnName), x);
    }

    public void updateTime(String columnName, Time x) throws SQLException {
        updateTime(findColumn(columnName), x);
    }

    public void updateTimestamp(String columnName, Timestamp x) throws SQLException {
        updateTimestamp(findColumn(columnName), x);
    }

    public void updateAsciiStream(String columnName, java.io.InputStream x, int length)
    throws SQLException {
        updateAsciiStream(findColumn(columnName), x, length);
    }

    public void updateBinaryStream(String columnName, java.io.InputStream x, int length)
    throws SQLException {
        updateBinaryStream(findColumn(columnName), x, length);
    }

    public void updateCharacterStream(String columnName, java.io.Reader reader, int length)
    throws SQLException {
        updateCharacterStream(findColumn(columnName), reader, length);
    }

    public void updateObject(String columnName, Object x, int scale) throws SQLException {
        updateObject(findColumn(columnName), x, scale);
    }

    public void updateObject(String columnName, Object x) throws SQLException {
        updateObject(findColumn(columnName), x);
    }

    public boolean wasNull() throws SQLException {
        return currentRow().wasNull();
    }

    public void updateBoolean(int index, boolean x) throws SQLException {
        updateObject(index, new Boolean(x));
    }

    public void updateByte(int index, byte x) throws SQLException {
        updateObject(index, new Byte(x));
    }

    public void updateShort(int index, short x) throws SQLException {
        updateObject(index, new Short(x));
    }

    public void updateInt(int index, int x) throws SQLException {
        updateObject(index, new Integer(x));
    }

    public void updateLong(int index, long x) throws SQLException {
        updateObject(index, new Long(x));
    }

    public void updateFloat(int index, float x) throws SQLException {
        updateObject(index, new Float(x));
    }

    public void updateDouble(int index, double x) throws SQLException {
        updateObject(index, new Double(x));
    }

    public void updateBigDecimal(int index, BigDecimal x) throws SQLException {
        updateObject(index, x);
    }

    public void updateString(int index, String x) throws SQLException {
        updateObject(index, x);
    }

    public void updateBytes(int index, byte x[]) throws SQLException {
        updateObject(index, x);
    }

    public void updateDate(int index, Date x) throws SQLException {
        updateObject(index, x);
    }

    public void updateTime(int index, Time x) throws SQLException {
        updateObject(index, x);
    }

    public void updateTimestamp(int index, Timestamp x) throws SQLException {
        updateObject(index, x);
    }

    public void updateAsciiStream(int index, java.io.InputStream inputStream, int length)
    throws SQLException {
        if (inputStream == null) {
            updateCharacterStream(index, null, 0);
        } else {
            try {
                updateCharacterStream(index,
                                      new java.io.InputStreamReader(inputStream, "ASCII"),
                                      length);
            } catch (java.io.UnsupportedEncodingException e) {
                // This should never happen...
                throw TdsUtil.getSQLException("Unexpected encoding exception", null, e);
            }
        }
    }

    public void updateBinaryStream(int index, java.io.InputStream inputStream, int length)
    throws SQLException {
        if (inputStream == null || length == 0) {
            updateBytes(index, null);
            return;
        }

        byte[] bs = new byte[length];
        int actlen;

        try {
            actlen = inputStream.read(bs);
        } catch (java.io.IOException e) {
            throw new SQLException("setBinaryStream: IO-Exception occured reading Stream" + e.toString());
        }

        if (actlen != length) {
            throw new SQLException("SetBinaryStream parameterized Length: " + Integer.toString(length) + " got length: " + Integer.toString(actlen));
        } else {
            try {
                actlen = inputStream.read(bs);
            } catch (java.io.IOException e) {
                throw new SQLException("setBinaryStream: IO-Exception occured reading Stream" + e.toString());
            }

            if (actlen != -1) {
                throw new SQLException("SetBinaryStream parameterized Length: " + Integer.toString(length) + " got more than that ");
            }
        }

        updateBytes(index, bs);
    }

    public void updateCharacterStream(int index, java.io.Reader reader, int length)
    throws SQLException {
        if (reader == null || length < 0) {
            updateString(index, null);
            return;
        }

        StringBuffer value = new StringBuffer(length);
        char[] buffer = new char[1024];
        int bytes;

        try {
            while ((bytes = reader.read(buffer, 0, buffer.length)) != -1) {
                value.append(buffer, 0, bytes);
            }
        } catch (java.io.IOException e) {
            throw TdsUtil.getSQLException("Error reading stream", null, e);
        }

        updateString(index, value.toString());
    }

    public void updateObject(int index, Object x, int scale) throws SQLException {
        if (x instanceof BigDecimal) {
            f.setMaximumFractionDigits(scale);
            updateObject(index, ((BigDecimal)x).setScale(scale));
        } else if (x instanceof Number) {
            f.setMaximumFractionDigits(scale);
            updateObject(index, f.format(x));
        } else {
            updateObject(index, x);
        }
    }

    public void updateObject(int index, Object x) throws SQLException {
        if (x instanceof Blob) {
            updateBlob(index, (Blob) x);
        } else if (x instanceof Clob) {
            updateClob(index, (Clob) x);
        } else {
            currentRow().setElementAt(index, x);
        }
    }

    public void updateRef(int param, Ref ref) throws SQLException {
        throw new SQLException("Not Implemented");
    }

    public void updateRef(String columnName, Ref ref) throws SQLException {
        updateRef(findColumn(columnName), ref);
    }

    public void updateClob(int param, Clob clob) throws SQLException {
        if (clob == null) {
            updateCharacterStream(param, null, 0);
        } else {
            updateCharacterStream(param, clob.getCharacterStream(), (int) clob.length());
        }
    }

    public void updateClob(String columnName, Clob clob) throws SQLException {
        updateClob(findColumn(columnName), clob);
    }

    public void updateBlob(String columnName, Blob blob) throws SQLException {
        updateBlob(findColumn(columnName), blob);
    }

    public void updateBlob(int param, Blob blob) throws SQLException {
        if (blob == null) {
            updateBinaryStream(param, null, 0);
        } else {
            updateBinaryStream(param, blob.getBinaryStream(), (int) blob.length());
        }
    }

    public void updateArray(String columnName, Array array) throws SQLException {
        updateArray(findColumn(columnName), array);
    }

    public void updateArray(int param, Array array) throws SQLException {
        throw new SQLException("Not Implemented");
    }

    public java.net.URL getURL(String columnName) throws SQLException {
        return getURL(findColumn(columnName));
    }

    public java.net.URL getURL(int param) throws SQLException {
        throw new SQLException("Not Implemented");
    }
}
