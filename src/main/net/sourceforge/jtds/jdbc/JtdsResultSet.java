// jTDS JDBC Driver for Microsoft SQL Server
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
import java.net.MalformedURLException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;
import java.util.TimeZone;
import java.text.NumberFormat;
import java.io.UnsupportedEncodingException;
import java.io.InputStreamReader;

import net.sourceforge.jtds.util.ReaderInputStream;

/**
 * jTDS Implementation of the java.sql.ResultSet interface supporting forward read
 * only result sets.
 * <p>
 * Implementation notes:
 * <ol>
 * <li>This class is also the base for more sophisticated result sets and
 * incorporates the update methods required by them.
 * <li>The class supports the BLOB/CLOB objects added by Brian.
 * </ol>
 *
 * @author Mike Hutchinson
 * @version $Id: JtdsResultSet.java,v 1.7 2004-07-22 17:09:58 bheineman Exp $
 */
public class JtdsResultSet implements ResultSet {
    /*
     * Constants for backwards compatibility with JDK 1.3
     */
    static final int HOLD_CURSORS_OVER_COMMIT = 1;
    static final int CLOSE_CURSORS_AT_COMMIT = 2;
	
    protected static final int POS_BEFORE_FIRST = 0;
    protected static final int POS_AFTER_LAST = -1;

    /*
     * Protected Instance variables.
     */
    /** The current row number. */
    protected int pos = POS_BEFORE_FIRST;
    /** The number of rows in the result. */
    protected int rowsInResult;
    /** The fetch direction. */
    protected int direction = FETCH_FORWARD;
    /** The result set type. */
    protected int resultSetType = TYPE_FORWARD_ONLY;
    /** The result set concurrency. */
    protected int concurrency = CONCUR_READ_ONLY;
    /** Number of visible columns in row. */
    protected int columnCount;
    /** The array of column descriptors. */
    protected ColInfo[] columns;
    /** The current result set row. */
    protected ColData[] currentRow = null;
    /** True if last column retrieved was null. */
    protected boolean wasNull = false;
    /** The parent statement or null if this is a dummy result set. */
    protected JtdsStatement statement;
    /** True if this result set is closed. */
    protected boolean closed = false;
    /** True if the query has been cancelled by another thread. */
    protected boolean cancelled = false;
    /** The fetch direction. */
    protected int fetchDirection = FETCH_FORWARD;
    /** The fetch size (always one at present). */
    protected int fetchSize = 1;
    /** True if the resultset should read ahead to ensure return parameters are processed. */
    protected boolean readAhead = true;
    /*
     * Private instance variables.
     */
    /** Used to format numeric values when scale is specified. */
    private static NumberFormat f = NumberFormat.getInstance();

    /**
     * Construct a simple result set from a statement, metadata or generated keys.
     *
     * @param statement The parent statement object or null.
     * @param resultSetType one of FORWARD_ONLY, SCROLL_INSENSITIVE, SCROLL_SENSITIVE.
     * @param concurrency One of CONCUR_READ_ONLY, CONCUR_UPDATE.
     * @param columns The array of column descriptors for the result set row.
     * @param readAhead True if the next() method should read ahead to ensure that
     *        stored procedure return parameters are processed. NB. This is
     *        only possible when the cursor is created by executeQuery().
     * @throws SQLException
     */
    JtdsResultSet(JtdsStatement statement,
                  int resultSetType,
                  int concurrency,
                  ColInfo[] columns,
                  boolean readAhead)
        throws SQLException {
        this.statement = statement;
        this.resultSetType = resultSetType;
        this.concurrency = concurrency;
        this.readAhead = readAhead;
        this.columns = columns;

        if (columns != null) {
            columnCount  = getColumnCount(columns);
            rowsInResult = (statement.getTds().isDataInResultSet()) ? 1 : 0;
        }
    }

    /**
     * Retrieve the column count excluding hidden columns
     *
     * @param columns The columns array
     * @return The new column count as an <code>int</code>.
     */
    protected int getColumnCount(ColInfo[] columns) {
        for (int i = columns.length - 1; i >= 0; i--) {
            if (columns[i].isHidden) {
                return i;
            }
        }

        return columns.length;
    }

    /**
     * Retrieve the column descriptor array.
     *
     * @return The column descriptors as a <code>ColInfo[]</code>.
     */
    protected ColInfo[] getColumns() {
        return this.columns;
    }

    /**
     * Set the specified column's name.
     *
     * @param colIndex The index of the column in the row.
     * @param name The new name.
     */
    protected void setColName(int colIndex, String name) {
        if (colIndex < 1 || colIndex > columns.length) {
            throw new IllegalArgumentException("columnIndex "
                    + colIndex + " invalid");
        }

        columns[colIndex - 1].name = name;
    }

    /**
     * Set the specified column's label.
     *
     * @param colIndex The index of the column in the row.
     * @param name The new label.
     */
    protected void setColLabel(int colIndex, String name) {
        if (colIndex < 1 || colIndex > columns.length) {
            throw new IllegalArgumentException("columnIndex "
                    + colIndex + " invalid");
        }

        columns[colIndex - 1].label = name;
    }

    /**
     * Set the specified column's JDBC type.
     *
     * @param colIndex The index of the column in the row.
     * @param jdbcType The new type value.
     */
    protected void setColType(int colIndex, int jdbcType) {
        if (colIndex < 1 || colIndex > columns.length) {
            throw new IllegalArgumentException("columnIndex "
                    + colIndex + " invalid");
        }

        columns[colIndex - 1].jdbcType = jdbcType;
    }

    /**
     * Set the specified column's data value.
     *
     * @param colIndex The index of the column in the row.
     * @param value The new column value.
     */
    protected void setColValue(int colIndex, Object value, int length)
        throws SQLException {
        if (colIndex < 1 || colIndex > columns.length) {
            throw new IllegalArgumentException("columnIndex "
                    + colIndex + " invalid");
        }
    }

    /**
     * Set the current row's column count.
     *
     * @param columnCount The number of visible columns in the row.
     */
    protected void setColumnCount(int columnCount) {
        if (columnCount < 1 || columnCount > columns.length) {
            throw new IllegalArgumentException("columnCount "
                    + columnCount + " is invalid");
        }

        this.columnCount = columnCount;
    }

    /**
     * Get the specified column's data item.
     *
     * @param index The column index in the row.
     * @return The column value as a <code>ColData</code> object.
     * @throws SQLException if the connection is closed;
     *         if <code>index</code> is less than <code>1</code>;
     *         if <code>index</code> is greater that the number of columns;
     *         if there is no current row
     */
    protected ColData getColumn(int index) throws SQLException {
        checkOpen();

        if (index < 1 || index > columnCount) {
            throw new SQLException(Support.getMessage("error.resultset.colindex",
                                                      Integer.toString(index)),
                                                       "07009");
        }

        if (currentRow == null) {
            throw new SQLException(Support.getMessage("error.resultset.norow"), "24000");
        }

        ColData data = currentRow[index - 1];

        wasNull = data.isNull();

        return data;
    }

    /**
     * Check that this connection is still open.
     *
     * @throws SQLException if connection closed.
     */
    protected void checkOpen() throws SQLException {
        if (closed) {
            throw new SQLException(Support.getMessage("error.generic.closed", "ResultSet"),
                                        "HY010");
        }

        if (cancelled) {
            throw new SQLException(Support.getMessage("error.generic.cancelled", "ResultSet"),
                                        "HY010");
        }
    }

    /**
     * Check that this resultset is scrollable.
     *
     * @throws SQLException if connection closed.
     */
    protected void checkScrollable() throws SQLException {
        if (resultSetType == ResultSet.TYPE_FORWARD_ONLY) {
            throw new SQLException(Support.getMessage("error.resultset.fwdonly"), "24000");
        }
    }

    /**
     * Check that this resultset is updateable.
     *
     * @throws SQLException if connection closed.
     */
    protected void checkUpdateable() throws SQLException {
        if (concurrency != ResultSet.CONCUR_UPDATABLE) {
            throw new SQLException(Support.getMessage("error.resultset.readonly"), "24000");
        }
    }

    /**
     * Report that user tried to call a method which has not been implemented.
     *
     * @param method The method name to report in the error message.
     * @throws SQLException
     */
    protected void notImplemented(String method) throws SQLException {
        throw new SQLException(Support.getMessage("error.generic.notimp", method), "HYC00");
    }

    /**
     * Create a new row containing empty data items.
     *
     * @return The new row as a <code>ColData[]</code>.
     */
    protected ColData[] newRow() {
        ColData row[] = new ColData[columns.length];

        for (int i = 0; i < row.length; i++) {
            row[i] = new ColData();
        }

        return row;
    }

    /**
     * Copy an existing result set row.
     *
     * @param row The result set row to copy.
     * @return The new row as a <code>ColData[]</code>.
     */
    protected ColData[] copyRow(ColData[] row) {
        ColData copy[] = new ColData[columns.length];

        System.arraycopy(row,0, copy, 0, row.length);

        return copy;
    }

    /**
     * Copy an existing result set column descriptor array.
     *
     * @param info The result set column descriptors to copy.
     * @return The new descriptors as a <code>ColInfo[]</code>.
     */
    protected ColInfo[] copyInfo(ColInfo[] info) {
        ColInfo copy[] = new ColInfo[info.length];

        System.arraycopy(info, 0, copy, 0, info.length);

        return copy;
    }

//
// -------------------- java.sql.ResultSet methods -------------------
//
    public int getConcurrency() throws SQLException {
        checkOpen();

        return this.concurrency;
    }

    public int getFetchDirection() throws SQLException {
        checkOpen();

        return this.fetchDirection;
    }

    public int getFetchSize() throws SQLException {
        checkOpen();

        return fetchSize;
    }

    public int getRow() throws SQLException {
        checkOpen();

        return pos > 0 ? pos : 0;
    }

    public int getType() throws SQLException {
        checkOpen();

        return resultSetType;
    }

    public void afterLast() throws SQLException {
        checkOpen();
        checkScrollable();
    }

    public void beforeFirst() throws SQLException {
        checkOpen();
        checkScrollable();
    }

    public void cancelRowUpdates() throws SQLException {
        checkOpen();
        checkUpdateable();
    }

    public void clearWarnings() throws SQLException {
        checkOpen();

        if (statement != null) {
            statement.clearWarnings();
        }
    }

    public void close() throws SQLException {
        if (!closed) {
            try {
                if (!statement.getConnection().isClosed()) {
                   // Skip to end of result set
                   // Could send cancel but this is safer as
                   // cancel could kill other statements in a batch.
                   while (next()) ;
                }
            } finally {
                closed = true;
                statement = null;
            }
        }
    }

    public void deleteRow() throws SQLException {
        checkOpen();
        checkUpdateable();
    }

    public void insertRow() throws SQLException {
        checkOpen();
        checkUpdateable();
    }

    public void moveToCurrentRow() throws SQLException {
        checkOpen();
        checkUpdateable();
    }


    public void moveToInsertRow() throws SQLException {
        checkOpen();
        checkUpdateable();
    }

    public void refreshRow() throws SQLException {
        checkOpen();
        checkUpdateable();
    }

    public void updateRow() throws SQLException {
        checkOpen();
        checkUpdateable();
    }

    public boolean first() throws SQLException {
        checkOpen();
        checkScrollable();

        return false;
    }

    public boolean isAfterLast() throws SQLException {
        checkOpen();

        return (pos == POS_AFTER_LAST) && (rowsInResult != 0);
    }

    public boolean isBeforeFirst() throws SQLException {
        checkOpen();

        return (pos == POS_BEFORE_FIRST) && (rowsInResult != 0);
    }

    public boolean isFirst() throws SQLException {
        checkOpen();

        return pos == 1;
    }

    public boolean isLast() throws SQLException {
        checkOpen();

        if (statement.getTds().isDataInResultSet()) {
            rowsInResult = pos + 1; // Keep rowsInResult 1 ahead of pos
        }

        return (pos == rowsInResult) && (rowsInResult != 0);
    }

    public boolean last() throws SQLException {
        checkOpen();
        checkScrollable();

        return false;
    }

    public boolean next() throws SQLException {
        checkOpen();

        if (!statement.getTds().getNextRow(readAhead)) {
            currentRow = null;
        } else {
            currentRow = statement.getTds().getRowData();
            pos++;
            rowsInResult = pos;
        }

        return currentRow != null;
    }

    public boolean previous() throws SQLException {
        checkOpen();
        checkScrollable();

        return false;
    }

    public boolean rowDeleted() throws SQLException {
        checkOpen();
        checkUpdateable();

        return false;
    }

    public boolean rowInserted() throws SQLException {
        checkOpen();
        checkUpdateable();

        return false;
    }

    public boolean rowUpdated() throws SQLException {
        checkOpen();
        checkUpdateable();

        return false;
    }

    public boolean wasNull() throws SQLException {
        checkOpen();

        return wasNull;
    }

    public byte getByte(int columnIndex) throws SQLException {
        ColData data = getColumn(columnIndex);

        return ((Byte) Support.convert(this, data.getValue(), java.sql.Types.TINYINT, null)).byteValue();
    }

    public double getDouble(int columnIndex) throws SQLException {
        ColData data = getColumn(columnIndex);

        return ((Double) Support.convert(this, data.getValue(), java.sql.Types.DOUBLE, null)).doubleValue();
    }

    public float getFloat(int columnIndex) throws SQLException {
        ColData data = getColumn(columnIndex);

        return ((Float) Support.convert(this, data.getValue(), java.sql.Types.FLOAT, null)).floatValue();
    }

    public int getInt(int columnIndex) throws SQLException {
        ColData data = getColumn(columnIndex);

        return ((Integer) Support.convert(this, data.getValue(), java.sql.Types.INTEGER, null)).intValue();
    }

    public long getLong(int columnIndex) throws SQLException {
        ColData data = getColumn(columnIndex);

        return ((Long) Support.convert(this, data.getValue(), java.sql.Types.BIGINT, null)).longValue();
    }

    public short getShort(int columnIndex) throws SQLException {
        ColData data = getColumn(columnIndex);

        return ((Short) Support.convert(this, data.getValue(), java.sql.Types.SMALLINT, null)).shortValue();
    }

    public void setFetchDirection(int direction) throws SQLException {
        checkOpen();
        switch (direction) {
        case FETCH_UNKNOWN:
        case FETCH_REVERSE:
            if (this.resultSetType == ResultSet.TYPE_FORWARD_ONLY) {
                throw new SQLException(Support.getMessage("error.resultset.fwdonly"), "24000");
            }
            // Fall through

        case FETCH_FORWARD:
            this.fetchDirection = direction;
            break;

        default:
            throw new SQLException(Support.getMessage("error.generic.badoption",
                                   Integer.toString(direction),
                                   "setFetchDirection"), "24000");
        }
    }

    public void setFetchSize(int size) throws SQLException {
        checkOpen();

        if (size < 0 || statement != null && size > statement.getFetchSize()) {
            throw new SQLException(Support.getMessage("error.generic.badparam",
                                         Integer.toString(size), "setFetchSize"),
                                            "HY092");
        }

        this.fetchSize = size;
    }

    public void updateNull(int columnIndex) throws SQLException {
        checkUpdateable();
        ColData data = getColumn(columnIndex);
        data.setValue(null);
    }

    public boolean absolute(int row) throws SQLException {
        checkOpen();
        checkScrollable();
        return false;
    }

    public boolean getBoolean(int columnIndex) throws SQLException {
        ColData data = getColumn(columnIndex);

        return ((Boolean) Support.convert(this, data.getValue(), JtdsStatement.BOOLEAN, null)).booleanValue();
    }

    public boolean relative(int row) throws SQLException {
        checkOpen();
        checkScrollable();
        return false;
    }

    public byte[] getBytes(int columnIndex) throws SQLException {
        ColData data = getColumn(columnIndex);
        String charSet = (statement != null) ?
                ((ConnectionJDBC2) statement.getConnection()).getCharSet() : null;
        return (byte[]) Support.convert(this, data.getValue(), java.sql.Types.BINARY, charSet);
    }

    public void updateByte(int columnIndex, byte x) throws SQLException {
        checkOpen();
        checkUpdateable();
        updateObject(columnIndex, new Byte(x));
    }

    public void updateDouble(int columnIndex, double x) throws SQLException {
        checkOpen();
        checkUpdateable();
        updateObject(columnIndex, new Double(x));
    }

    public void updateFloat(int columnIndex, float x) throws SQLException {
        checkOpen();
        checkUpdateable();
        updateObject(columnIndex, new Float(x));
    }

    public void updateInt(int columnIndex, int x) throws SQLException {
        checkOpen();
        checkUpdateable();
        updateObject(columnIndex, new Integer(x));
    }

    public void updateLong(int columnIndex, long x) throws SQLException {
        checkOpen();
        checkUpdateable();
        updateObject(columnIndex, new Long(x));
    }

    public void updateShort(int columnIndex, short x) throws SQLException {
        checkOpen();
        checkUpdateable();
        updateObject(columnIndex, new Short(x));
    }

    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        checkOpen();
        checkUpdateable();
        updateObject(columnIndex, new Boolean(x));
    }

    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        checkOpen();
        checkUpdateable();
        updateObject(columnIndex, x);
    }

    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        Clob clob = getClob(columnIndex);

        if (clob == null) {
            return null;
        }

        return clob.getAsciiStream();
    }

    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        Blob blob = getBlob(columnIndex);

        if (blob == null) {
            return null;
        }

        return blob.getBinaryStream();
    }

    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        Reader reader = getCharacterStream(columnIndex);

        if (reader == null) {
            return null;
        }

        return new ReaderInputStream(reader, "UTF-16BE");
    }

    public void updateAsciiStream(int columnIndex, InputStream inputStream, int length)
        throws SQLException {
        if (inputStream == null || length < 1) {
             updateCharacterStream(columnIndex, null, 0);
        } else {
            try {
                updateCharacterStream(columnIndex, new InputStreamReader(inputStream, "US-ASCII"), length);
            } catch (UnsupportedEncodingException e) {
                // Should never happen!
            }
         }
    }

    public void updateBinaryStream(int columnIndex, InputStream inputStream, int length)
        throws SQLException {
        ColData data = getColumn(columnIndex);

        checkUpdateable();

        if (inputStream == null || length < 1) {
            updateBytes(columnIndex, null);
            return;
        }

        ColInfo ci   = this.columns[columnIndex - 1];

        if (ci.jdbcType != java.sql.Types.BINARY
            && ci.jdbcType != java.sql.Types.VARBINARY
            && ci.jdbcType != java.sql.Types.LONGVARBINARY) {
            throw new SQLException(
                            Support.getMessage("error.convert.badtypes",
                                "Binary Stream",
                                    Support.getJdbcTypeName(ci.jdbcType)), "22005");

        }

        data.setValue(inputStream);
        data.setLength(length);
    }

    public Reader getCharacterStream(int columnIndex) throws SQLException {
        Clob clob = getClob(columnIndex);

        if (clob == null) {
            return null;
        }

        return clob.getCharacterStream();
    }

    public void updateCharacterStream(int columnIndex, Reader reader, int length)
        throws SQLException {
        ColData data = this.getColumn(columnIndex);

        checkUpdateable();

        if (reader == null || length < 1) {
            updateString(columnIndex, null);
            return;
        }

        ColInfo ci = this.columns[columnIndex - 1];

        if (ci.jdbcType != java.sql.Types.CHAR
            && ci.jdbcType != java.sql.Types.VARCHAR
            && ci.jdbcType != java.sql.Types.LONGVARCHAR) {
            throw new SQLException(
                            Support.getMessage("error.convert.badtypes",
                                "Character Stream",
                                    Support.getJdbcTypeName(ci.jdbcType)), "22005");

        }

        data.setValue(reader);
        data.setLength(length);
    }

    public Object getObject(int columnIndex) throws SQLException {
        ColData data = getColumn(columnIndex);

        return data.getValue();
    }

    public void updateObject(int columnIndex, Object x) throws SQLException {
        ColData data = getColumn(columnIndex);
        checkUpdateable();
        ColInfo ci = columns[columnIndex - 1];
        String charSet = ((ConnectionJDBC2) statement.getConnection()).getCharSet();

        x = Support.convert(this, x, ci.jdbcType, charSet);

        if (x instanceof BigDecimal) {
            int prec = ((ConnectionJDBC2) statement.getConnection()).getMaxPrecision();
            x = Support.normalizeBigDecimal((BigDecimal)x, prec);
        }

        data.setValue(x);
    }

    public void updateObject(int columnIndex, Object x, int scale) throws SQLException {
        checkOpen();
        checkUpdateable();

        if (scale < 0 || scale > 28) {
            throw new SQLException(Support.getMessage("error.generic.badscale"), "HY092");
        }

        if (x instanceof BigDecimal) {
            updateObject(columnIndex, ((BigDecimal) x).setScale(scale, BigDecimal.ROUND_HALF_UP));
        } else if (x instanceof Number) {
            f.setMaximumFractionDigits(scale);
            updateObject(columnIndex, f.format(x));
        } else {
            updateObject(columnIndex, x);
        }
    }

    public String getCursorName() throws SQLException {
        checkOpen();
        throw new SQLException(Support.getMessage("error.resultset.noposupdate"), "24000");
    }

    public String getString(int columnIndex) throws SQLException {
        ColData data = getColumn(columnIndex);
        Object tmp = data.getValue();

        if (tmp != null && (tmp instanceof String)){
            return (String) tmp;
        }

        String charSet = (statement != null) ?
                ((ConnectionJDBC2) statement.getConnection()).getCharSet() : null;

        return (String) Support.convert(this, data.getValue(), java.sql.Types.LONGVARCHAR, charSet);
    }

    public void updateString(int columnIndex, String x) throws SQLException {
        checkOpen();
        checkUpdateable();
        updateObject(columnIndex, x);
    }

    public byte getByte(String columnName) throws SQLException {
        return getByte(findColumn(columnName));
    }

    public double getDouble(String columnName) throws SQLException {
        return getDouble(findColumn(columnName));
    }

    public float getFloat(String columnName) throws SQLException {
        return getFloat(findColumn(columnName));
    }

    public int findColumn(String columnName) throws SQLException {
        checkOpen();

        for (int i = 0; i < columnCount; i++) {
            if (columns[i].name.equalsIgnoreCase(columnName)) {
                return i+1;
            }
        }

        throw new SQLException(
            Support.getMessage("error.resultset.colname", columnName), "07009");
    }

    public int getInt(String columnName) throws SQLException {
        return getInt(findColumn(columnName));
    }

    public long getLong(String columnName) throws SQLException {
        return getLong(findColumn(columnName));
    }

    public short getShort(String columnName) throws SQLException {
        return getShort(findColumn(columnName));
    }

    public void updateNull(String columnName) throws SQLException {
        updateObject(findColumn(columnName), null);
    }

    public boolean getBoolean(String columnName) throws SQLException {
        return getBoolean(findColumn(columnName));
    }

    public byte[] getBytes(String columnName) throws SQLException {
        return getBytes(findColumn(columnName));
    }

    public void updateByte(String columnName, byte x) throws SQLException {
        updateByte(findColumn(columnName), x);
    }

    public void updateDouble(String columnName, double x) throws SQLException {
        updateDouble(findColumn(columnName), x);
    }

    public void updateFloat(String columnName, float x) throws SQLException {
        updateFloat(findColumn(columnName), x);
    }

    public void updateInt(String columnName, int x) throws SQLException {
        updateInt(findColumn(columnName), x);
    }

    public void updateLong(String columnName, long x) throws SQLException {
        updateLong(findColumn(columnName), x);
    }

    public void updateShort(String columnName, short x) throws SQLException {
        updateShort(findColumn(columnName), x);
    }

    public void updateBoolean(String columnName, boolean x) throws SQLException {
        updateBoolean(findColumn(columnName), x);
    }

    public void updateBytes(String columnName, byte[] x) throws SQLException {
        updateBytes(findColumn(columnName), x);
    }

    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        ColData data = getColumn(columnIndex);
        return (BigDecimal) Support.convert(this, data.getValue(), java.sql.Types.DECIMAL, null);
    }

    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        ColData data = getColumn(columnIndex);
        BigDecimal result = (BigDecimal) Support.convert(this, data.getValue(), java.sql.Types.DECIMAL, null);

        if (result == null) {
            return null;
        }

        return result.setScale(scale);
    }

    public void updateBigDecimal(int columnIndex, BigDecimal x)
        throws SQLException {
        checkOpen();
        checkUpdateable();
        updateObject(columnIndex, x);
    }

    public URL getURL(int columnIndex) throws SQLException {
        String url = getString(columnIndex);

        try {
            return new java.net.URL(url);
        } catch (MalformedURLException e) {
            throw new SQLException(Support.getMessage("error.resultset.badurl", url), "22000");
        }
    }

    public Array getArray(int columnIndex) throws SQLException {
        checkOpen();
        notImplemented("ResultSet.getArray()");
        return null;
    }

    public void updateArray(int columnIndex, Array x) throws SQLException {
        checkOpen();
        checkUpdateable();
        notImplemented("ResultSet.updateArray()");
    }

    public Blob getBlob(int columnIndex) throws SQLException {
        ColData data = getColumn(columnIndex);

        return (Blob) Support.convert(this, data.getValue(), java.sql.Types.BLOB, null);
    }

    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        if (x == null) {
            updateBinaryStream(columnIndex, null, 0);
        } else {
            updateBinaryStream(columnIndex, x.getBinaryStream(), (int) x.length());
        }
    }

    public Clob getClob(int columnIndex) throws SQLException {
        ColData data = getColumn(columnIndex);

        return (Clob) Support.convert(this, data.getValue(), java.sql.Types.CLOB, null);
    }

    public void updateClob(int columnIndex, Clob x) throws SQLException {
        if (x == null) {
            updateCharacterStream(columnIndex, null, 0);
        } else {
            updateCharacterStream(columnIndex, x.getCharacterStream(), (int) x.length());
        }
    }

    public Date getDate(int columnIndex) throws SQLException {
        ColData data = getColumn(columnIndex);

        return (java.sql.Date)Support.convert(this, data.getValue(), java.sql.Types.DATE, null);
    }

    public void updateDate(int columnIndex, Date x) throws SQLException {
        updateObject(columnIndex, x);
    }

    public Ref getRef(int columnIndex) throws SQLException {
        checkOpen();
        notImplemented("ResultSet.getRef()");

        return null;
    }

    public void updateRef(int columnIndex, Ref x) throws SQLException {
        checkOpen();
        checkUpdateable();
        notImplemented("ResultSet.updateRef()");
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        checkOpen();

        return new JtdsResultSetMetaData(this.columns, this.columnCount);
    }

    public SQLWarning getWarnings() throws SQLException {
        checkOpen();

        return (statement != null) ? statement.getWarnings() : null;
    }

    public Statement getStatement() throws SQLException {
        checkOpen();

        return this.statement;
    }

    public Time getTime(int columnIndex) throws SQLException {
        ColData data = getColumn(columnIndex);

        return (java.sql.Time) Support.convert(this, data.getValue(), java.sql.Types.TIME, null);
    }

    public void updateTime(int columnIndex, Time x) throws SQLException {
        updateObject(columnIndex, x);
    }

    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        ColData data = getColumn(columnIndex);

        return (Timestamp) Support.convert(this, data.getValue(), java.sql.Types.TIMESTAMP, null);
    }

    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        updateObject(columnIndex, x);
    }

    public InputStream getAsciiStream(String columnName) throws SQLException {
        return getAsciiStream(findColumn(columnName));
    }

    public InputStream getBinaryStream(String columnName) throws SQLException {
        return getBinaryStream(findColumn(columnName));
    }

    public InputStream getUnicodeStream(String columnName) throws SQLException {
        return getUnicodeStream(findColumn(columnName));
    }

    public void updateAsciiStream(String columnName, InputStream x, int length)
        throws SQLException {
        updateAsciiStream(findColumn(columnName), x, length);
    }

    public void updateBinaryStream(String columnName, InputStream x, int length)
        throws SQLException {
        updateBinaryStream(findColumn(columnName), x, length);
    }

    public Reader getCharacterStream(String columnName) throws SQLException {
        return getCharacterStream(findColumn(columnName));
    }

    public void updateCharacterStream(String columnName, Reader x, int length)
        throws SQLException {
        updateCharacterStream(findColumn(columnName), x, length);
    }

    public Object getObject(String columnName) throws SQLException {
        return getObject(findColumn(columnName));
    }

    public void updateObject(String columnName, Object x) throws SQLException {
        updateObject(findColumn(columnName), x);
    }

    public void updateObject(String columnName, Object x, int scale)
        throws SQLException {
        updateObject(findColumn(columnName), x, scale);
    }

    public Object getObject(int columnIndex, Map map) throws SQLException {
        notImplemented("ResultSet.getObject(int, Map)");
        return null;
    }

    public String getString(String columnName) throws SQLException {
        return getString(findColumn(columnName));
    }

    public void updateString(String columnName, String x) throws SQLException {
        updateString(findColumn(columnName), x);
    }

    public BigDecimal getBigDecimal(String columnName) throws SQLException {
        return getBigDecimal(findColumn(columnName));
    }

    public BigDecimal getBigDecimal(String columnName, int scale)
        throws SQLException {
        return getBigDecimal(findColumn(columnName), scale);
    }

    public void updateBigDecimal(String columnName, BigDecimal x)
        throws SQLException {
        updateObject(findColumn(columnName), x);
    }

    public URL getURL(String columnName) throws SQLException {
        return getURL(findColumn(columnName));
    }

    public Array getArray(String columnName) throws SQLException {
        return getArray(findColumn(columnName));
    }

    public void updateArray(String columnName, Array x) throws SQLException {
        updateArray(findColumn(columnName), x);
    }

    public Blob getBlob(String columnName) throws SQLException {
        return getBlob(findColumn(columnName));
    }

    public void updateBlob(String columnName, Blob x) throws SQLException {
        updateBlob(findColumn(columnName), x);
    }

    public Clob getClob(String columnName) throws SQLException {
        return getClob(findColumn(columnName));
    }

    public void updateClob(String columnName, Clob x) throws SQLException {
        updateClob(findColumn(columnName), x);
    }

    public Date getDate(String columnName) throws SQLException {
        return getDate(findColumn(columnName));
    }

    public void updateDate(String columnName, Date x) throws SQLException {
        updateDate(findColumn(columnName), x);
    }

    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        java.sql.Date date = getDate(columnIndex);

        if (date != null && cal != null) {
            TimeZone timeZone = TimeZone.getDefault();
            long newTime = date.getTime();

            newTime -= timeZone.getRawOffset();
            newTime += cal.getTimeZone().getRawOffset();
            date = new java.sql.Date(newTime);
        }

        return date;
    }

    public Ref getRef(String columnName) throws SQLException {
        return getRef(findColumn(columnName));
    }

    public void updateRef(String columnName, Ref x) throws SQLException {
        updateRef(findColumn(columnName), x);
    }

    public Time getTime(String columnName) throws SQLException {
        return getTime(findColumn(columnName));
    }

    public void updateTime(String columnName, Time x) throws SQLException {
        updateTime(findColumn(columnName), x);
    }

    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        checkOpen();
        java.sql.Time time = getTime(columnIndex);

        if (time != null && cal != null) {
            TimeZone timeZone = TimeZone.getDefault();
            long newTime = time.getTime();

            newTime -= timeZone.getRawOffset();
            newTime += cal.getTimeZone().getRawOffset();
            time = new java.sql.Time(newTime);
        }

        return time;
    }

    public Timestamp getTimestamp(String columnName) throws SQLException {
        return getTimestamp(findColumn(columnName));
    }

    public void updateTimestamp(String columnName, Timestamp x)
        throws SQLException {
        updateTimestamp(findColumn(columnName), x);
    }

    public Timestamp getTimestamp(int columnIndex, Calendar cal)
        throws SQLException {
            checkOpen();
            Timestamp timestamp = getTimestamp(columnIndex);

            if (timestamp != null && cal != null) {
                TimeZone timeZone = TimeZone.getDefault();
                long newTime = timestamp.getTime();

                newTime -= timeZone.getRawOffset();
                newTime += cal.getTimeZone().getRawOffset();
                timestamp = new Timestamp(newTime);
            }

            return timestamp;
    }

    public Object getObject(String columnName, Map map) throws SQLException {
        return getObject(findColumn(columnName), map);
    }

    public Date getDate(String columnName, Calendar cal) throws SQLException {
        return getDate(findColumn(columnName), cal);
    }

    public Time getTime(String columnName, Calendar cal) throws SQLException {
        return getTime(findColumn(columnName), cal);
    }

    public Timestamp getTimestamp(String columnName, Calendar cal)
        throws SQLException {
        return getTimestamp(findColumn(columnName), cal);
    }

}
