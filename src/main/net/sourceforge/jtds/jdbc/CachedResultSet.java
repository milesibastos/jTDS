//jTDS JDBC Driver for Microsoft SQL Server and Sybase
//Copyright (C) 2004 The jTDS Project
//
//This library is free software; you can redistribute it and/or
//modify it under the terms of the GNU Lesser General Public
//License as published by the Free Software Foundation; either
//version 2.1 of the License, or (at your option) any later version.
//
//This library is distributed in the hope that it will be useful,
//but WITHOUT ANY WARRANTY; without even the implied warranty of
//MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
//Lesser General Public License for more details.
//
//You should have received a copy of the GNU Lesser General Public
//License along with this library; if not, write to the Free Software
//Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
//
package net.sourceforge.jtds.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;

/**
 * This class implements a memory cached scrollable/updateable result set.
 * <p>
 * Notes:
 * <ol>
 * <li>For maximum performance use the scroll insensitive result set type.
 * <li>As the result set is cached in memory this implementation is limited
 * to small result sets.
 * <li>Updateable or scroll sensitive result sets are limited to selects which
 * reference one table only.
 * <li>Scroll sensitive result sets must have primary keys.
 * <li>Updates are optimistic. To guard against lost updates it is recommended
 * that the table includes a timestamp column.
 * <li>This class is a plug-in replacement for the MSCursorResultSet class which
 * may be advantageous in certain applications as the scroll insensitive result
 * set implemented here is much faster than the server side cursor.
 * <li>Updateable result sets cannot be built from the output of stored procedures.
 * <li>This implementation uses 'select ... for browse' to obtain the column meta
 * data needed to generate update statements etc. For some reason temporary tables
 * do not report the keyed columns correctly and so cannot be used to build scroll
 * sensitive result sets.
 * <ol>
 *
 * @author Mike Hutchinson
 */
public class CachedResultSet extends JtdsResultSet {
    /** Array of rows comprising the result set. */
    protected ArrayList rowData;
    /** Initial size for row array. */
    protected final static int INITIAL_ROW_COUNT = 1000;
    /** Saved row data used to track changes. */
    protected ColData[] savedRow;
    /** Buffer row used for inserts. */
    protected ColData[] insertRow;
    /** Indicates currently inserting. */
    protected boolean onInsertRow;
    /** Indicates that result set is keyed. */
    protected boolean hasKeys;
    // FIXME Remember if the row was updated/deleted for each row in the ResultSet
    /** Indicates that row has been updated. */
    protected boolean rowUpdated = false;
    /** Indicates that row has been deleted. */
    protected boolean rowDeleted  = false;
    /** The row count of the initial result set. */
    protected int initialRowCnt;
    /** True if this is a local temporary result set. */
    protected boolean tempResultSet = false;

    /**
     * Construct a new cached result set.
     *
     * @param statement       The parent statement object.
     * @param sql             The SQL statement used to build the result set.
     * @param procName        An optional stored procedure name.
     * @param procedureParams Parameters for prepared statements.
     * @param resultSetType   The result set type eg scrollable.
     * @param concurrency     The result set concurrency eg updateable.
     * @exception java.sql.SQLException
     */
    CachedResultSet(JtdsStatement statement,
            String sql,
            String procName,
            ParamInfo[] procedureParams,
            int resultSetType,
            int concurrency) throws SQLException {
        super(statement, resultSetType, concurrency, null, false);

        cursorCreate(sql, procName, procedureParams);
    }

    /**
     * Construct a cached result set based on locally generated data.
     * @param statement       The parent statement object.
     * @param colName         Array of column names.
     * @param colType         Array of corresponding data types.
     * @exception java.sql.SQLException
     */
    CachedResultSet(JtdsStatement statement,
                    String[] colName, int[] colType) throws SQLException {
        super(statement, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE, null, false);
        //
        // Construct the column descriptor array
        //
        this.columns = new ColInfo[colName.length];
        for (int i = 0; i < colName.length; i++) {
            ColInfo ci = new ColInfo();
            ci.name     = colName[i];
            ci.realName = colName[i];
            ci.jdbcType = colType[i];
            switch (ci.jdbcType) {
                case java.sql.Types.VARCHAR:
                    ci.sqlType = "varchar";
                    break;
                case java.sql.Types.INTEGER:
                    ci.sqlType = "int";
                    break;
                case java.sql.Types.SMALLINT:
                    ci.sqlType = "smallint";
                    break;
            }
            columns[i] = ci;
        }
        this.columnCount   = getColumnCount(columns);
        this.rowData       = new ArrayList(INITIAL_ROW_COUNT);
        this.rowsInResult  = 0;
        this.initialRowCnt = 0;
        this.pos           = POS_BEFORE_FIRST;
        this.tempResultSet = true;
    }

    /**
     * Create a cached result set with the same columns as an existing result set.
     * @param rs The result set to copy.
     * @throws SQLException
     */
    CachedResultSet(JtdsResultSet rs) throws SQLException {
        super((JtdsStatement)rs.getStatement(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE, null, false);
        this.columns       = rs.getColumns();
        this.columnCount   = getColumnCount(columns);
        this.rowData       = new ArrayList(INITIAL_ROW_COUNT);
        this.rowsInResult  = 0;
        this.initialRowCnt = 0;
        this.pos           = POS_BEFORE_FIRST;
        this.tempResultSet = true;
    }

    /**
     * Create a cached result set containing one row.
     * @param statement The parent statement object.
     * @param columns   The column descriptor array.
     * @param data      The row data.
     * @throws SQLException
     */
    CachedResultSet(JtdsStatement statement,
            ColInfo columns[], ColData data[]) throws SQLException {
        super(statement, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, null, false);
        this.columns       = columns;
        this.columnCount   = getColumnCount(columns);
        this.rowData       = new ArrayList(1);
        this.rowsInResult  = 1;
        this.initialRowCnt = 1;
        this.pos           = POS_BEFORE_FIRST;
        this.tempResultSet = true;
        this.rowData.add(copyRow(data));
    }

    /**
     * Modify the concurrency of the result set.
     * <p>Use to make result set read only once loaded.
     * @param concurrency The concurrency value eg ResultSet.CONCUR_READ_ONLY.
     */
    void setConcurrency(int concurrency)
    {
        this.concurrency = concurrency;
    }

    /**
     * Create a new scrollable result set in memory.
     *
     * @param sql        The SQL SELECT statement.
     * @param procName   Optional procedure name for cursors based on a stored procedure.
     * @param parameters Optional stored procedure parameters.
     * @exception java.sql.SQLException
     */
    private void cursorCreate(String sql,
                              String procName,
                              ParamInfo[] parameters)
            throws SQLException {
        TdsCore tds = statement.getTds();

        if (concurrency == ResultSet.CONCUR_UPDATABLE || resultSetType == ResultSet.TYPE_SCROLL_SENSITIVE) {
            // User is requesting an updateable or sensitive result set
            // We need to see if the SQL statement is a select
            // that references only one underlying table.
            if (procName == null || procName.startsWith("#jtds") || TdsCore.isPreparedProcedureName(procName)) {
                // OK Should have an SQL select statement
                // append " FOR BROWSE" to obtain table names
                // NB. We can't use any jTDS temporary stored proc
                try {
                    tds.executeSQL(sql + " FOR BROWSE", null, parameters, false,
                            		statement.getQueryTimeout(),
                            		statement.getMaxRows());
                    while (!tds.getMoreResults() && !tds.isEndOfResponse());

                    if (tds.isResultSet()) {
                        columns = tds.getColumns();
                        // Check base table names
                        String tableName = null;
                        for (int i = 0; i < columns.length; i++) {
                            if (columns[i].tableName != null) {
                                if (tableName != null && !tableName.equals(columns[i].tableName)) {
                                // Too many table names
                                    columns = null;
                                    break;
                                }
                                tableName = columns[i].tableName;
                            }
                            if (columns[i].isKey) {
                                hasKeys = true;
                            }
                         }
                        if (tableName == null) {
                            // No table in select?
                            columns = null;
                        }
                    }
                } catch (SQLException e) {
                    if (statement.getConnection().isClosed()) {
                        // Serious error rethrow
                        throw e;
                    }
                }
            } // else a stored procedure so we can't discover table names
        }
        if (columns != null && resultSetType == ResultSet.TYPE_SCROLL_SENSITIVE && !isKeyed()) {
            //
            // We have downgraded to scroll insensitive tell user
            //
            resultSetType = ResultSet.TYPE_SCROLL_INSENSITIVE;
            statement.addWarning(new SQLWarning(
                    Messages.get("warning.cursordowngraded", "TYPE_SCROLL_INSENSITIVE"), "01000"));
        }
        //
        // If we get here and columns is still null we need to execute normally
        //
        if (columns == null) {
            tds.executeSQL(sql, procName, parameters, false,
                    	   statement.getQueryTimeout(),
        		           statement.getMaxRows());
            while (!tds.getMoreResults() && !tds.isEndOfResponse());

            if (!tds.isResultSet()) {
                throw new SQLException(Messages.get("error.statement.noresult"), "24000");
            }
            columns = tds.getColumns();
            if (concurrency == ResultSet.CONCUR_UPDATABLE ) {
                //
                // We have downgraded to read only tell user
                //
                statement.addWarning(new SQLWarning(
                        Messages.get("warning.cursordowngraded", "CONCUR_READ_ONLY"), "01000"));
                concurrency = ResultSet.CONCUR_READ_ONLY;
            }
        }
        columnCount = getColumnCount(columns);
        rowData = new ArrayList(INITIAL_ROW_COUNT);
        //
        // Load result set into buffer
        //
        while (super.next()) {
            rowData.add(copyRow(currentRow));
        }
        rowsInResult  = rowData.size();
        initialRowCnt = rowsInResult;
        pos = POS_BEFORE_FIRST;
    }

    /**
     * Fetch the next result row from the internal row array.
     *
     * @param rowNum The row number to fetch.
     * @return <code>boolean</code> true if a result set row is returned.
     * @throws java.sql.SQLException
     */
    private boolean cursorFetch(int rowNum)
            throws SQLException {
        savedRow = null;
        rowUpdated = false;
        if (rowsInResult == 0) {
            pos = POS_BEFORE_FIRST;
            currentRow = null;
            return false;
        }
        if (rowNum == pos) {
            // On current row
            if (resultSetType == ResultSet.TYPE_SCROLL_SENSITIVE) {
                refreshRow();
            }
            return true;
        }
        if (rowNum < 1) {
            currentRow = null;
            pos = POS_BEFORE_FIRST;
            return false;
        }
        if (rowNum > rowsInResult) {
            currentRow = null;
            pos = POS_AFTER_LAST;
            return false;
        }
        pos = rowNum;
        currentRow = (ColData[])rowData.get(rowNum-1);
        rowDeleted = currentRow == null;
        if (resultSetType == ResultSet.TYPE_SCROLL_SENSITIVE && currentRow != null) {
            refreshRow();
        }
        return true;
    }

    /**
     * Close the result set.
     */
    private void cursorClose() {
        rowData = null;
    }

    /**
     * Retrieve the keyed status of the result set.
     *
     * @return True if result set has one or more keys.
     */
    protected boolean isKeyed() {
        return hasKeys;
    }

    /**
     * Create a cloned copy of the specifed ROW.
     *
     * @param row The row to copy.
     */
    private void saveRow(ColData[] row) {
        savedRow = new ColData[row.length];
        for (int i = 0; i < row.length; i++) {
            savedRow[i] = new ColData(row[i]);
        }
    }

    /**
     * Create a parameter object for an update, delete or insert statement.
     *
     * @param pos  The substitution position of the parameter marker in the SQL.
     * @param info The ColInfo column descriptor.
     * @param col  The column data item.
     * @return The new parameter as a <code>ParamInfo</code> object.
     */
    protected ParamInfo buildParameter(int pos, ColInfo info, ColData col)
            throws SQLException {

        Object value  = col.getValue();
        int    length = col.getLength();
        if (value instanceof BlobImpl) {
            BlobImpl blob = (BlobImpl)value;
            value   = blob.getBinaryStream();
            length  = (int)blob.length();
        } else
        if (value instanceof ClobImpl) {
            ClobImpl clob = (ClobImpl)value;
            value   = clob.getCharacterStream();
            length  = (int)clob.length();
        }
        ParamInfo param = new ParamInfo(info, null, value, length);
        param.isUnicode = info.sqlType.equals("nvarchar") ||
                          info.sqlType.equals("nchar") ||
                          info.sqlType.equals("ntext");
        param.markerPos = pos;

        return param;
    }

    /**
     * Set the specified column's data value.
     *
     * @param colIndex The index of the column in the row.
     * @param value The new column value.
     */
    protected void setColValue(int colIndex, Object value, int length)
            throws SQLException {

        if (onInsertRow && insertRow == null || !onInsertRow && currentRow == null) {
            throw new SQLException(Messages.get("error.resultset.norow"), "24000");
        }

        ColData col;
        if (onInsertRow) {
            col = insertRow[colIndex - 1];
        } else {
            if (savedRow == null) {
                saveRow(currentRow);
            }
            col = currentRow[colIndex - 1];
        }
        col.setValue(value);
        col.setLength(length);
    }

//
//  -------------------- java.sql.ResultSet methods -------------------
//

     public void afterLast() throws SQLException {
         checkOpen();
         checkScrollable();
         if (pos != POS_AFTER_LAST) {
             cursorFetch(rowsInResult+1);
         }
     }

     public void beforeFirst() throws SQLException {
         checkOpen();
         checkScrollable();

         if (pos != POS_BEFORE_FIRST) {
             cursorFetch(0);
         }
     }

     public void cancelRowUpdates() throws SQLException {
         checkOpen();
         checkUpdateable();
         if (onInsertRow) {
             throw new SQLException(Messages.get("error.resultset.insrow"), "24000");
         }
         if (savedRow != null) {
             rowUpdated = false;
             for (int i = 0; i < savedRow.length; i++) {
                 currentRow[i] = new ColData(savedRow[i]);
             }
         }
     }

     public void close() throws SQLException {
         if (!closed) {
             try {
                 cursorClose();
             } finally {
                 closed    = true;
                 statement = null;
             }
         }
     }

     public void deleteRow() throws SQLException {
         checkOpen();
         checkUpdateable();

         if (currentRow == null) {
             throw new SQLException(Messages.get("error.resultset.norow"), "24000");
         }

         if (onInsertRow) {
             throw new SQLException(Messages.get("error.resultset.insrow"), "24000");
         }
         //
         // Construct an SQL DELETE statement
         //
         StringBuffer sql = new StringBuffer(128);
         String dbName    = columns[0].catalog;
         String userName  = columns[0].schema;
         String tableName = columns[0].tableName;
         ArrayList params = new ArrayList();
         sql.append("DELETE FROM ");
         if (dbName != null) {
             sql.append(dbName);
             sql.append('.');
             if (userName == null) {
                 sql.append('.');
             }
         }
         if (userName != null) {
             sql.append(userName);
             sql.append('.');
         }
         sql.append(tableName);
         //
         // Create the WHERE clause
         //
         sql.append(" WHERE ");
         int count = 0;
         for (int i = 0; i < columnCount; i++) {
             if (currentRow[i].getValue() == null) {
                 if (count > 0) {
                     sql.append(" AND ");
                 }
                 sql.append(columns[i].realName);
                 sql.append(" IS NULL");
             } else {
                 // Only include searchable columns in where clause
                 if (!columns[i].sqlType.equals("text")
                     && !columns[i].sqlType.equals("ntext")
                     && !columns[i].sqlType.equals("image")) {
                     if (count > 0) {
                         sql.append(" AND ");
                     }
                     sql.append(columns[i].realName);
                     sql.append("=?");
                     count++;
                     params.add(buildParameter(sql.length()-1, columns[i], currentRow[i]));
                 }
             }
         }
         ParamInfo parameters[] = (ParamInfo[]) params.toArray(new ParamInfo[params.size()]);
         //
         // Execute the delete statement
         //
         TdsCore tds = statement.getTds();
         tds.executeSQL(sql.toString(), null, parameters, false, 0, statement.getMaxRows());
         int updateCount = 0;
         while (!tds.isEndOfResponse()) {
             if (!tds.getMoreResults()) {
                 if (tds.isUpdateCount()) {
                     updateCount = tds.getUpdateCount();
                 }
             }
         }
         tds.clearResponseQueue();
         statement.getMessages().checkErrors();
         if (updateCount > 0) {
             // Leave a 'hole' in the result set array.
             rowDeleted = true;
             currentRow = null;
             rowData.set(pos-1, null);
         } else {
             // No delete. Possibly row was changed on database by another user?
             throw new SQLException(Messages.get("error.resultset.deletefail"), "24000");
         }
     }

     public void insertRow() throws SQLException {
         checkOpen();
         //
         // If this is a local temporary result set just insert row into buffer
         //
         if (tempResultSet) {
             rowData.add(insertRow);
             rowsInResult++;
             initialRowCnt++;
             insertRow = newRow();
             return;
         }

         checkUpdateable();

         if (!onInsertRow) {
             throw new SQLException(Messages.get("error.resultset.notinsrow"), "24000");
         }
         //
         // Construct an SQL INSERT statement
         //
         StringBuffer sql = new StringBuffer(128);
         String dbName    = columns[0].catalog;
         String userName  = columns[0].schema;
         String tableName = columns[0].tableName;
         ArrayList params = new ArrayList();
         sql.append("INSERT INTO ");
         if (dbName != null) {
             sql.append(dbName);
             sql.append('.');
             if (userName == null) {
                 sql.append('.');
             }
         }
         if (userName != null) {
             sql.append(userName);
             sql.append('.');
         }
         sql.append(tableName);
         //
         // Create column list
         //
         sql.append(" (");
         int count = 0;
         for (int i = 0; i < columnCount; i++) {
             if (insertRow[i].isUpdated()) {
                 if (count > 0) {
                     sql.append(", ");
                 }
                 sql.append(columns[i].realName);
                 count++;
             }
         }
         //
         // Create new values list
         //
         sql.append(") VALUES(");
         count = 0;
         for (int i = 0; i < columnCount; i++) {
             if (insertRow[i].isUpdated()) {
                 if (count > 0) {
                     sql.append(", ");
                 }
                 sql.append("?");
                 params.add(buildParameter(sql.length()-1, columns[i], insertRow[i]));
                 count++;
             }
         }
         sql.append(')');
         ParamInfo parameters[] = (ParamInfo[]) params.toArray(new ParamInfo[params.size()]);
         //
         // execute the insert statement
         //
         TdsCore tds = statement.getTds();
         tds.executeSQL(sql.toString(), null, parameters, false, 0, statement.getMaxRows());
         int updateCount = 0;
         while (!tds.isEndOfResponse()) {
             if (!tds.getMoreResults()) {
                 if (tds.isUpdateCount()) {
                     updateCount = tds.getUpdateCount();
                 }
             }
         }
         tds.clearResponseQueue();
         statement.getMessages().checkErrors();
         if (updateCount > 0) {
             rowData.add(insertRow);
             rowsInResult++;
         } else {
             // No Insert. Probably will not get here as duplicate key etc
             // will have already been reported as an exception.
             throw new SQLException(Messages.get("error.resultset.insertfail"), "24000");
         }
         insertRow = newRow();
     }

     public void moveToCurrentRow() throws SQLException {
         checkOpen();
         checkUpdateable();
         insertRow = null;
         onInsertRow = false;
     }


     public void moveToInsertRow() throws SQLException {
         checkOpen();
         checkUpdateable();
         insertRow   = newRow();
         onInsertRow = true;
     }

     public void refreshRow() throws SQLException {
         checkOpen();

         if (onInsertRow) {
             throw new SQLException(Messages.get("error.resultset.insrow"), "24000");
         }
         //
         // If row is being updated discard updates now
         //
         if (savedRow != null) {
             rowUpdated = false;
             for (int i = 0; i < savedRow.length; i++) {
                 currentRow[i] = new ColData(savedRow[i]);
             }
         }
         //
         // If result set is keyed we can refresh the row data from the
         // database using the key.
         // NB. #Temporary tables with keys are not identified correctly
         // in the column meta data sent after 'for browse'. This means that
         // temporary tables can not be used with this logic.
         //
         if (isKeyed() && currentRow != null){
             //
             // Construct a SELECT statement
             //
             StringBuffer sql = new StringBuffer();
             sql.append("SELECT ");
             for (int i = 0; i < columns.length; i++) {
                 if (i > 0) {
                    sql.append(',');
                 }
                 sql.append(columns[i].realName);
             }
             sql.append(" FROM ");
             if (columns[0].catalog != null && columns[0].catalog.length() > 0) {
                 sql.append(columns[0].catalog).append('.');
                 if (columns[0].schema == null || columns[0].schema.length() == 0) {
                     sql.append("..");
                 }
             }
             if (columns[0].schema != null && columns[0].schema.length() > 0) {
                 sql.append(columns[0].schema).append('.');
             }
             sql.append(columns[0].tableName);
             //
             // Construct a where clause using keyed columns only
             //
             sql.append(" WHERE ");
             ArrayList params = new ArrayList();
             int count = 0;
             for (int i = 0; i < columns.length; i++) {
                 if (columns[i].isKey) {
                     if (currentRow[i].getValue() == null) {
                         if (count > 0) {
                             sql.append(" AND ");
                         }
                         sql.append(columns[i].realName);
                         sql.append(" IS NULL");
                     } else {
                         if (count > 0) {
                             sql.append(" AND ");
                         }
                         sql.append(columns[i].realName);
                         sql.append("=?");
                         params.add(buildParameter(sql.length()-1, columns[i], currentRow[i]));
                     }
                     count++;
                 }
             }
             ParamInfo parameters[] = (ParamInfo[]) params.toArray(new ParamInfo[params.size()]);
             //
             // Execute the select
             //
             TdsCore tds = statement.getTds();
             tds.executeSQL(sql.toString(), null, parameters, false, 0, statement.getMaxRows());
             if (!tds.isEndOfResponse()) {
                 if (tds.getMoreResults() && tds.getNextRow()) {
                     // refresh the row data
                     ColData col[] = tds.getRowData();
                     for (int i = 0; i < col.length; i++) {
                         currentRow[i] = col[i];
                     }
                 } else {
                     currentRow = null;
                 }
             } else {
                 currentRow = null;
             }
             tds.clearResponseQueue();
             statement.getMessages().checkErrors();
             if (currentRow == null) {
                 rowData.set(pos-1, null);
                 rowDeleted = true;
             }
         }
     }

     public void updateRow() throws SQLException {
         checkOpen();
         checkUpdateable();

         if (currentRow == null) {
             throw new SQLException(Messages.get("error.resultset.norow"), "24000");
         }

         if (onInsertRow) {
             throw new SQLException(Messages.get("error.resultset.insrow"), "24000");
         }

         if (savedRow == null) {
             // Nothing to update
             return;
         }
         //
         // Construct an SQL UPDATE statement
         //
         StringBuffer sql = new StringBuffer(128);
         String dbName    = columns[0].catalog;
         String userName  = columns[0].schema;
         String tableName = columns[0].tableName;
         ArrayList params = new ArrayList();

         sql.append("UPDATE ");
         if (dbName != null) {
             sql.append(dbName);
             sql.append('.');
             if (userName == null) {
                 sql.append('.');
             }
         }
         if (userName != null) {
             sql.append(userName);
             sql.append('.');
         }
         sql.append(tableName);
         //
         // OK now create assign new values
         //
         sql.append(" SET ");
         int count = 0;
         for (int i = 0; i < columnCount; i++) {
             if (currentRow[i].isUpdated()) {
                 if (count > 0) {
                     sql.append(", ");
                 }
                 sql.append(columns[i].realName);
                 sql.append("=?");
                 params.add(buildParameter(sql.length()-1, columns[i], currentRow[i]));
                 count++;
             }
         }
         //
         // Now construct where clause
         //
         sql.append(" WHERE ");
         count = 0;
         for (int i = 0; i < columnCount; i++) {
             if (savedRow[i].getValue() == null) {
                 if (count > 0) {
                     sql.append(" AND ");
                 }
                 sql.append(columns[i].realName);
                 sql.append(" IS NULL");
             } else {
                 // Only include 'searchable' columns in where clause
                 if (!columns[i].sqlType.equals("text")
                     && !columns[i].sqlType.equals("ntext")
                     && !columns[i].sqlType.equals("image")) {
                     if (count > 0) {
                         sql.append(" AND ");
                     }
                     sql.append(columns[i].realName);
                     sql.append("=?");
                     count++;
                     params.add(buildParameter(sql.length()-1, columns[i], savedRow[i]));
                 }
             }
         }
         ParamInfo parameters[] = (ParamInfo[]) params.toArray(new ParamInfo[params.size()]);
         //
         // Now execute update
         //
         TdsCore tds = statement.getTds();
         tds.executeSQL(sql.toString(), null, parameters, false, 0, statement.getMaxRows());
         int updateCount = 0;
         while (!tds.isEndOfResponse()) {
             if (!tds.getMoreResults()) {
                 if (tds.isUpdateCount()) {
                     updateCount = tds.getUpdateCount();
                 }
             }
         }
         tds.clearResponseQueue();
         statement.getMessages().checkErrors();
         rowUpdated = updateCount > 0;
         savedRow = null;
         if (updateCount == 0) {
             // No update. Possibly row was changed on database by another user?
             throw new SQLException(Messages.get("error.resultset.updatefail"), "24000");
         }
     }

     public boolean first() throws SQLException {
         checkOpen();
         checkScrollable();
         return cursorFetch(1);
     }

     public boolean isLast() throws SQLException {
         checkOpen();

         return(pos == rowsInResult) && (rowsInResult != 0);
     }

     public boolean last() throws SQLException {
         checkOpen();
         checkScrollable();
         return cursorFetch(rowsInResult);
     }

     public boolean next() throws SQLException {
         checkOpen();
         if (pos != POS_AFTER_LAST) {
             return cursorFetch(pos+1);
         } else {
             return false;
         }
     }

     public boolean previous() throws SQLException {
         checkOpen();
         checkScrollable();
         if (pos == POS_AFTER_LAST) {
             pos = rowsInResult+1;
         }
         return cursorFetch(pos-1);
     }

     public boolean rowDeleted() throws SQLException {
         checkOpen();

         return rowDeleted;
     }

     public boolean rowInserted() throws SQLException {
         checkOpen();

         return pos > initialRowCnt;
     }

     public boolean rowUpdated() throws SQLException {
         checkOpen();

         return rowUpdated;
     }

     public boolean absolute(int row) throws SQLException {
         checkOpen();
         checkScrollable();
         if (row < 1) {
             row = (rowsInResult + 1) + row;
         }

         return cursorFetch(row);
     }

     public boolean relative(int row) throws SQLException {
         if (pos == POS_AFTER_LAST) {
             return absolute((rowsInResult+1)+row);
         } else {
             return absolute(pos+row);
         }
     }
}
