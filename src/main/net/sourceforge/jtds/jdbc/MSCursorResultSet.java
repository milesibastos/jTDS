// jTDS JDBC Driver for Microsoft SQL Server and Sybase
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

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Types;

/**
 * This class extends the JtdsResultSet to support scrollable and or
 * updateable cursors on Microsoft servers.
 * <p>The undocumented Microsoft sp_cursor procedures are used.
 * <p>
 * Implementation notes:
 * <ol>
 * <li>All of Alin's cursor result set logic is incorporated here.
 * <li>This logic was originally implemented in the JtdsResultSet class but on reflection
 * it seems that Alin's original approch of having a dedicated cursor class leads to a more
 * flexible and maintainable design.
 * </ol>
 *
 * @author Alin Sinpalean
 * @author Mike Hutchinson
 * @version $Id: MSCursorResultSet.java,v 1.19 2004-09-23 14:26:59 alin_sinpalean Exp $
 */
public class MSCursorResultSet extends JtdsResultSet {
    /*
     * Constants
     */
    private static final int FETCH_FIRST = 1;
    private static final int FETCH_NEXT = 2;
    private static final int FETCH_PREVIOUS = 4;
    private static final int FETCH_LAST = 8;
    private static final int FETCH_ABSOLUTE = 16;
    private static final int FETCH_RELATIVE = 32;
    private static final int FETCH_REPEAT = 128;
    private static final int FETCH_INFO = 256;

    private static final int CURSOR_TYPE_KEYSET = 1;
    private static final int CURSOR_TYPE_DYNAMIC = 2;
    private static final int CURSOR_TYPE_FORWARD = 4;
    private static final int CURSOR_TYPE_STATIC = 8;

    private static final int CURSOR_CONCUR_READ_ONLY = 1;
    private static final int CURSOR_CONCUR_SCROLL_LOCKS = 2;
    private static final int CURSOR_CONCUR_OPTIMISTIC = 4;

    private static final int CURSOR_OP_INSERT = 4;
    private static final int CURSOR_OP_UPDATE = 33;
    private static final int CURSOR_OP_DELETE = 34;

    /**
     * The row is unchanged.
     */
    private static final int SQL_ROW_SUCCESS = 1;

    /**
     * The row has been deleted.
     */
    private static final int SQL_ROW_DELETED = 2;

    // @todo Check the values for the constants below (above are ok).
    /**
     * The row has been updated.
     */
    private static final int SQL_ROW_UPDATED = 3;

    /**
     * There is no row that corresponds this position.
     */
    private static final int SQL_ROW_NOROW = 4;

    /**
     * The row has been added.
     */
    private static final int SQL_ROW_ADDED = 5;

    /**
     * The row is unretrievable due to an error.
     */
    private static final int SQL_ROW_ERROR = 6;

    private static final int POS_BEFORE_FIRST = 0;
    private static final int POS_AFTER_LAST = -1;

    /*
     * Instance variables.
     */
    /** The cursor handle from sp_cusoropen. */
    private Integer cursorHandle;
    /** The prepared statment handle from prepare/prepexec. */
    private Integer prepStmtHandle;
    /** Stores return values from stored procedure calls. */
    private Integer retVal;
    /** Set when <code>moveToInsertRow()</code> was called. */
    private boolean onInsertRow = false;
    /** The "insert row". */
    private ColData[] insertRow = null;

    /**
     * Construct a cursor result set using Microsoft sp_cursorcreate etc.
     *
     * @param statement The parent statement object or null.
     * @param resultSetType one of FORWARD_ONLY, SCROLL_INSENSITIVE, SCROLL_SENSITIVE.
     * @param concurrency One of CONCUR_READ_ONLY, CONCUR_UPDATE.
     * @throws SQLException
     */
    MSCursorResultSet(JtdsStatement statement,
                      String sql,
                      String procName,
                      ParamInfo[] procedureParams,
                      int resultSetType,
                      int concurrency)
    throws SQLException {
        super(statement, resultSetType, concurrency, null, false);

        cursorCreate(sql, procName, procedureParams);
    }

    /**
     * Get the specified column's data item.
     *
     * @param index The column index in the row.
     * @return The column value as a <code>ColData</code> object.
     * @throws SQLException
     */
    protected ColData getColumn(int index) throws SQLException {
        if (index < 1 || index > columnCount) {
            throw new SQLException(Messages.get("error.resultset.colindex",
                    Integer.toString(index)),
                    "07009");
        }

        if (onInsertRow && insertRow == null || !onInsertRow && currentRow == null) {
            throw new SQLException(Messages.get("error.resultset.norow"), "24000");
        }

        ColData data = (onInsertRow) ? insertRow[index - 1] : currentRow[index - 1];
        wasNull = data.isNull();

        return data;
    }

    /**
     * Create a new Cursor result set using the internal sp_cursoropen procedure.
     *
     * @param sql The SQL SELECT statement.
     * @param procName Optional procedure name for cursors based on a stored procedure.
     * @param parameters Optional stored procedure parameters.
     * @throws SQLException
     */
    private void cursorCreate(String sql,
                              String procName,
                              ParamInfo[] parameters)
    throws SQLException {
        TdsCore tds = statement.getTds();
        int prepareSql = statement.connection.getPrepareSql();
        int scrollOpt;
        int ccOpt;

        //
        // Select the correct type of Server side cursor to
        // match the scroll and concurrency options.
        //
        switch (resultSetType) {
            case TYPE_SCROLL_INSENSITIVE:
                scrollOpt = CURSOR_TYPE_STATIC;
                break;

            case TYPE_SCROLL_SENSITIVE:
                scrollOpt = CURSOR_TYPE_KEYSET;
                break;

            case TYPE_FORWARD_ONLY:
            default:
                scrollOpt = CURSOR_TYPE_FORWARD;
                break;
        }

        switch (concurrency) {
            case CONCUR_READ_ONLY:
            default:
                ccOpt = CURSOR_CONCUR_READ_ONLY;
                break;

            case CONCUR_UPDATABLE:
                ccOpt = CURSOR_CONCUR_SCROLL_LOCKS;
                break;
        }

        //
        // Simplify future tests for parameters
        //
        if (parameters != null && parameters.length == 0) {
            parameters = null;
        }
        //
        // If there are no parameters we will opt for
        // the sp_cursoropen option only.
        //
        if (parameters == null) {
            prepareSql = TdsCore.UNPREPARED;
        }
        //
        // SQL 6.5 does not support stored procs (with params) in the sp_cursor call
        // will need to substitute any parameter values into the SQL.
        //
        if (tds.getTdsVersion() == Driver.TDS42) {
            prepareSql = TdsCore.UNPREPARED;
            if (parameters != null) {
                procName = null;
            }
        }
        //
        // If we are running in unprepare mode and there are parameters
        // substitute these into the SQL statement now.
        //
        if (parameters != null && prepareSql == TdsCore.UNPREPARED) {
            sql = Support.substituteParameters(sql, parameters, tds.getTdsVersion());
            parameters = null;
        }
        //
        // For most prepare modes we need to substitute parameter
        // names for the ? markers.
        //
        if (parameters != null) {
            if (procName == null || !procName.startsWith("#jtds")) {
                sql = Support.substituteParamMarkers(sql, parameters);
            }
        }
        //
        // There are generally three situations in which procName is not null:
        // 1. Running in prepareSQL=1 and contains a temp proc name e.g. #jtds00001
        //    in which case we need to generate an SQL statement exec #jtds...
        // 2. Running in prepareSQL=4 and contains an existing statement handle.
        // 3. CallableStatement in which case the SQL string has a valid exec
        //    statement and we can ignore procName.
        //
        if (procName != null) {
            if (procName.startsWith("#jtds")) {
                StringBuffer buf = new StringBuffer(64);
                buf.append("EXEC ").append(procName).append(' ');
                for (int i = 0; parameters != null && i < parameters.length; i++) {
                    if (i != 0) {
                        buf.append(',');
                    }
                    if (parameters[i].name != null) {
                        buf.append(parameters[i].name);
                    } else {
                        buf.append("@P").append(i);
                    }
                }
                sql = buf.toString();
            } else
            // Prepared Statement Handle?
            if (TdsCore.isPreparedProcedureName(procName)) {
                //
                // At present procName is set to the value obtained by
                // the connection.prepareSQL() call in JtdsPreparedStatement.
                // Ideally we need to find a way eliminate the redundant prepare
                // for PreparedStatements that are going to open cursors.
                // This handle was obtained using sp_prepare not sp_cursorprepare
                // so I am going to ignore it for now until Brian has finished the
                // prepared statement logic.
                //
//                try {
//                    prepStmtHandle = new Integer(procName);
//                } catch (NumberFormatException e) {
//                    throw new IllegalStateException(
//                               "Invalid prepared statement handle: " +
//                                      procName);
//                }
            }
//            procName = null;
        }
        //
        // If using sp_cursoropen need to set a flag on scrollOpt.
        // The 0x1000 tells the server that there is a parameter
        // definition and user parameters present. If this flag is
        // not set the driver will ignore the additional parameters.
        //
        if (parameters != null) {
            scrollOpt |= 0x1000;
        }
        //
        // Create parameter objects
        //
        // Setup scroll options parameter
        //
        ParamInfo pScrollOpt  = new ParamInfo();
        pScrollOpt.isSet      = true;
        pScrollOpt.jdbcType   = Types.INTEGER;
        pScrollOpt.value      = new Integer(scrollOpt);
        pScrollOpt.isOutput   = true;
        //
        // Setup concurrency options parameter
        //
        ParamInfo pConCurOpt  = new ParamInfo();
        pConCurOpt.isSet      = true;
        pConCurOpt.jdbcType   = Types.INTEGER;
        pConCurOpt.value      = new Integer(ccOpt);
        pConCurOpt.isOutput   = true;
        //
        // Setup number of rows parameter
        //
        ParamInfo pRowCount   = new ParamInfo();
        pRowCount.isSet       = true;
        pRowCount.jdbcType    = Types.INTEGER;
        pRowCount.isOutput    = true;
        //
        // Setup cursor handle parameter
        //
        ParamInfo pCursor     = new ParamInfo();
        pCursor.isSet         = true;
        pCursor.jdbcType      = Types.INTEGER;
        pCursor.isOutput      = true;
        //
        // Setup statement handle param
        //
        ParamInfo pStmtHand = null;
        if (prepareSql == TdsCore.PREPEXEC || prepareSql == TdsCore.PREPARE) {
            pStmtHand          = new ParamInfo();
            pStmtHand.isSet    = true;
            pStmtHand.jdbcType = Types.INTEGER;
            pStmtHand.value    = prepStmtHandle;
            pStmtHand.isOutput = true;
        }
        //
        // Setup parameter definitions parameter
        //
        ParamInfo pParamDef = null;
        if (parameters != null ) {
            // Parameter declarations
            for (int i = 0; i < parameters.length; i++) {
                TdsData.getNativeType(statement.connection, parameters[i]);
            }

            pParamDef           = new ParamInfo();
            pParamDef.isSet     = true;
            pParamDef.jdbcType  = Types.LONGVARCHAR;
            pParamDef.value     = Support.getParameterDefinitions(parameters);
            pParamDef.length    = ((String)pParamDef.value).length();
            pParamDef.isUnicode = true;
        }
        //
        // Setup SQL statement parameter
        //
        ParamInfo pSQL = new ParamInfo();
        pSQL.isSet     = true;
        pSQL.jdbcType  = Types.LONGVARCHAR;
        pSQL.value     = sql;
        pSQL.length    = sql.length();
        pSQL.isUnicode = true;
        //
        // If using the prepare/execute model, now need to pepare SQL
        //
        if (prepareSql == TdsCore.PREPARE && prepStmtHandle == null) {
            ParamInfo params[] = new ParamInfo[6];

            // Setup statement handle param
            params[0] = pStmtHand;

            // Setup parameter definitions
            params[1] = pParamDef;

            // Setup statement param
            params[2] = pSQL;

            // Setup flag param
            params[3] = new ParamInfo();
            params[3].isSet = true;
            params[3].jdbcType = Types.INTEGER;
            params[3].value = new Integer(1);
            // Setup scroll options
            params[4] = pScrollOpt;
            // Setup concurrency options
            params[5] = pConCurOpt;

            columns = null; // Will be populated if preparing a select

            // Use sp_cursorprepare approach
            tds.executeSQL(null, "sp_cursorprepare", params, false, statement.getQueryTimeout(), -1);
            tds.clearResponseQueue();

            // columns will now hold meta data for select statements
            prepStmtHandle = (Integer) pStmtHand.value;

            if (prepStmtHandle == null) {
                // Some types of statements cannot be prepared
                prepareSql = TdsCore.UNPREPARED;
                if (parameters != null) {
                    // Ensure that sp_cursoropen will recognise user params
                    pScrollOpt.value = new Integer(((Integer)pScrollOpt.value).intValue() | 0x1000);
                }
            }
        }
        //
        // OK now open the Cursor
        //
        if (prepareSql == TdsCore.PREPARE && prepStmtHandle != null) {
            // Use sp_cursorexecute approach
            ParamInfo[] params = new ParamInfo[5 + parameters.length];
            System.arraycopy(parameters, 0, params, 5, parameters.length);
            // Setup statement handle param
            pStmtHand.isOutput = false;
            params[0] = pStmtHand;
            // Setup cursor handle param
            params[1] = pCursor;
            // Setup scroll options (mask off parameter flag)
            pScrollOpt.value = new Integer(((Integer)pScrollOpt.value).intValue() & 0XFF);
            params[2] = pScrollOpt;
            // Setup concurrency options
            params[3] = pConCurOpt;
            // Setup numRows parameter
            params[4] = pRowCount;
            parameters = params;
            procName = "sp_cursorexecute";
        } else if (prepareSql == TdsCore.PREPEXEC) {
            // Use sp_cursorprepexec approach
            ParamInfo[] params = new ParamInfo[7 + parameters.length];
            System.arraycopy(parameters, 0, params, 7, parameters.length);

            // Setup statement handle param
            params[0] = pStmtHand;

            // Setup cursor handle param
            params[1] = pCursor;

            // Setup parameter definitions
            params[2] = pParamDef;

            // Setup statement param
            params[3] = pSQL;

            // Setup scroll options
            params[4] = pScrollOpt;

            // Setup concurrency options
            params[5] = pConCurOpt;

            // Setup numRows parameter
            params[6] = pRowCount;

            parameters = params;

            procName = "sp_cursorprepexec";
        } else {
            // Use sp_cursoropen approach
            ParamInfo[] params;

            if (parameters == null) {
                params = new ParamInfo[5];
            } else {
                params = new ParamInfo[6 + parameters.length];
                System.arraycopy(parameters, 0, params, 6, parameters.length);
                params[5] = pParamDef;
            }
            // Setup cursor handle param
            params[0] = pCursor;

            // Setup statement param
            params[1] = pSQL;

            // Setup scroll options
            params[2] = pScrollOpt;

            // Setup concurrency options
            params[3] = pConCurOpt;

            // Setup numRows parameter
            params[4] = pRowCount;

            parameters = params;

            procName = "sp_cursoropen";
        }

        retVal = null;

        tds.executeSQL(null, procName, parameters, false, statement.getQueryTimeout(), 0);

        while (!tds.getMoreResults() && !tds.isEndOfResponse());

        if (tds.isResultSet()) {
            this.columns = copyInfo(tds.getColumns());
            this.columnCount = getColumnCount(columns);
        } else {
            statement.getMessages().addException(new SQLException(
                    Messages.get("error.statement.noresult"), "24000"));
        }

        tds.clearResponseQueue();
        statement.messages.checkErrors();
        retVal = tds.getReturnStatus();

        int actualScroll;
        int actualCc;

        //
        // Retrieve values of output parameters
        //
        cursorHandle = (Integer) pCursor.value;
        actualScroll = ((Integer)pScrollOpt.value).intValue();
        actualCc     = ((Integer)pConCurOpt.value).intValue();
        rowsInResult = ((Integer)pRowCount.value).intValue();
        if (prepareSql == TdsCore.PREPEXEC) {
            prepStmtHandle = (Integer)pStmtHand.value;
        }
        //
        // Check for downgrade of scroll or concurrency options
        //
        if ((actualScroll != (scrollOpt & 0xFFF)) || (actualCc != ccOpt)) {
            if (actualScroll != scrollOpt) {
                switch (actualScroll) {
                    case CURSOR_TYPE_FORWARD:
                        this.resultSetType = TYPE_FORWARD_ONLY;
                        break;

                    case CURSOR_TYPE_STATIC:
                        this.resultSetType = TYPE_SCROLL_INSENSITIVE;
                        break;

                    case CURSOR_TYPE_DYNAMIC:
                    case CURSOR_TYPE_KEYSET:
                        this.resultSetType = TYPE_SCROLL_SENSITIVE;
                        break;

                    default:
                        statement.getMessages().addWarning(new SQLWarning(
                                Messages.get("warning.cursortype", Integer.toString(actualScroll)),
                                "01000"));
                }
            }

            if (actualCc != ccOpt) {
                switch (actualCc) {
                    case CURSOR_CONCUR_READ_ONLY:
                        concurrency = CONCUR_READ_ONLY;
                        break;

                    case CURSOR_CONCUR_SCROLL_LOCKS:
                    case CURSOR_CONCUR_OPTIMISTIC:
                        concurrency = CONCUR_UPDATABLE;
                        break;

                    default:
                        statement.getMessages().addWarning(new SQLWarning(
                                Messages.get("warning.concurrtype", Integer.toString(actualCc)),
                                "01000"));
                }
            }

            // SAfe This warning goes to the Statement, not the ResultSet
            statement.addWarning(new SQLWarning(Messages.get(
                    "warning.cursordowngraded", resultSetType + "/" + concurrency), "01000"));
        }

        if ((retVal == null) || (retVal.intValue() != 0)) {
            throw new SQLException(Messages.get("error.resultset.openfail"), "24000");
        }
        //
        // TODO: Save the value of the prepStmtHandle for reuse later
        //
    }

    /**
     * Fetch the next result row from a cursor using the internal sp_cursorfetch procedure.
     *
     * @param fetchType The type of fetch eg FETCH_ABSOLUTE.
     * @param rowNum The row number to fetch.
     * @return <code>boolean</code> true if a result set row is returned.
     * @throws SQLException
     */
    private boolean cursorFetch(int fetchType, int rowNum)
    throws SQLException {
        TdsCore tds = statement.getTds();

        statement.clearWarnings();

        boolean isInfo = (fetchType == FETCH_INFO);

        if (fetchType != FETCH_ABSOLUTE && fetchType != FETCH_RELATIVE) {
            rowNum = 1;
        }

        ParamInfo[] param = new ParamInfo[4];
        // Setup cursor handle param
        param[0] = new ParamInfo();
        param[0].isSet = true;
        param[0].jdbcType = Types.INTEGER;
        param[0].value = cursorHandle;
        param[0].isOutput = false;

        // Setup fetchtype param
        param[1] = new ParamInfo();
        param[1].isSet = true;
        param[1].jdbcType = Types.INTEGER;
        param[1].value = new Integer(fetchType);
        param[1].isOutput = false;

        // Setup rownum
        param[2] = new ParamInfo();
        param[2].isSet = true;
        param[2].jdbcType = Types.INTEGER;
        param[2].value = isInfo ? null : new Integer(rowNum);
        param[2].isOutput = isInfo;

        // Setup numRows parameter
        param[3] = new ParamInfo();
        param[3].isSet = true;
        param[3].jdbcType = Types.INTEGER;
        param[3].value = isInfo ? null : new Integer(1);
        param[3].isOutput = isInfo;

        retVal = null;

        tds.executeSQL(null, "sp_cursorfetch", param, true, statement.getQueryTimeout(), 0);

        while (!tds.getMoreResults() && !tds.isEndOfResponse());

        if (tds.isResultSet()) {
            if (tds.isRowData()) {
                // With TDS 7 the data row (if any) is sent without any
                // preceding resultset header.
                this.currentRow = copyRow(tds.getRowData());
            } else if (tds.getNextRow()) {
                // With TDS 8 there is a dummy result set header first
                // then the data. This case also used if meta data not supressed.
                this.currentRow = copyRow(tds.getRowData());
            } else {
                this.currentRow = null;
            }
        } else {
            this.currentRow  = null;
        }

        tds.clearResponseQueue();
        statement.getMessages().checkErrors();
        retVal = tds.getReturnStatus();

        if (isInfo) {
            pos = ((Integer) param[2].value).intValue();
            rowsInResult = ((Integer) param[3].value).intValue();
        }

        return  currentRow != null;
    }

    /**
     * Support general cursor operations such as delete, update etc.
     *
     * @param opType The type of update to perform.
     * @param row The row number to update.
     * @throws SQLException
     */
    private void cursor(int opType , ColData[] row) throws SQLException {
        TdsCore tds = statement.getTds();

        statement.clearWarnings();
        ParamInfo param[];

        if (opType == CURSOR_OP_DELETE) {
            if (row != null) {
            }

            // 3 parameters for delete
            param = new ParamInfo[3];
        } else {
            if (row == null) {
                throw new SQLException(Messages.get("error.resultset.update"), "24000");
            }
            // 4 parameters plus one for each column for insert/update
            param = new ParamInfo[4 + columnCount];
        }

        // Setup cursor handle param
        param[0] = new ParamInfo();
        param[0].isSet = true;
        param[0].jdbcType = Types.INTEGER;
        param[0].value = cursorHandle;

        // Setup optype param
        param[1] = new ParamInfo();
        param[1].isSet = true;
        param[1].jdbcType = Types.INTEGER;
        param[1].value = new Integer(opType);

        // Setup rownum
        param[2] = new ParamInfo();
        param[2].isSet = true;
        param[2].jdbcType = Types.INTEGER;
        param[2].value = new Integer(1);

        // If row is not null, we're dealing with an insert/update
        if (row != null) {
            // Setup table
            param[3] = new ParamInfo();
            param[3].isSet = true;
            param[3].jdbcType = Types.VARCHAR;
            param[3].value = "";

            int colCnt = columnCount;
            // Current column; we should only update/insert columns
            // that are not read-only (such as identity columns)
            int crtCol = 4;

            for (int i = 0; i < colCnt; i++) {
                // Only send non-read-only columns
                if (columns[i].isWriteable && row[i].isUpdated()) {
                    ColInfo col = columns[i];
                    ParamInfo pi = new ParamInfo();
                    param[crtCol] = pi;
                    pi.isSet = true;
                    pi.jdbcType = col.jdbcType;
                    pi.value = row[i].getValue();
                    pi.length = row[i].getLength();
                    pi.name = '@' + col.realName;
                    pi.bufferSize = col.bufferSize;
                    pi.scale = col.scale;
                    pi.tdsType = col.tdsType;
                    pi.collation = col.collation;
                    crtCol++;
                } else {
                    if (opType == CURSOR_OP_INSERT && row[i].getValue() != null)
                        throw new SQLException(Messages.get("error.resultset.insert",
                                Integer.toString(i + 1), columns[i].realName), "24000");
                }
            }

            if (crtCol == 4) {
                // No column to update so bail out!
                return;
            }

            // If the count is different (i.e. there were read-only
            // columns) reallocate the parameters into a shorter array
            if (crtCol != colCnt + 4) {
                ParamInfo[] newParam = new ParamInfo[crtCol];

                System.arraycopy(param, 0, newParam, 0, crtCol);
                param = newParam;
            }
        }

        retVal = null;

        tds.executeSQL(null, "sp_cursor", param, false, statement.getQueryTimeout(), -1);
        tds.clearResponseQueue();
        statement.getMessages().checkErrors();
        retVal = tds.getReturnStatus();
    }

    /**
     * Close a server side cursor.
     *
     * @throws SQLException
     */
    private void cursorClose() throws SQLException {
        TdsCore tds = statement.getTds();

        statement.clearWarnings();

        ParamInfo param[] = new ParamInfo[1];
        param[0] = new ParamInfo();

        // Setup cursor handle param
        param[0].isSet = true;
        param[0].jdbcType = Types.INTEGER;
        param[0].value = cursorHandle;
        param[0].isOutput = false;

        tds.executeSQL(null, "sp_cursorclose", param, false, statement.getQueryTimeout(), -1);
        tds.clearResponseQueue();
        statement.getMessages().checkErrors();
        retVal = tds.getReturnStatus();
        //
        // If using prepared statements unprepare now to prevent resource leaks on server.
        //
        if (prepStmtHandle != null) {
            param[0].value = prepStmtHandle;
            tds.executeSQL(null, "sp_cursorunprepare", param, false, statement.getQueryTimeout(), -1);
            tds.clearResponseQueue();
            statement.getMessages().checkErrors();
        }
    }

//
// -------------------- java.sql.ResultSet methods -------------------
//

    public void afterLast() throws SQLException {
        checkOpen();
        checkScrollable();

        if (pos != POS_AFTER_LAST) {
            // SAfe -1 should work just as well
            cursorFetch(FETCH_ABSOLUTE, rowsInResult + 1);
            pos = POS_AFTER_LAST;
        }
    }

    public void beforeFirst() throws SQLException {
        checkOpen();
        checkScrollable();

        if (pos != POS_BEFORE_FIRST) {
            cursorFetch(FETCH_ABSOLUTE, 0);
            pos = POS_BEFORE_FIRST;
        }
    }

    public void cancelRowUpdates() throws SQLException {
        checkOpen();
        checkUpdateable();

        if (onInsertRow) {
            throw new SQLException(Messages.get("error.resultset.insrow"), "24000");
        }

        refreshRow();
    }

    public void close() throws SQLException {
        if (!closed) {
            try {
                if (!statement.getConnection().isClosed()) {
                    cursorClose();
                }
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

        cursor(CURSOR_OP_DELETE, null);
        cursorFetch(FETCH_REPEAT, 1);
    }

    public void insertRow() throws SQLException {
        checkOpen();
        checkUpdateable();

        if (!onInsertRow) {
            throw new SQLException(Messages.get("error.resultset.notinsrow"), "24000");
        }

        cursor(CURSOR_OP_INSERT, insertRow);
        // Update the number of rows and the cursor position
        cursorFetch(FETCH_INFO, 1);

        insertRow = newRow();
    }

    public void moveToCurrentRow() throws SQLException {
        checkOpen();
        checkUpdateable();

        onInsertRow = false;
    }


    public void moveToInsertRow() throws SQLException {
        checkOpen();
        checkUpdateable();

        onInsertRow = true;
        insertRow = newRow();
    }

    public void refreshRow() throws SQLException {
        checkOpen();
        checkUpdateable();

        if (onInsertRow) {
            throw new SQLException(Messages.get("error.resultset.insrow"), "24000");
        }

        cursorFetch(FETCH_REPEAT, 1);
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

        cursor(CURSOR_OP_UPDATE, currentRow);
        // Update the number of rows and the cursor position
        cursorFetch(FETCH_INFO, 1);
        refreshRow();
    }

    public boolean first() throws SQLException {
        checkOpen();
        checkScrollable();

        boolean res = cursorFetch(FETCH_FIRST, 0);

        pos = 1;

        return res;
    }

    public boolean isLast() throws SQLException {
        checkOpen();

        return(pos == rowsInResult) && (rowsInResult != 0);
    }

    public boolean last() throws SQLException {
        checkOpen();
        checkScrollable();

        boolean res = cursorFetch(FETCH_LAST, 0);

        pos = rowsInResult;

        return res;
    }

    public boolean next() throws SQLException {
        checkOpen();

        if (cursorFetch(FETCH_NEXT, 0)) {
            pos += 1;

            return true;
        }

        pos = POS_AFTER_LAST;

        return false;
    }

    public boolean previous() throws SQLException {
        checkOpen();
        checkScrollable();

        if (cursorFetch(FETCH_PREVIOUS, 0)) {
            pos -= (pos < 0) ? (pos - rowsInResult) : 1;

            return true;
        }

        pos = POS_BEFORE_FIRST;

        return false;
    }

    public boolean rowDeleted() throws SQLException {
        checkOpen();
        checkUpdateable();

        return getRowStat() == SQL_ROW_DELETED;
    }

    public boolean rowInserted() throws SQLException {
        checkOpen();
        checkUpdateable();

        return getRowStat() == SQL_ROW_ADDED;
    }

    public boolean rowUpdated() throws SQLException {
        checkOpen();
        checkUpdateable();

        return getRowStat() == SQL_ROW_UPDATED;
    }

    private int getRowStat() throws SQLException {
        ColData data = currentRow[columns.length - 1];

        return((Integer)Support.convert(this, data.getValue(), Types.INTEGER, null)).intValue();
    }

    public boolean absolute(int row) throws SQLException {
        checkOpen();
        checkScrollable();

        // If nothing was fetched, we got passed the beginning or end
        if (!cursorFetch(FETCH_ABSOLUTE, row)) {
            if (row > 0) {
                pos = POS_AFTER_LAST;
            } else {
                pos = POS_BEFORE_FIRST;
            }

            return false;
        }

        pos = row;

        return true;
    }

    public boolean relative(int row) throws SQLException {
        checkOpen();
        checkScrollable();

        if (!cursorFetch(FETCH_RELATIVE, row)) {
            if (row > 0) {
                pos = POS_AFTER_LAST;
            } else {
                pos = POS_BEFORE_FIRST;
            }

            return false;
        }

        // If less than 0, it can only be POS_AFTER_LAST
        if (pos < 0) {
            pos = rowsInResult + 1;
        }

        pos += row;

        return true;
    }
}
