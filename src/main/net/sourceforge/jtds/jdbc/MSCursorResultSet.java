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
import java.sql.ResultSet;

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
 * @version $Id: MSCursorResultSet.java,v 1.45 2005-03-26 21:07:58 alin_sinpalean Exp $
 */
public class MSCursorResultSet extends JtdsResultSet {
    /*
     * Constants
     */
    private static final Integer FETCH_FIRST    = new Integer(1);
    private static final Integer FETCH_NEXT     = new Integer(2);
    private static final Integer FETCH_PREVIOUS = new Integer(4);
    private static final Integer FETCH_LAST     = new Integer(8);
    private static final Integer FETCH_ABSOLUTE = new Integer(16);
    private static final Integer FETCH_RELATIVE = new Integer(32);
    private static final Integer FETCH_REPEAT   = new Integer(128);
    private static final Integer FETCH_INFO     = new Integer(256);

    private static final int CURSOR_TYPE_KEYSET = 1;
    private static final int CURSOR_TYPE_DYNAMIC = 2;
    private static final int CURSOR_TYPE_FORWARD = 4;
    private static final int CURSOR_TYPE_STATIC = 8;
    private static final int CURSOR_TYPE_PARAMETERIZED = 0x1000;

    private static final int CURSOR_CONCUR_READ_ONLY = 1;
    private static final int CURSOR_CONCUR_SCROLL_LOCKS = 2;
    private static final int CURSOR_CONCUR_OPTIMISTIC = 4;

    private static final Integer CURSOR_OP_INSERT = new Integer(4);
    private static final Integer CURSOR_OP_UPDATE = new Integer(33);
    private static final Integer CURSOR_OP_DELETE = new Integer(34);

    /**
     * The row is dirty and needs to be reloaded (internal state).
     */
    private static final Integer SQL_ROW_DIRTY   = new Integer(0);

    /**
     * The row is valid.
     */
    private static final Integer SQL_ROW_SUCCESS = new Integer(1);

    /**
     * The row has been deleted.
     */
    private static final Integer SQL_ROW_DELETED = new Integer(2);

    /*
     * Instance variables.
     */
    /** The prepared statment handle from prepare/prepexec. */
    private Integer prepStmtHandle;
    /** Set when <code>moveToInsertRow()</code> was called. */
    private boolean onInsertRow;
    /** The "insert row". */
    private ParamInfo[] insertRow;
    /** The "update row". */
    private ParamInfo[] updateRow;
    /** The row cache used instead {@link #currentRow}. */
    private Object[][] rowCache;
    /** Actual position of the cursor. */
    private int cursorPos;

    //
    // Fixed sp_XXX parameters
    //
    /** Cursor handle parameter. */
    private final ParamInfo PARAM_CURSOR_HANDLE = new ParamInfo(Types.INTEGER, null, ParamInfo.INPUT);

    /** <code>sp_cursorfetch</code> fetchtype parameter. */
    private final ParamInfo PARAM_FETCHTYPE = new ParamInfo(Types.INTEGER, null, ParamInfo.INPUT);

    /** <code>sp_cursorfetch</code> rownum IN parameter (for actual fetches). */
    private final ParamInfo PARAM_ROWNUM_IN = new ParamInfo(Types.INTEGER, null, ParamInfo.INPUT);

    /** <code>sp_cursorfetch</code> numrows IN parameter (for actual fetches). */
    private final ParamInfo PARAM_NUMROWS_IN = new ParamInfo(Types.INTEGER, new Integer(1), ParamInfo.INPUT);

    /** <code>sp_cursorfetch</code> rownum OUT parameter (for FETCH_INFO). */
    private final ParamInfo PARAM_ROWNUM_OUT = new ParamInfo(Types.INTEGER, null, ParamInfo.OUTPUT);

    /** <code>sp_cursorfetch</code> numrows OUT parameter (for FETCH_INFO). */
    private final ParamInfo PARAM_NUMROWS_OUT = new ParamInfo(Types.INTEGER, null, ParamInfo.OUTPUT);

    /** <code>sp_cursor</code> optype parameter. */
    private final ParamInfo PARAM_OPTYPE = new ParamInfo(Types.INTEGER, null, ParamInfo.INPUT);

    /** <code>sp_cursor</code> rownum parameter. */
    private final ParamInfo PARAM_ROWNUM = new ParamInfo(Types.INTEGER, new Integer(1), ParamInfo.INPUT);

    /** <code>sp_cursor</code> table parameter. */
    private final ParamInfo PARAM_TABLE = new ParamInfo(Types.VARCHAR, "", ParamInfo.UNICODE);

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
        super(statement, resultSetType, concurrency, null);

        rowCache = new Object[fetchSize][];

        cursorCreate(sql, procName, procedureParams);
    }

    /**
     * Set the specified column's data value.
     *
     * @param colIndex The index of the column in the row.
     * @param value The new column value.
     */
    protected void setColValue(int colIndex, int jdbcType, Object value, int length)
            throws SQLException {

        super.setColValue(colIndex, jdbcType, value, length);

        if (!onInsertRow && getCurrentRow() == null) {
            throw new SQLException(Messages.get("error.resultset.norow"), "24000");
        }
        colIndex--;
        ParamInfo pi;
        ColInfo ci = columns[colIndex];

        if (onInsertRow) {
            pi = insertRow[colIndex];
        } else {
            if (updateRow == null) {
                updateRow = new ParamInfo[columnCount];
            }
            pi = updateRow[colIndex];
        }

        if (pi == null) {
            pi = new ParamInfo(-1, TdsData.isUnicode(ci));
            pi.name = '@'+ci.realName;
            pi.collation = ci.collation;
            pi.charsetInfo = ci.charsetInfo;
            if (onInsertRow) {
                insertRow[colIndex] = pi;
            } else {
                updateRow[colIndex] = pi;
            }
        }

        if (value == null) {
            pi.value    = null;
            pi.length   = 0;
            pi.jdbcType = ci.jdbcType;
            pi.isSet    = true;
        } else {
            pi.value     = value;
            pi.length    = length;
            pi.isSet     = true;
            pi.jdbcType  = jdbcType;
            pi.isUnicode = ci.sqlType.equals("ntext")
                    || ci.sqlType.equals("nchar")
                    || ci.sqlType.equals("nvarchar");
        }
    }

    /**
     * Get the specified column's data item.
     *
     * @param index the column index in the row
     * @return the column value as an <code>Object</code>
     * @throws SQLException if the index is out of bounds or there is no
     *                      current row
     */
    protected Object getColumn(int index) throws SQLException {
        checkOpen();

        if (index < 1 || index > columnCount) {
            throw new SQLException(Messages.get("error.resultset.colindex",
                    Integer.toString(index)),
                    "07009");
        }

        Object[] currentRow;
        if (onInsertRow || (currentRow = getCurrentRow()) == null) {
            throw new SQLException(
                    Messages.get("error.resultset.norow"), "24000");
        }

        if (SQL_ROW_DIRTY.equals(currentRow[columns.length - 1])) {
            cursorFetch(FETCH_REPEAT, 0);
            currentRow = getCurrentRow();
        }

        Object data = currentRow[index - 1];
        wasNull = data == null;

        return data;
    }

    /**
     * Translates a JDBC result set type into SQL Server native @scrollOpt value
     * for use with stored procedures such as sp_cursoropen, sp_cursorprepare
     * or sp_cursorprepexec.
     *
     * @param resultSetType JDBC result set type (one of the
     *                      <code>ResultSet.TYPE_<i>XXX</i></code> values)
     * @return a value for the @scrollOpt parameter
     */
    static int getCursorScrollOpt(int resultSetType, boolean parameterized) {
        int scrollOpt;

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

        // If using sp_cursoropen need to set a flag on scrollOpt.
        // The 0x1000 tells the server that there is a parameter
        // definition and user parameters present. If this flag is
        // not set the driver will ignore the additional parameters.
        if (parameterized) {
            scrollOpt |= CURSOR_TYPE_PARAMETERIZED;
        }

        return scrollOpt;
    }

    /**
     * Translates a JDBC result set concurrency into SQL Server native @ccOpt
     * value for use with stored procedures such as sp_cursoropen,
     * sp_cursorprepare or sp_cursorprepexec.
     *
     * @param resultSetConcurrency JDBC result set concurrency (one of the
     *                             <code>ResultSet.CONCUR_<i>XXX</i></code>
     *                             values)
     * @return a value for the @scrollOpt parameter
     */
    static int getCursorConcurrencyOpt(int resultSetConcurrency) {
        switch (resultSetConcurrency) {
            case CONCUR_READ_ONLY:
            default:
                return CURSOR_CONCUR_READ_ONLY;

            case CONCUR_UPDATABLE:
                return CURSOR_CONCUR_SCROLL_LOCKS;
        }
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

        //
        // If this cursor is going to be a named forward only cursor
        // force the concurrency to be updateable.
        // TODO: Cursor is updateable unless user appends FOR READ to the select
        // but we would need to parse the SQL to discover this.
        //
        if (cursorName != null
            && resultSetType == ResultSet.TYPE_FORWARD_ONLY
            && concurrency == ResultSet.CONCUR_READ_ONLY) {
            concurrency = ResultSet.CONCUR_UPDATABLE;
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
        // TODO Is this really the best solution? What if the query is complex?
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
                StringBuffer buf = new StringBuffer(procName.length() + 16
                        + (parameters != null ? parameters.length * 5 : 0));
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
                // This handle was obtained using sp_cursorprepare not sp_prepare
                // so it's ok to use here.
                //
                try {
                    prepStmtHandle = new Integer(procName);
                } catch (NumberFormatException e) {
                    throw new IllegalStateException(
                               "Invalid prepared statement handle: " +
                                      procName);
                }
            }
        }

        //
        // Select the correct type of Server side cursor to
        // match the scroll and concurrency options.
        //
        int scrollOpt = getCursorScrollOpt(resultSetType, parameters != null);
        int ccOpt = getCursorConcurrencyOpt(concurrency);

        //
        // Create parameter objects
        //
        // Setup scroll options parameter
        //
        ParamInfo pScrollOpt  = new ParamInfo(Types.INTEGER, new Integer(scrollOpt), ParamInfo.OUTPUT);
        //
        // Setup concurrency options parameter
        //
        ParamInfo pConCurOpt  = new ParamInfo(Types.INTEGER, new Integer(ccOpt), ParamInfo.OUTPUT);
        //
        // Setup number of rows parameter
        //
        ParamInfo pRowCount   = new ParamInfo(Types.INTEGER, null, ParamInfo.OUTPUT);
        //
        // Setup cursor handle parameter
        //
        ParamInfo pCursor = new ParamInfo(Types.INTEGER, null, ParamInfo.OUTPUT);
        //
        // Setup statement handle param
        //
        ParamInfo pStmtHand = null;
        if (prepareSql == TdsCore.PREPEXEC || prepareSql == TdsCore.PREPARE) {
            pStmtHand = new ParamInfo(Types.INTEGER, prepStmtHandle, ParamInfo.OUTPUT);
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

            pParamDef  = new ParamInfo(Types.LONGVARCHAR,
                    Support.getParameterDefinitions(parameters),
                    ParamInfo.UNICODE);
        }
        //
        // Setup SQL statement parameter
        //
        ParamInfo pSQL = new ParamInfo(Types.LONGVARCHAR, sql, ParamInfo.UNICODE);
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
            params[3] = new ParamInfo(Types.INTEGER, new Integer(1), ParamInfo.INPUT);
            // Setup scroll options
            params[4] = pScrollOpt;
            // Setup concurrency options
            params[5] = pConCurOpt;

            columns = null; // Will be populated if preparing a select

            // Use sp_cursorprepare approach
            tds.executeSQL(null, "sp_cursorprepare", params, false,
                    statement.getQueryTimeout(), -1, -1, true);
            tds.clearResponseQueue();

            // columns will now hold meta data for select statements
            prepStmtHandle = (Integer) pStmtHand.getOutValue();

            if (prepStmtHandle == null) {
                // Some types of statements cannot be prepared
                prepareSql = TdsCore.UNPREPARED;
                if (parameters != null) {
                    // Ensure that sp_cursoropen will recognise user params
                    pScrollOpt.value = new Integer(
                            ((Integer)pScrollOpt.getOutValue()).intValue() | 0x1000);
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
            pStmtHand.value = prepStmtHandle;
            params[0] = pStmtHand;
            // Setup cursor handle param
            params[1] = pCursor;
            // Setup scroll options (mask off parameter flag)
            pScrollOpt.value = new Integer(
                    ((Integer)pScrollOpt.getOutValue()).intValue() & 0XFF);
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

        tds.executeSQL(null, procName, parameters, false,
                statement.getQueryTimeout(), statement.getMaxRows(),
                statement.getMaxFieldSize(), true);

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
        Integer retVal = tds.getReturnStatus();

        int actualScroll;
        int actualCc;

        //
        // Retrieve values of output parameters
        //
        PARAM_CURSOR_HANDLE.value = pCursor.getOutValue();
        actualScroll = ((Integer)pScrollOpt.getOutValue()).intValue();
        actualCc     = ((Integer)pConCurOpt.getOutValue()).intValue();
        rowsInResult = ((Integer)pRowCount.getOutValue()).intValue();
        if (prepareSql == TdsCore.PREPEXEC) {
            prepStmtHandle = (Integer)pStmtHand.getOutValue();
        }

        if ((retVal == null) || (retVal.intValue() != 0)) {
            throw new SQLException(Messages.get("error.resultset.openfail"), "24000");
        }
        //
        // Set the cursor name if required allowing positioned updates.
        // We need to do this here as any downgrade warnings will be wiped
        // out by the executeSQL call.
        //
        if (cursorName != null) {
            ParamInfo params[] = new ParamInfo[3];
            params[0] = PARAM_CURSOR_HANDLE;
            PARAM_OPTYPE.value = new Integer(2);
            params[1] = PARAM_OPTYPE;
            params[2] = new ParamInfo(Types.VARCHAR, cursorName, ParamInfo.UNICODE);
            tds.executeSQL(null, "sp_cursoroption", params, true, 0, -1, -1, true);
            tds.clearResponseQueue();
            if (tds.getReturnStatus().intValue() != 0) {
                throw new SQLException(Messages.get("error.resultset.openfail"), "24000");
            }
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
    private boolean cursorFetch(Integer fetchType, int rowNum)
            throws SQLException {
        TdsCore tds = statement.getTds();

        statement.clearWarnings();

        if (fetchType != FETCH_ABSOLUTE && fetchType != FETCH_RELATIVE) {
            rowNum = 1;
        }

        ParamInfo[] param = new ParamInfo[4];
        // Setup cursor handle param
        param[0] = PARAM_CURSOR_HANDLE;

        // Setup fetchtype param
        PARAM_FETCHTYPE.value = fetchType;
        param[1] = PARAM_FETCHTYPE;

        // Setup rownum
        PARAM_ROWNUM_IN.value = new Integer(rowNum);
        param[2] = PARAM_ROWNUM_IN;
        // Setup numRows parameter
        if (((Integer) PARAM_NUMROWS_IN.value).intValue() != fetchSize) {
            // If the fetch size changed, update the parameter and cache size
            PARAM_NUMROWS_IN.value = new Integer(fetchSize);
            rowCache = new Object[fetchSize][];
        }
        param[3] = PARAM_NUMROWS_IN;

        synchronized (tds) {
            // No meta data, no timeout (we're not sending it yet), no row
            // limit, don't send yet
            tds.executeSQL(null, "sp_cursorfetch", param, true, 0, 0,
                    statement.getMaxFieldSize(), false);

            // Setup fetchtype param
            PARAM_FETCHTYPE.value = FETCH_INFO;
            param[1] = PARAM_FETCHTYPE;

            // Setup rownum
            PARAM_ROWNUM_OUT.clearOutValue();
            param[2] = PARAM_ROWNUM_OUT;
            // Setup numRows parameter
            PARAM_NUMROWS_OUT.clearOutValue();
            param[3] = PARAM_NUMROWS_OUT;

            // No meta data, use the statement timeout, leave max rows as it is
            // (no limit), leave max field size as it is, send now
            tds.executeSQL(null, "sp_cursorfetch", param, true,
                    statement.getQueryTimeout(), -1, -1, true);
        }

        while (!tds.getMoreResults() && !tds.isEndOfResponse());

        int i = 0;
        if (tds.isResultSet()) {
            // With TDS 7 the data row (if any) is sent without any
            // preceding resultset header.
            // With TDS 8 there is a dummy result set header first
            // then the data. This case also used if meta data not supressed.
            if (tds.isRowData() || tds.getNextRow()) {
                do {
                    rowCache[i++] = copyRow(tds.getRowData());
                } while (tds.getNextRow());
            }
        }
        // Set the rest of the rows to null
        for (; i < rowCache.length; ++i) {
            rowCache[i] = null;
        }

        tds.clearResponseQueue();

        pos = ((Integer) PARAM_ROWNUM_OUT.getOutValue()).intValue();
        cursorPos = pos;
        rowsInResult = ((Integer) PARAM_NUMROWS_OUT.getOutValue()).intValue();

        statement.getMessages().checkErrors();

        return getCurrentRow() != null;
    }

    /**
     * Support general cursor operations such as delete, update etc.
     *
     * @param opType the type of operation to perform
     * @param row    the row number to update
     * @throws SQLException
     */
    private void cursor(Integer opType , ParamInfo[] row) throws SQLException {
        TdsCore tds = statement.getTds();

        statement.clearWarnings();
        ParamInfo param[];

        if (opType == CURSOR_OP_DELETE) {
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
        param[0] = PARAM_CURSOR_HANDLE;

        // Setup optype param
        PARAM_OPTYPE.value = opType;
        param[1] = PARAM_OPTYPE;

        // Setup rownum
        PARAM_ROWNUM.value = new Integer(pos - cursorPos + 1);
        param[2] = PARAM_ROWNUM;

        // If row is not null, we're dealing with an insert/update
        if (row != null) {
            // Setup table
            param[3] = PARAM_TABLE;

            int colCnt = columnCount;
            // Current column; we will only update/insert columns for which
            // values were specified
            int crtCol = 4;
            // Name of the table to insert default values into (if necessary)
            String tableName = null;

            for (int i = 0; i < colCnt; i++) {
                ParamInfo pi = row[i];
                ColInfo col = columns[i];

                if (pi != null && pi.isSet) {
                    if (!col.isWriteable) {
                        // Column is read-only but was updated
                        throw new SQLException(Messages.get("error.resultset.insert",
                                Integer.toString(i + 1), col.realName), "24000");
                    }

                    param[crtCol++] = pi;
                }
                if (tableName == null && col.tableName != null) {
                    if (col.catalog != null || col.schema != null) {
                        tableName = (col.catalog != null ? col.catalog : "")
                                + "." + (col.schema != null ? col.schema : "")
                                + "." + col.tableName;
                    } else {
                        tableName = col.tableName;
                    }
                }
            }

            if (crtCol == 4) {
                if (opType == CURSOR_OP_INSERT) {
                    // Insert default values for all columns.
                    // There seem to be two forms of sp_cursor: one with
                    // parameter names and values and one w/o names and with
                    // expressions (this is where 'default' comes in).
                    param[crtCol] = new ParamInfo(Types.VARCHAR,
                            "insert " + tableName + " default values",
                            ParamInfo.UNICODE);
                    crtCol++;
                } else {
                    // No column to update so bail out!
                    return;
                }
            }

            // If the count is different (i.e. there were read-only
            // columns) reallocate the parameters into a shorter array
            if (crtCol != colCnt + 4) {
                ParamInfo[] newParam = new ParamInfo[crtCol];

                System.arraycopy(param, 0, newParam, 0, crtCol);
                param = newParam;
            }
        }

        synchronized (tds) {
            // With meta data (we're not expecting any ResultSets), no timeout
            // (because we're not sending the request yet), don't alter max
            // rows, don't alter max field size, don't send yet
            tds.executeSQL(null, "sp_cursor", param, false, 0, -1, -1, false);

            if (param.length != 4) {
                param = new ParamInfo[4];
                param[0] = PARAM_CURSOR_HANDLE;
            }

            // Setup fetchtype param
            PARAM_FETCHTYPE.value = FETCH_INFO;
            param[1] = PARAM_FETCHTYPE;

            // Setup rownum
            PARAM_ROWNUM_OUT.clearOutValue();
            param[2] = PARAM_ROWNUM_OUT;
            // Setup numRows parameter
            PARAM_NUMROWS_OUT.clearOutValue();
            param[3] = PARAM_NUMROWS_OUT;

            // No meta data (no ResultSets expected), use statement timeout,
            // don't alter max rows, don't alter max field size, send now
            tds.executeSQL(null, "sp_cursorfetch", param, true,
                    statement.getQueryTimeout(), -1, -1, true);
        }

        // Consume the sp_cursor response
        tds.consumeOneResponse();
        statement.getMessages().checkErrors();
        Integer retVal = tds.getReturnStatus();
        if (retVal.intValue() != 0) {
            throw new SQLException(Messages.get("error.resultset.cursorfail"),
                    "24000");
        }

        //
        // Allow row values to be garbage collected
        //
        if (row != null) {
            for (int i = 0; i < row.length; i++) {
                if (row[i] != null) {
                    row[i].clearInValue();
                }
            }
        }

        // Consume the sp_cursorfetch response
        tds.clearResponseQueue();
        statement.getMessages().checkErrors();
        cursorPos = ((Integer) PARAM_ROWNUM_OUT.getOutValue()).intValue();
        rowsInResult = ((Integer) PARAM_NUMROWS_OUT.getOutValue()).intValue();

        // Update row status
        if (opType == CURSOR_OP_DELETE || opType == CURSOR_OP_UPDATE) {
            Object[] currentRow = getCurrentRow();
            if (currentRow == null) {
                throw new SQLException(
                        Messages.get("error.resultset.updatefail"), "24000");
            }
            // No need to re-fetch the row, just mark it as deleted or dirty
            currentRow[columns.length - 1] =
                    (opType == CURSOR_OP_DELETE) ? SQL_ROW_DELETED : SQL_ROW_DIRTY;
        }
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

        // Setup cursor handle param
        param[0] = PARAM_CURSOR_HANDLE;

        tds.executeSQL(null, "sp_cursorclose", param, false,
                statement.getQueryTimeout(), -1, -1, true);
        tds.clearResponseQueue();
        statement.getMessages().checkErrors();
        //
        // If using prepared statements unprepare now to prevent resource leaks on server.
        //
        if (prepStmtHandle != null) {
            param[0].value = prepStmtHandle;
            tds.executeSQL(null, "sp_cursorunprepare", param, false,
                    statement.getQueryTimeout(), -1, -1, true);
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
            // SAfe Just fetch a very large absolute value
            cursorFetch(FETCH_ABSOLUTE, Integer.MAX_VALUE);
        }
    }

    public void beforeFirst() throws SQLException {
        checkOpen();
        checkScrollable();

        if (pos != POS_BEFORE_FIRST) {
            cursorFetch(FETCH_ABSOLUTE, 0);
        }
    }

    public void cancelRowUpdates() throws SQLException {
        checkOpen();
        checkUpdateable();

        if (onInsertRow) {
            throw new SQLException(Messages.get("error.resultset.insrow"), "24000");
        }

        for (int i = 0; updateRow != null && i < updateRow.length; i++) {
            if (updateRow[i] != null) {
                updateRow[i].clearInValue();
            }
        }
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

        if (getCurrentRow() == null) {
            throw new SQLException(Messages.get("error.resultset.norow"), "24000");
        }

        if (onInsertRow) {
            throw new SQLException(Messages.get("error.resultset.insrow"), "24000");
        }

        cursor(CURSOR_OP_DELETE, null);
    }

    public void insertRow() throws SQLException {
        checkOpen();
        checkUpdateable();

        if (!onInsertRow) {
            throw new SQLException(Messages.get("error.resultset.notinsrow"), "24000");
        }

        cursor(CURSOR_OP_INSERT, insertRow);
    }

    public void moveToCurrentRow() throws SQLException {
        checkOpen();
        checkUpdateable();

        onInsertRow = false;
    }

    public void moveToInsertRow() throws SQLException {
        checkOpen();
        checkUpdateable();
        if (insertRow == null) {
            insertRow = new ParamInfo[columnCount];
        }
        onInsertRow = true;
    }

    public void refreshRow() throws SQLException {
        checkOpen();

        if (onInsertRow) {
            throw new SQLException(Messages.get("error.resultset.insrow"), "24000");
        }

        // Save and restore current position
        int crtPos = pos;
        cursorFetch(FETCH_REPEAT, 0);
        pos = crtPos;
    }

    public void updateRow() throws SQLException {
        checkOpen();
        checkUpdateable();

        if (getCurrentRow() == null) {
            throw new SQLException(Messages.get("error.resultset.norow"), "24000");
        }

        if (onInsertRow) {
            throw new SQLException(Messages.get("error.resultset.insrow"), "24000");
        }

        if (updateRow != null) {
            cursor(CURSOR_OP_UPDATE, updateRow);
        }
    }

    public boolean first() throws SQLException {
        checkOpen();
        checkScrollable();

        pos = 1;
        if (getCurrentRow() == null) {
            return cursorFetch(FETCH_FIRST, 0);
        } else {
            return true;
        }
    }

    // FIXME Make the isXXX() methods work with forward-only cursors (rowsInResult == -1)
    public boolean isLast() throws SQLException {
        checkOpen();

        return(pos == rowsInResult) && (rowsInResult != 0);
    }

    public boolean last() throws SQLException {
        checkOpen();
        checkScrollable();

        pos = rowsInResult;
        if (getCurrentRow() == null) {
            if (cursorFetch(FETCH_LAST, 0)) {
                // Set pos to the last row, as the number of rows can change
                pos = rowsInResult;
                return true;
            } else {
                return false;
            }
        } else {
            return true;
        }
    }

    public boolean next() throws SQLException {
        checkOpen();

        ++pos;
        if (getCurrentRow() == null) {
            return cursorFetch(FETCH_NEXT, 0);
        } else {
            return true;
        }
    }

    public boolean previous() throws SQLException {
        checkOpen();
        checkScrollable();

        // Save current ResultSet position
        int initPos = pos;
        // Decrement current position
        --pos;
        if (initPos == POS_AFTER_LAST || getCurrentRow() == null) {
            boolean res = cursorFetch(FETCH_PREVIOUS, 0);
            pos = (initPos == POS_AFTER_LAST) ? rowsInResult : (initPos - 1);
            return res;
        } else {
            return true;
        }
    }

    public boolean rowDeleted() throws SQLException {
        checkOpen();

        Object[] currentRow = getCurrentRow();

        // If there is no current row, return false (the row was not deleted)
        if (currentRow == null) {
            return false;
        }

        // Reload if dirty
        if (SQL_ROW_DIRTY.equals(currentRow[columns.length - 1])) {
            // Save and restore ResultSet position
            int crtPos = pos;
            cursorFetch(FETCH_REPEAT, 0);
            pos = crtPos;
            currentRow = getCurrentRow();
        }

        return SQL_ROW_DELETED.equals(currentRow[columns.length - 1]);
    }

    public boolean rowInserted() throws SQLException {
        checkOpen();
        // No way to find out
        return false;
    }

    public boolean rowUpdated() throws SQLException {
        checkOpen();
        // No way to find out
        return false;
    }

    public boolean absolute(int row) throws SQLException {
        checkOpen();
        checkScrollable();

        pos = row;
        if (getCurrentRow() == null) {
            return cursorFetch(FETCH_ABSOLUTE, row);
        } else {
            return true;
        }
    }

    public boolean relative(int row) throws SQLException {
        checkOpen();
        checkScrollable();

        pos += row;
        if (getCurrentRow() == null) {
            return cursorFetch(FETCH_RELATIVE, row);
        } else {
            return true;
        }
    }

    protected Object[] getCurrentRow() {
        if (pos < cursorPos || pos >= cursorPos + rowCache.length) {
            return null;
        }

        return rowCache[pos - cursorPos];
    }
}
