package net.sourceforge.jtds.jdbc;

import java.sql.*;

/**
 * Implements a cursor-based <code>ResultSet</code>. Implementation is based on
 * the undocumented <code>sp_cursor</code> procedures.
 *
 * @created    March 16, 2001
 * @version    2.0
 */
public class CursorResultSet extends AbstractResultSet implements OutputParamHandler {
    public static final int FETCH_FIRST = 1;
    public static final int FETCH_NEXT = 2;
    public static final int FETCH_PREVIOUS = 4;
    public static final int FETCH_LAST = 8;
    public static final int FETCH_ABSOLUTE = 16;
    public static final int FETCH_RELATIVE = 32;
    public static final int FETCH_REPEAT = 128;
    public static final int FETCH_INFO = 256;

    public static final int CURSOR_TYPE_KEYSET = 1;
    public static final int CURSOR_TYPE_DYNAMIC = 2;
    public static final int CURSOR_TYPE_FORWARD = 4;
    public static final int CURSOR_TYPE_STATIC = 8;

    public static final int CURSOR_CONCUR_READ_ONLY = 1;
    public static final int CURSOR_CONCUR_SCROLL_LOCKS = 2;
    public static final int CURSOR_CONCUR_OPTIMISTIC = 4;

    public static final int CURSOR_OP_INSERT = 4;
    public static final int CURSOR_OP_UPDATE = 33;
    public static final int CURSOR_OP_DELETE = 34;

    /**
     * The row is unchanged.
     */
    public static final int SQL_ROW_SUCCESS = 1;

    /**
     * The row has been deleted.
     */
    public static final int SQL_ROW_DELETED = 2;

    // @todo Check the values for the constants below (above are ok).
    /**
     * The row has been updated.
     */
    public static final int SQL_ROW_UPDATED = 3;

    /**
     * There is no row that corresponds this position.
     */
    public static final int SQL_ROW_NOROW = 4;

    /**
     * The row has been added.
     */
    public static final int SQL_ROW_ADDED = 5;

    /**
     * The row is unretrievable due to an error.
     */
    public static final int SQL_ROW_ERROR = 6;

    private static final int POS_BEFORE_FIRST = 0;
    private static final int POS_AFTER_LAST = -1;

    private int pos = POS_BEFORE_FIRST;

    private Integer cursorHandle;
    private int rowsInResult;

    private String sql;
    private TdsStatement stmt;
    private TdsConnection conn;

    private int direction = FETCH_FORWARD;

    private boolean open = false;

    private Context context;

    private PacketRowResult current;

    /**
     * Parameter list used for calling stored procedures.
     */
    private ParameterListItem[] parameterList;

    /**
     * Stores return values from stored procedure calls.
     */
    private Integer retVal;

    /**
     * Index of the last output parameter.
     */
    private int lastOutParam = -1;

    private int type;
    private int concurrency;

    /**
     * Set when <code>moveToInsertRow()</code> was called.
     */
    private boolean onInsertRow = false;
    /**
     * The "insert row".
     */
    private PacketRowResult insertRow;

    /**
     * Internal <code>Statement</code> used to execute the cursor API calls
     * without affecting the (external) <code>Statement</code>, that created
     * this <code>ResultSet</code>.
     */
    private TdsStatement internalStmt;

    public CursorResultSet(TdsStatement stmt, String sql, int fetchDir,
                           ParameterListItem[] parameters)
            throws SQLException {
        this.stmt = stmt;
        this.conn = (TdsConnection) stmt.getConnection();
        this.sql = sql;
        this.direction = fetchDir == FETCH_UNKNOWN ? FETCH_FORWARD : fetchDir;

        this.type = stmt.getResultSetType();
        this.concurrency = stmt.getResultSetConcurrency();

        // Create the internal Statement
        internalStmt = new TdsStatement(conn);
        internalStmt.outParamHandler = this;

        // SAfe Until we're actually created use the Statement's warning chain.
        warningChain = stmt.warningChain;

        cursorCreate(parameters);

        // SAfe Now we can create our own warning chain.
        warningChain = new SQLWarningChain();
    }

    protected void finalize() {
        try {
            close();
        } catch (SQLException e) {
        }
    }

    public Context getContext() {
        return context;
    }

    public void setFetchDirection(int direction) throws SQLException {
        switch (direction) {
        case FETCH_UNKNOWN:
        case FETCH_REVERSE:
            if (type == ResultSet.TYPE_FORWARD_ONLY) {
                throw new SQLException("ResultSet is forward-only.");
            }
            // Fall through

        case FETCH_FORWARD:
            this.direction = direction;
            break;

        default:
            throw new SQLException("Invalid fetch direction: "+direction);
        }
    }

    public void setFetchSize(int rows) throws SQLException {
    }

    public String getCursorName() throws SQLException {
        throw new SQLException("Positioned update not supported.");
    }

    public boolean isBeforeFirst() throws SQLException {
        return (pos == POS_BEFORE_FIRST) && (rowsInResult != 0);
    }

    public boolean isAfterLast() throws SQLException {
        return (pos == POS_AFTER_LAST) && (rowsInResult != 0);
    }

    public boolean isFirst() throws SQLException {
        return pos == 1;
    }

    public boolean isLast() throws SQLException {
        return (pos == rowsInResult) && (rowsInResult != 0);
    }

    public int getRow() throws SQLException {
        return pos>0 ? pos : 0;
    }

    public int getFetchDirection() throws SQLException {
        return direction;
    }

    public int getFetchSize() throws SQLException {
        return 1;
    }

    public int getType() throws SQLException {
        return type;
    }

    public int getConcurrency() throws SQLException {
        return concurrency;
    }

    public Statement getStatement() throws SQLException {
        return stmt;
    }

    public void close() throws SQLException {
        if (open) {
            try {
                warningChain.clearWarnings();
                if (!conn.isClosed())
                    cursorClose();
                stmt.cursorResults.remove(this);
            } finally {
                open = false;
                stmt = null;
                conn = null;

                try {
                    internalStmt.close();
                } catch (SQLException e) {
                    warningChain.addException(e);
                    internalStmt = null;
                }
            }
        }

        warningChain.checkForExceptions();
    }

    public SQLWarning getWarnings() throws SQLException {
        return warningChain.getWarnings();
    }

    public boolean next() throws SQLException {
        if (cursorFetch(FETCH_NEXT, 0)) {
            pos += 1;
            return true;
        } else {
            pos = POS_AFTER_LAST;
            return false;
        }
    }

    public void clearWarnings() throws SQLException {
        warningChain.clearWarnings();
    }

    public void beforeFirst() throws SQLException {
        if (pos != POS_BEFORE_FIRST) {
            cursorFetch(FETCH_ABSOLUTE, 0);
            pos = POS_BEFORE_FIRST;
        }
    }

    public void afterLast() throws SQLException {
        if (pos != POS_AFTER_LAST) {
            // SAfe -1 should work just as well
            cursorFetch(FETCH_ABSOLUTE, rowsInResult + 1);
            pos = POS_AFTER_LAST;
        }
    }

    public boolean first()
            throws SQLException {
        boolean res = cursorFetch(FETCH_FIRST, 0);
        pos = 1;
        return res;
    }

    public boolean last()
            throws SQLException {
        boolean res = cursorFetch(FETCH_LAST, 0);
        pos = rowsInResult;
        return res;
    }

    public boolean absolute(int row)
            throws SQLException {
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

    public boolean relative(int rows) throws SQLException {
        if (!cursorFetch(FETCH_RELATIVE, rows)) {
            if (rows > 0) {
                pos = POS_AFTER_LAST;
            } else {
                pos = POS_BEFORE_FIRST;
            }
            return false;
        }

        // If less than 0, it can only be POS_AFTER_LAST
        if( pos < 0 ) {
            pos = rowsInResult + 1;
        }
        pos += rows;
        return true;
    }

    public boolean previous() throws SQLException {
        if (cursorFetch(FETCH_PREVIOUS, 0)) {
            pos -= (pos < 0) ? (pos-rowsInResult) : 1;
            return true;
        } else {
            pos = POS_BEFORE_FIRST;
            return false;
        }
    }

    public boolean rowUpdated() throws SQLException {
        return getRowStat() == SQL_ROW_UPDATED;
    }

    private int getRowStat() throws SQLException {
        return ((Long) current.getObjectAs(
                context.getColumnInfo().realColumnCount(), Types.BIGINT)).intValue();
    }

    public boolean rowInserted() throws SQLException {
        return getRowStat() == SQL_ROW_ADDED;
    }

    public boolean rowDeleted() throws SQLException {
        return getRowStat() == SQL_ROW_DELETED;
    }

    public void cancelRowUpdates() throws SQLException {
        if (onInsertRow) {
            throw new SQLException("The cursor is on the insert row.");
        }

        refreshRow();
    }

    public void moveToInsertRow() throws SQLException {
        onInsertRow = true;
    }

    public void moveToCurrentRow() throws SQLException {
        onInsertRow = false;
    }

    public PacketRowResult currentRow() throws SQLException {
        if (onInsertRow) {
            if (insertRow == null) {
                insertRow = new PacketRowResult(context);
            }
            return insertRow;
        }

        if (current == null) {
            throw new SQLException("No current row in the ResultSet");
        }

        return current;
    }

    public void insertRow() throws SQLException {
        if (!onInsertRow) {
            throw new SQLException("The cursor is not on the insert row.");
        }

        // This might just happen if the ResultSet has no writeable column.
        // Very unlikely and very stupid, but who knows?
        if (insertRow == null) {
            insertRow = new PacketRowResult(context);
        }

        cursor(CURSOR_OP_INSERT, insertRow);
        // Update the number of rows and the cursor position
        cursorFetch(FETCH_INFO, 1);
        insertRow = new PacketRowResult(context);
    }

    public void updateRow() throws SQLException {
        if (current == null) {
            throw new SQLException("No current row in the ResultSet");
        }

        if (onInsertRow) {
            throw new SQLException("The cursor is on the insert row.");
        }

        cursor(CURSOR_OP_UPDATE, current);
        // Update the number of rows and the cursor position
        cursorFetch(FETCH_INFO, 1);
        refreshRow();
    }

    public void deleteRow() throws SQLException {
        if (current == null) {
            throw new SQLException("No current row in the ResultSet");
        }

        if (onInsertRow) {
            throw new SQLException("The cursor is on the insert row.");
        }

        cursor(CURSOR_OP_DELETE, null);
        cursorFetch(FETCH_REPEAT, 1);
    }

    public void refreshRow() throws SQLException {
        if (onInsertRow) {
            throw new SQLException("The cursor is on the insert row.");
        }

        cursorFetch(FETCH_REPEAT, 1);
    }

    private void cursorCreate(ParameterListItem[] procedureParams) throws SQLException {
        parameterList = new ParameterListItem[5];
        int scrollOpt, ccOpt;

        switch (type) {
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

        ParameterListItem param[] = parameterList;

        // Setup cursor handle param
        param[0] = new ParameterListItem();
        param[0].isSet = true;
        param[0].type = Types.INTEGER;
        param[0].isOutput = true;

        // Setup statement param
        param[1] = new ParameterListItem();
        param[1].isSet = true;
        param[1].type = Types.LONGVARCHAR;
        param[1].maxLength = Integer.MAX_VALUE;
        param[1].formalType =
                (conn.getTdsVer() >= TdsDefinitions.TDS70) ? "ntext" : "text";
        param[1].value = sql;

        // Setup scroll options
        param[2] = new ParameterListItem();
        param[2].isSet = true;
        param[2].type = Types.INTEGER;
        param[2].value = new Integer(scrollOpt);
        param[2].isOutput = true;

        // Setup concurrency options
        param[3] = new ParameterListItem();
        param[3].isSet = true;
        param[3].type = Types.INTEGER;
        param[3].value = new Integer(ccOpt);
        param[3].isOutput = true;

        // Setup numRows parameter
        param[4] = new ParameterListItem();
        param[4].isSet = true;
        param[4].type = Types.INTEGER;
        param[4].isOutput = true;

        // @todo Add return value handling for stored procedure calls
        if (procedureParams != null && procedureParams.length != 0) {
            StringBuffer buf = new StringBuffer(64);
            buf.append("exec ").append(sql).append(' ');
            for (int i = 0; ; ) {
                if (procedureParams[i].formalName != null) {
                    buf.append(procedureParams[i].formalName);
                } else {
                    buf.append("@P").append(i + 1);
                }
                // SAfe Doesn't actually work, because if the stored procedure
                //      does anything else than the SELECT (even a RETURN or
                //      SET) the sp_cursoropen will fail.
                /*if (procedureParams[i].isOutput) {
                    buf.append(" output");
                }*/
                if (++i == procedureParams.length) {
                    break;
                }
                buf.append(',');
            }
            param[1].value = buf.toString();
            param[2].value = new Integer(scrollOpt | 0x1000);

            param = new ParameterListItem[6 + procedureParams.length];
            System.arraycopy(parameterList, 0, param, 0, 5);

            buf.delete(0, buf.length());
            // Parameter declarations
            for (int i = 0; ; ) {
                if (procedureParams[i].formalName == null) {
                    buf.append("@P").append(i + 1);
                } else {
                    buf.append(procedureParams[i].formalName);
                }
                buf.append(' ').append(procedureParams[i].formalType);
                if (++i == procedureParams.length) {
                    break;
                }
                buf.append(',');
            }
            param[5] = new ParameterListItem();
            param[5].isSet = true;
            param[5].type = Types.LONGVARCHAR;
            param[5].maxLength = Integer.MAX_VALUE;
            // Use the same type as determined for the SQL query
            param[5].formalType = param[1].formalType;
            param[5].value = buf.toString();

            // Parameter values
            for (int i = 0; i < procedureParams.length; i++) {
                param[6 + i] = procedureParams[i];
            }
        }

        lastOutParam = -1;
        retVal = null;

        // SAfe We have to synchronize this, in order to avoid mixing up our
        //      actions with another CursorResultSet's and eating up its
        //      results.
        synchronized (conn.mainTdsMonitor) {
            Tds tds = conn.allocateTds(true);
            try {
                tds.executeProcedure("sp_cursoropen", param, param, internalStmt,
                                     warningChain, stmt.getQueryTimeout(), false);

                if (internalStmt.getMoreResults(tds, warningChain, true)) {
                    context = internalStmt.results.context;

                    // Hide rowstat column.
                    context.getColumnInfo().setFakeColumnCount(
                            context.getColumnInfo().realColumnCount() - 1);

                    internalStmt.results.close();
                } else {
                    warningChain.addException(new SQLException(
                            "Expected a ResultSet."));
                }

                // There are some TDS_DONEINPROC packets that need to be handled
                while (internalStmt.getMoreResults(tds, warningChain, true)
                        || (internalStmt.getUpdateCount() != -1));
            } catch (SQLException e) {
                warningChain.addException(e);
            } finally {
                try {
                    conn.freeTds(tds);
                } catch (TdsException e) {
                    warningChain.addException(TdsUtil.getSQLException(null, null, e));
                }

                warningChain.checkForExceptions();
            }
        }

        cursorHandle = (Integer) param[0].value;
        rowsInResult = ((Number) param[4].value).intValue();

        int actualScroll = ((Number) param[2].value).intValue();
        int actualCc = ((Number) param[3].value).intValue();

        if ((actualScroll != scrollOpt) || (actualCc != ccOpt)) {
            if (actualScroll != scrollOpt) {
                switch (actualScroll) {
                case CURSOR_TYPE_FORWARD:
                    type = TYPE_FORWARD_ONLY;
                    break;

                case CURSOR_TYPE_STATIC:
                    type = TYPE_SCROLL_INSENSITIVE;
                    break;

                case CURSOR_TYPE_DYNAMIC:
                case CURSOR_TYPE_KEYSET:
                    type = TYPE_SCROLL_SENSITIVE;
                    break;

                default:
                    warningChain.addException(new SQLException(
                            "Don't know how to handle cursor type "
                            + actualScroll));
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
                    warningChain.addException(new SQLException(
                            "Don't know how to handle concurrency type "
                            + actualScroll));
                }
            }

            // SAfe This warning goes to the Statement, not the ResultSet
            warningChain.addWarning(new SQLWarning(
                    "ResultSet type/concurrency downgraded."));
        }

        if ((retVal == null) || (retVal.intValue() != 0)) {
            warningChain.addException(
                    new SQLException("Cursor open failed."));
        }

        warningChain.checkForExceptions();
        open = true;
    }

    private boolean cursorFetch(int fetchType, int rowNum)
            throws SQLException {
        warningChain.clearWarnings();

        ParameterListItem param[] = new ParameterListItem[4];
        System.arraycopy(parameterList, 0, param, 0, 4);

        boolean isInfo = (fetchType == FETCH_INFO);

        if (fetchType != FETCH_ABSOLUTE && fetchType != FETCH_RELATIVE)
            rowNum = 1;

        // Setup cursor handle param
        param[0].clear();
        param[0].isSet = true;
        param[0].type = Types.INTEGER;
        param[0].value = cursorHandle;

        // Setup fetchtype param
        param[1].clear();
        param[1].isSet = true;
        param[1].type = Types.INTEGER;
        param[1].value = new Integer(fetchType);

        // Setup rownum
        param[2].clear();
        param[2].isSet = true;
        param[2].type = Types.INTEGER;
        param[2].value = isInfo ? null : new Integer(rowNum);
        param[2].isOutput = isInfo;

        // Setup numRows parameter
        param[3].clear();
        param[3].isSet = true;
        param[3].type = Types.INTEGER;
        param[3].value = isInfo ? null : new Integer(1);
        param[3].isOutput = isInfo;

        lastOutParam = -1;
        retVal = null;

        // SAfe We have to synchronize this, in order to avoid mixing up our
        //      actions with another CursorResultSet's and eating up its
        //      results.
        synchronized (conn.mainTdsMonitor) {
            Tds tds = conn.allocateTds(true);
            try {
                tds.executeProcedure("sp_cursorfetch", param, param, internalStmt,
                                     warningChain, stmt.getQueryTimeout(), true);

                current = tds.fetchRow(internalStmt, warningChain, context);

                // There are some TDS_DONEINPROC packets that need to be handled
                while (internalStmt.getMoreResults(tds, warningChain, true)
                        || (internalStmt.getUpdateCount() != -1));
            } catch (SQLException e) {
                warningChain.addException(e);
            } finally {
                try {
                    conn.freeTds(tds);
                } catch (TdsException e) {
                    warningChain.addException(TdsUtil.getSQLException(null, null, e));
                }

                warningChain.checkForExceptions();
            }
        }

        if (isInfo) {
            pos = ((Integer) param[2].value).intValue();
            rowsInResult = ((Integer) param[3].value).intValue();
        }

        return current != null;
    }

    private void cursorClose() throws SQLException {
        warningChain.clearWarnings();

        ParameterListItem param[] = new ParameterListItem[1];
        param[0] = parameterList[0];

        // Setup cursor handle param
        param[0].clear();
        param[0].isSet = true;
        param[0].type = Types.INTEGER;
        param[0].value = cursorHandle;

        lastOutParam = -1;
        retVal = null;

        // SAfe We have to synchronize this, in order to avoid mixing up our
        //      actions with another CursorResultSet's and eating up its
        //      results.
        synchronized (conn.mainTdsMonitor) {
            Tds tds = conn.allocateTds(true);
            try {
                tds.executeProcedure("sp_cursorclose", param, param, internalStmt,
                                     warningChain, stmt.getQueryTimeout(), false);

                // There are some TDS_DONEINPROC packets that need to be handled
                while (internalStmt.getMoreResults(tds, warningChain, true)
                        || (internalStmt.getUpdateCount() != -1));

                if ((retVal == null) || (retVal.intValue() != 0)) {
                    warningChain.addException(
                            new SQLException("Cursor close failed."));
                }
            } catch (SQLException e) {
                warningChain.addException(e);
            } finally {
                try {
                    conn.freeTds(tds);
                } catch (TdsException e) {
                    warningChain.addException(TdsUtil.getSQLException(null, null, e));
                }

                warningChain.checkForExceptions();
            }
        }
    }

    private void cursor(int opType, PacketRowResult row) throws SQLException {
        warningChain.clearWarnings();
        ParameterListItem param[];

        if (opType == CURSOR_OP_DELETE) {
            if (row != null) {
                throw new SQLException(
                        "Non-null row provided to delete operation.");
            }
            // 3 parameters for delete
            param = new ParameterListItem[3];
            System.arraycopy(parameterList, 0, param, 0, 3);
        } else {
            if (row == null) {
                throw new SQLException(
                        "Null row provided to insert/update operation.");
            }
            // 4 parameters plus one for each column for insert/update
            param = new ParameterListItem[4
                    + row.context.getColumnInfo().fakeColumnCount()];
            System.arraycopy(parameterList, 0, param, 0, 4);
        }

        // Setup cursor handle param
        param[0].clear();
        param[0].isSet = true;
        param[0].type = Types.INTEGER;
        param[0].value = cursorHandle;

        // Setup optype param
        param[1].clear();
        param[1].isSet = true;
        param[1].type = Types.INTEGER;
        param[1].value = new Integer(opType);

        // Setup rownum
        param[2].clear();
        param[2].isSet = true;
        param[2].type = Types.INTEGER;
        param[2].value = new Integer(1);

        // If row is not null, we're dealing with an insert/update
        if (row != null) {
            // Setup table
            param[3].clear();
            param[3].isSet = true;
            param[3].type = Types.VARCHAR;
            param[3].value = "";
            if (conn.getTdsVer() >= TdsDefinitions.TDS70) {
                param[3].formalType = "nvarchar(4000)";
                param[3].maxLength = 4000;
            } else {
                param[3].formalType = "varchar(255)";
                param[3].maxLength = 255;
            }

            Columns cols = row.context.getColumnInfo();
            int colCnt = cols.fakeColumnCount();
            // Current column; we should only update/insert columns
            // that are not read-only (such as identity columns)
            int crtCol = 4;
            for (int i=1; i <= colCnt; i++) {
                // Only send non-read-only columns
                if (!cols.isReadOnly(i).booleanValue()) {
                    param[crtCol] = new ParameterListItem();
                    param[crtCol].isSet = true;
                    param[crtCol].type = cols.getJdbcType(i);
                    param[crtCol].value = row.getObject(i);
                    param[crtCol].formalName = '@' + cols.getName(i);
                    param[crtCol].maxLength = cols.getBufferSize(i);
                    param[crtCol].scale = cols.getScale(i);

                    if (param[crtCol].value instanceof Blob) {
                        Blob blob = (Blob) param[crtCol].value;
                        long length = blob.length();

                        if (length > Integer.MAX_VALUE) {
                            throw new SQLException("Blob lengths greater than " + Integer.MAX_VALUE
                                + " are not suported.");
                        }

                        param[crtCol].value = blob.getBinaryStream();
                        param[crtCol].scale = (int) length;
                    } else if (param[crtCol].value instanceof Clob) {
                        Clob clob = (Clob) param[crtCol].value;
                        long length = clob.length();

                        if (length > Integer.MAX_VALUE) {
                            throw new SQLException("Clob lengths greater than " + Integer.MAX_VALUE
                                + " are not suported.");
                        }

                        param[crtCol].value = clob.getCharacterStream();
                        param[crtCol].scale = (int) length;
                    }

                    // This is only for Tds.executeProcedureInternal to
                    // know that it's a Unicode column
                    switch (cols.getNativeType(i)) {
                    case TdsDefinitions.SYBBIGNVARCHAR:
                    case TdsDefinitions.SYBNVARCHAR:
                    case TdsDefinitions.SYBNCHAR:
                        param[crtCol].formalType = "nvarchar(4000)";
                        break;

                    case TdsDefinitions.SYBNTEXT:
                        param[crtCol].formalType = "ntext";
                        break;
                    }
                    crtCol++;
                } else {
                    if (opType == CURSOR_OP_INSERT && row.getObject(i) != null)
                        throw new SQLException(
                                "Column " + i + "/" + cols.getName(i)
                                + " is read-only.");
                }
            }

            // If the count is different (i.e. there were read-only
            // columns) reallocate the parameters into a shorter array
            if (crtCol != colCnt+4) {
                ParameterListItem[] newParam =
                        new ParameterListItem[crtCol];
                System.arraycopy(param, 0, newParam, 0, crtCol);
                param = newParam;
            }
        }

        lastOutParam = -1;
        retVal = null;

        // SAfe We have to synchronize this, in order to avoid mixing up our
        //      actions with another CursorResultSet's and eating up its
        //      results.
        synchronized (conn.mainTdsMonitor) {
            Tds tds = conn.allocateTds(true);
            try {
                tds.executeProcedure("sp_cursor", param, param, internalStmt,
                                     warningChain, stmt.getQueryTimeout(), false);

                // There are some TDS_DONEINPROC packets that need to be handled
                while (internalStmt.getMoreResults(tds, warningChain, true)
                        || (internalStmt.getUpdateCount() != -1));

                if ((retVal == null) || (retVal.intValue() != 0)) {
                    warningChain.addException(
                            new SQLException("Cursor operation failed."));
                }
            } catch (SQLException e) {
                warningChain.addException(e);
            } finally {
                try {
                    conn.freeTds(tds);
                } catch (TdsException e) {
                    warningChain.addException(TdsUtil.getSQLException(null, null, e));
                }

                warningChain.checkForExceptions();
            }
        }
    }

    /**
     * Handle a return status.
     *
     * @param packet a RetStat packet
     */
    public boolean handleRetStat(PacketRetStatResult packet) {
        retVal = new Integer(packet.getRetStat());
        return true;
    }

    /**
     * Handle an output parameter.
     *
     * @param packet        an OutputParam packet
     * @throws SQLException if there is a handling exception
     */
    public boolean handleParamResult(PacketOutputParamResult packet)
            throws SQLException {
        for (lastOutParam++; lastOutParam < parameterList.length; lastOutParam++)
            if (parameterList[lastOutParam].isOutput) {
                parameterList[lastOutParam].value = packet.value;
                return true;
            }

        throw new SQLException("More output params than expected.");
    }
}
