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

    /**
     * The row is unchanged.
     */
    public static final int SQL_ROW_SUCCESS = 0;

    /**
     * The row has been deleted.
     */
    public static final int SQL_ROW_DELETED = 1;

    /**
     * The row has been updated.
     */
    public static final int SQL_ROW_UPDATED = 2;

    /**
     * There is no row that corresponds this position.
     */
    public static final int SQL_ROW_NOROW = 3;

    /**
     * The row has been added.
     */
    public static final int SQL_ROW_ADDED = 4;

    /**
     * The row is unretrievable due to an error.
     */
    public static final int SQL_ROW_ERROR = 5;

    private static final int POS_BEFORE_FIRST = -100000;
    private static final int POS_AFTER_LAST = -200000;

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

    public Context getContext() {
        return context;
    }

    public CursorResultSet(TdsStatement stmt, String sql, int fetchDir)
            throws SQLException {
        this.stmt = stmt;
        this.conn = (TdsConnection) stmt.getConnection();
        this.sql = sql;
        this.direction = fetchDir == FETCH_UNKNOWN ? FETCH_FORWARD : fetchDir;

        this.type = stmt.getResultSetType();
        this.concurrency = stmt.getResultSetConcurrency();

        // SAfe Until we're actually created use the Statement's warning chain.
        warningChain = stmt.warningChain;

        cursorCreate();

        // SAfe Now we can create our own warning chain.
        warningChain = new SQLWarningChain();
    }

    protected void finalize() {
        try {
            close();
        } catch (SQLException ex) {
        }
    }

    public void setFetchDirection(int direction) throws SQLException {
        switch (direction) {
        case FETCH_UNKNOWN:
        case FETCH_REVERSE:
            if (type != ResultSet.TYPE_FORWARD_ONLY) {
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
        return pos == POS_BEFORE_FIRST;
    }

    public boolean isAfterLast() throws SQLException {
        return pos == POS_AFTER_LAST;
    }

    public boolean isFirst() throws SQLException {
        return pos == 1;
    }

    public boolean isLast() throws SQLException {
        throw new java.lang.UnsupportedOperationException(
                "Method isLast() not yet implemented.");
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
                cursorClose();
            } finally {
                open = false;
                stmt = null;
                conn = null;
            }
        }

        warningChain.checkForExceptions();
    }

    public SQLWarning getWarnings() throws SQLException {
        return warningChain.getWarnings();
    }

    public boolean next() throws SQLException {
        if (cursorFetch(FETCH_NEXT, 0)) {
            pos += (pos < 0) ? (1-pos) : 1;
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
        return (int) current.getLong(
                context.getColumnInfo().realColumnCount());
    }

    public boolean rowInserted() throws SQLException {
        return getRowStat() == this.SQL_ROW_ADDED;
    }

    public boolean rowDeleted() throws SQLException {
        return getRowStat() == SQL_ROW_DELETED;
    }

    public void cancelRowUpdates() throws SQLException {
        throw new java.lang.UnsupportedOperationException(
                "Method cancelRowUpdates() not yet implemented.");
    }

    public void moveToInsertRow() throws SQLException {
        throw new java.lang.UnsupportedOperationException(
                "Method moveToInsertRow() not yet implemented.");
    }

    public void moveToCurrentRow() throws SQLException {
        throw new java.lang.UnsupportedOperationException(
                "Method moveToCurrentRow() not yet implemented.");
    }

    public PacketRowResult currentRow() {
        return current;
    }

    public void insertRow() {
        throw new java.lang.UnsupportedOperationException(
                "Method moveToInsertRow() not yet implemented.");
    }

    public void updateRow() throws SQLException {
        throw new SQLException("ResultSet is not updateable");
    }

    public void deleteRow() throws SQLException {
        throw new SQLException("ResultSet is not updateable");
    }

    public void refreshRow() throws SQLException {
        throw new SQLException("ResultSet is not updateable");
    }

    private void cursorCreate() throws SQLException {
        // SAfe We have to synchronize this, in order to avoid mixing up our
        //      actions with another CursorResultSet's and eating up its
        //      results.
        synchronized (conn.mainTdsMonitor) {
            Tds tds = conn.allocateTds(true);
            try {
                warningChain.clearWarnings();
                parameterList = new ParameterListItem[5];
                int scrollOpt, ccOpt;

                switch (type) {
                case TYPE_SCROLL_INSENSITIVE:
                    scrollOpt = 8; // Static cursor
                    break;

                case TYPE_SCROLL_SENSITIVE:
                    scrollOpt = 1; // Keyset-driven
                    break;

                case TYPE_FORWARD_ONLY:
                default:
                    scrollOpt = 4; // Forward-only
                    break;
                }

                switch (concurrency) {
                case CONCUR_READ_ONLY:
                default:
                    ccOpt = 1; // Read-only
                    break;

                case CONCUR_UPDATABLE:
                    ccOpt = 2; // Scroll locks
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
                param[1].maxLength = 8001;
                param[1].formalType = "ntext";
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

                stmt.outParamHandler = this;
                lastOutParam = -1;
                retVal = null;

                tds.executeProcedure("sp_cursoropen", param, param, stmt,
                                     warningChain, stmt.getQueryTimeout(), false);

                // @todo Should maybe use a different statement here
                if (stmt.getMoreResults(tds, warningChain, true)) {
                    context = stmt.results.context;

                    // Hide rowstat column.
                    context.getColumnInfo().setFakeColumnCount(
                            context.getColumnInfo().realColumnCount() - 1);

                    stmt.results.close();
                } else {
                    warningChain.addException(new SQLException(
                            "Expected a ResultSet."));
                }

                if (stmt.getMoreResults(tds, warningChain, true)
                        || (stmt.getUpdateCount() != -1)) {
                    warningChain.addException(new SQLException(
                            "No more results expected."));
                }

                cursorHandle = (Integer) param[0].value;
                rowsInResult = ((Number) param[4].value).intValue();

                int actualScroll = ((Number) param[2].value).intValue();
                int actualCc = ((Number) param[3].value).intValue();

                if ((actualScroll != scrollOpt) || (actualCc != ccOpt)) {
                    // @todo Check the returned scroll and concurrency values
                    warningChain.addWarning(new SQLWarning(
                            "ResultSet type/concurrency downgraded."));
                }

                if ((retVal == null) || (retVal.intValue() != 0)) {
                    warningChain.addException(
                            new SQLException("Cursor open failed."));
                }
            } finally {
                try {
                    conn.freeTds(tds);
                } catch (TdsException ex) {
                    warningChain.addException(
                            new SQLException(ex.getMessage()));
                }

                lastOutParam = -1;
                retVal = null;
                stmt.outParamHandler = null;
            }
        }

        warningChain.checkForExceptions();
        open = true;
    }

    private boolean cursorFetch(int fetchType, int rowNum)
            throws SQLException {
        // SAfe We have to synchronize this, in order to avoid mixing up our
        //      actions with another CursorResultSet's and eating up its
        //      results.
        synchronized (conn.mainTdsMonitor) {
            Tds tds = conn.allocateTds(true);
            try {
                warningChain.clearWarnings();

                ParameterListItem param[] = new ParameterListItem[4];
                System.arraycopy(parameterList, 0, param, 0, 4);

                boolean isInfo = (fetchType == FETCH_INFO);

                if (fetchType != FETCH_ABSOLUTE && fetchType != FETCH_RELATIVE)
                    rowNum = 1;

                // Setup cursor handle param
                param[0] = new ParameterListItem();
                param[0].isSet = true;
                param[0].type = Types.INTEGER;
                param[0].value = cursorHandle;

                // Setup fetchtype param
                param[1] = new ParameterListItem();
                param[1].isSet = true;
                param[1].type = Types.INTEGER;
                param[1].value = new Integer(fetchType);

                // Setup rownum
                param[2] = new ParameterListItem();
                param[2].isSet = true;
                param[2].type = Types.INTEGER;
                param[2].value = isInfo ? null : new Integer(rowNum);
                param[2].isOutput = isInfo;

                // Setup numRows parameter
                param[3] = new ParameterListItem();
                param[3].isSet = true;
                param[3].type = Types.INTEGER;
                param[3].value = isInfo ? null : new Integer(1);
                param[3].isOutput = isInfo;

                stmt.outParamHandler = this;
                lastOutParam = -1;
                retVal = null;

                tds.executeProcedure("sp_cursorfetch", param, param, stmt,
                                     warningChain, stmt.getQueryTimeout(), true);

                current = tds.fetchRow(stmt, warningChain, context);

                // @todo Should maybe use a different statement here
                if (stmt.getMoreResults(tds, warningChain, true)
                        || (stmt.getUpdateCount() != -1)) {
                    warningChain.addException(new SQLException(
                            "No more results expected."));
                }

//                System.out.println("sp_cursorfetch " +
//                                   param[0].value + " " +
//                                   param[1].value + " " +
//                                   param[2].value + " " +
//                                   param[3].value);

                if ((retVal == null) || (retVal.intValue() != 0)) {
                    warningChain.addException(
                            new SQLException("Cursor open failed."));
                }
            } finally {
                try {
                    conn.freeTds(tds);
                } catch (TdsException ex) {
                    warningChain.addException(
                            new SQLException(ex.getMessage()));
                }

                lastOutParam = -1;
                retVal = null;
                stmt.outParamHandler = null;
            }
        }

        warningChain.checkForExceptions();
        return current != null;
    }

    private boolean cursorClose() throws SQLException {
        // SAfe We have to synchronize this, in order to avoid mixing up our
        //      actions with another CursorResultSet's and eating up its
        //      results.
        synchronized (conn.mainTdsMonitor) {
            Tds tds = conn.allocateTds(true);
            try {
                warningChain.clearWarnings();

                ParameterListItem param[] = new ParameterListItem[1];
                param[0] = parameterList[0];

                // Setup cursor handle param
                param[0] = new ParameterListItem();
                param[0].isSet = true;
                param[0].type = Types.INTEGER;
                param[0].value = cursorHandle;

                stmt.outParamHandler = this;
                lastOutParam = -1;
                retVal = null;

                tds.executeProcedure("sp_cursorclose", param, param, stmt,
                                     warningChain, stmt.getQueryTimeout(), false);

                // @todo Should maybe use a different statement here
                if (stmt.getMoreResults(tds, warningChain, true)
                        || (stmt.getUpdateCount() != -1)) {
                    warningChain.addException(new SQLException(
                            "No more results expected."));
                }

//                System.out.println("sp_cursorclose " +
//                                   param[0].value);

                if ((retVal == null) || (retVal.intValue() != 0)) {
                    warningChain.addException(
                            new SQLException("Cursor open failed."));
                }
            } finally {
                try {
                    conn.freeTds(tds);
                } catch (TdsException ex) {
                    warningChain.addException(
                            new SQLException(ex.getMessage()));
                }

                lastOutParam = -1;
                retVal = null;
                stmt.outParamHandler = null;
            }
        }

        warningChain.checkForExceptions();
        return current != null;
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
