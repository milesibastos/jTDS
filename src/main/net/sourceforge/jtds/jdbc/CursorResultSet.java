package net.sourceforge.jtds.jdbc;

import java.sql.*;

/**
 * @created    March 16, 2001
 * @version    1.0
 */
public class CursorResultSet extends AbstractResultSet
{
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
    public static final int SQL_ROW_ADDED  = 4;

     /**
      * The row is unretrievable due to an error.
      */
    public static final int SQL_ROW_ERROR = 5;

    private static final int POS_BEFORE_FIRST = -1;
    private static final int POS_AFTER_LAST = -2;
    private static final int POS_LAST = -3;

    private int pos = POS_BEFORE_FIRST;

    private String cursorName;

    private String sql;
    private TdsStatement stmt;
    private TdsConnection conn;

    private int direction = FETCH_FORWARD;

    private boolean open = false;

    private Context context;

    private static int count = 0;

    private PacketRowResult current;

    /**
     * Load a context so that we can return a metaData before
     * the first call of next().
     */
    private void loadContext() throws SQLException
    {
        if ( current == null ) {
            internalFetch("FETCH PRIOR FROM " + cursorName);
            // context = current.getContext();  Moved to internalFetch -- if no
            //            results are returned a NullPointerException is thrown
        }
    }

    public Context getContext()
    {
        return context;
    }

    public CursorResultSet(TdsStatement stmt, String sql, int fetchDir)
             throws SQLException
    {
        this.stmt = stmt;
        this.conn = (TdsConnection)stmt.getConnection();
        this.sql = sql;
        this.direction = fetchDir==FETCH_UNKNOWN ? FETCH_FORWARD : fetchDir;
        warningChain = new SQLWarningChain();

        createCursor();
        loadContext();
    }

    protected void finalize()
    {
        try
        {
            close();
        }
        catch( SQLException ex )
        {
        }
    }

    public void setFetchDirection(int direction) throws SQLException
    {
        this.direction = direction;
    }

    public void setFetchSize(int rows) throws SQLException
    {
    }

    public String getCursorName() throws SQLException
    {
        return cursorName;
    }

    public boolean isBeforeFirst() throws SQLException
    {
        return pos == POS_BEFORE_FIRST;
    }

    public boolean isAfterLast() throws SQLException
    {
        return pos == POS_AFTER_LAST;
    }

    public boolean isFirst() throws SQLException
    {
        return pos == 1;
    }

    public boolean isLast() throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Method isLast() not yet implemented.");
    }

    public int getRow() throws SQLException
    {
        return pos;
    }

    public int getFetchDirection() throws SQLException
    {
        return direction;
    }

    public int getFetchSize() throws SQLException
    {
        return 1;
    }

    public int getType() throws SQLException
    {
        return stmt.getResultSetType();
    }

    public int getConcurrency() throws SQLException
    {
        return stmt.getResultSetConcurrency();
    }

    public Statement getStatement() throws SQLException
    {
        return stmt;
    }

    public void close() throws SQLException
    {
        if( open )
        {
            synchronized( conn.mainTdsMonitor )
            {
                Tds tds = conn.allocateTds(true);
                try
                {
                    stmt.internalExecute("CLOSE " + cursorName, tds, warningChain);
                    stmt.internalExecute("DEALLOCATE " + cursorName, tds, warningChain);
                    open = false;
                }
                finally
                {
                    stmt.skipToEnd();
                    try{ conn.freeTds(tds); } catch( TdsException ex ){ throw new SQLException(ex.getMessage()); }
                }
            }
        }

        stmt = null;
        conn = null;
    }

    public SQLWarning getWarnings() throws SQLException
    {
        return warningChain.getWarnings();
    }

    public boolean next() throws SQLException
    {
        // SAfe Metadata is now obtained through calling "FETCH PRIOR", so no
        //      data is really prefetched. We're before the first result.
        //      Thanks go to wolfgang_a_h@gmx.at for discovering the bug.
//        if ( pos == POS_BEFORE_FIRST ) {
//            pos = 1;
//            return true;
//        }
        if( direction == ResultSet.FETCH_REVERSE )
        {
            pos -= 1;
            if( internalFetch("FETCH PRIOR FROM " + cursorName) )
                return true;
            else
            {
                pos = POS_BEFORE_FIRST;
                return false;
            }
        }
        else
        {
            pos += 1;
            if( internalFetch("FETCH NEXT FROM " + cursorName) )
                return true;
            else
            {
                pos = POS_AFTER_LAST;
                return false;
            }
        }
    }

    private boolean internalFetch( String sql )
        throws SQLException
    {
        TdsResultSet rs = null;

        synchronized( conn.mainTdsMonitor )
        {
            Tds tds = conn.allocateTds(true);
            try
            {
                if( !stmt.internalExecute(sql, tds, warningChain) )
                    throw new SQLException("No ResultSet was produced.");

                rs = (TdsResultSet)stmt.getResultSet();

                // Moved here from loadContext -- if the query didn't return any
                //                                results, loadContext crashed
                if( context == null )
                    context = rs.getContext();

                current = rs.next() ? rs.currentRow() : null;
                warningChain.addWarning(rs.getWarnings());
                rs.close();
            }
            finally
            {
                stmt.skipToEnd();
                try{ conn.freeTds(tds); } catch( TdsException ex ){ throw new SQLException(ex.getMessage()); }
            }
        }

        // Hide rowstat column.
        context.getColumnInfo().setFakeColumnCount(
            context.getColumnInfo().realColumnCount() - 1);

        return current != null;
    }

    public void clearWarnings() throws SQLException
    {
        warningChain.clearWarnings();
    }

    public void beforeFirst() throws SQLException
    {
        if( pos != POS_BEFORE_FIRST )
        {
            pos = POS_BEFORE_FIRST;
            internalFetch("FETCH FIRST FROM "+cursorName);
            internalFetch("FETCH PRIOR FROM "+cursorName);
        }
    }

    public void afterLast() throws SQLException
    {
        if( pos != POS_AFTER_LAST )
        {
            pos = POS_AFTER_LAST;
            internalFetch("FETCH LAST FROM "+cursorName);
            internalFetch("FETCH NEXT FROM "+cursorName);
        }
    }

    public boolean first()
             throws SQLException
    {
        pos = 1;
        return internalFetch( "FETCH FIRST FROM " + cursorName );
    }

    public boolean last()
             throws SQLException
    {
        pos = POS_LAST;
        return internalFetch( "FETCH LAST FROM " + cursorName);
    }

    public boolean absolute( int row )
             throws SQLException
    {
        pos = row;
        return internalFetch( "FETCH ABSOLUTE " + row + " FROM " + cursorName);
    }

    public boolean relative(int rows) throws SQLException
    {
        //We have prefetched the first row so we need to adjust the
        //relative setting
        // SAfe No longer true. We no longer prefetch the first row, we only
        //      get the header with "FETCH PRIOR".
//        if ( pos == POS_BEFORE_FIRST ) {
//            rows = rows - 1;
//        }
        if ( direction == ResultSet.FETCH_REVERSE ) {
            rows = -rows;
        }
        pos += rows;
        return internalFetch( "FETCH RELATIVE " + rows + " FROM " + cursorName);
    }

    public boolean previous() throws SQLException
    {
        if ( direction == ResultSet.FETCH_REVERSE ) {
            pos += 1;
            if( internalFetch("FETCH NEXT FROM " + cursorName) )
                return true;
            else
            {
                pos = POS_AFTER_LAST;
                return false;
            }
        }
        else {
            pos -= 1;
            if( internalFetch("FETCH PRIOR FROM " + cursorName) )
                return true;
            else
            {
                pos = POS_BEFORE_FIRST;
                return false;
            }
        }
    }

    public boolean rowUpdated() throws SQLException
    {
        return getRowStat() == SQL_ROW_UPDATED;
    }

    private int getRowStat() throws SQLException
    {
        return (int)current.getLong(getColumnInfo().realColumnCount());
    }

    public boolean rowInserted() throws SQLException
    {
        return getRowStat() == this.SQL_ROW_ADDED;
    }

    public boolean rowDeleted() throws SQLException
    {
        return getRowStat() == SQL_ROW_DELETED;
    }

    public void cancelRowUpdates() throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Method cancelRowUpdates() not yet implemented.");
    }

    public void moveToInsertRow() throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Method moveToInsertRow() not yet implemented.");
    }

    public void moveToCurrentRow() throws SQLException
    {
        throw new java.lang.UnsupportedOperationException("Method moveToCurrentRow() not yet implemented.");
    }

    private void createCursor() throws SQLException
    {
        cursorName = getNewCursorName();
        StringBuffer query = new StringBuffer(sql.length()+50);

        // SAfe It seems we can only use the Transact-SQL extended syntax with SQL Server 7.0+, so we will have to use
        //      the SQL-92 syntax for older versions (see https://sourceforge.net/forum/message.php?msg_id=1738464).
        if( conn.getTdsVer() >= Tds.TDS70 )
        {
            // Transact-SQL Extended Syntax
            query.append("DECLARE ").append(cursorName).append(" CURSOR ");

            if( stmt.getResultSetType()==ResultSet.TYPE_FORWARD_ONLY &&
                stmt.getResultSetConcurrency()==ResultSet.CONCUR_READ_ONLY )
            {
                query.append("FAST_FORWARD ");
            }
            else
            {
                if( stmt.getResultSetType() == ResultSet.TYPE_FORWARD_ONLY )
                    query.append("FORWARD_ONLY ");
                else if( stmt.getResultSetType() == ResultSet.TYPE_SCROLL_INSENSITIVE )
                    query.append("SCROLL STATIC ");
                else if( stmt.getResultSetType() == ResultSet.TYPE_SCROLL_SENSITIVE )
                    query.append("SCROLL KEYSET ");

                if( stmt.getResultSetConcurrency() == ResultSet.CONCUR_READ_ONLY )
                    query.append("READ_ONLY ");
                else
                    query.append("OPTIMISTIC ");
            }

            query.append("FOR ").append(sql);
        }
        else
        {
            // Standard SQL-92 syntax
            query.append("DECLARE ").append(cursorName);

            if( stmt.getResultSetType() != ResultSet.TYPE_SCROLL_SENSITIVE )
                query.append(" INSENSITIVE");
            if( stmt.getResultSetType() != ResultSet.TYPE_FORWARD_ONLY )
                query.append(" SCROLL");

            query.append(" CURSOR FOR ").append(sql).append(" FOR ");

            if( stmt.getResultSetConcurrency() == ResultSet.CONCUR_READ_ONLY )
                query.append("READ ONLY");
            else
                query.append("UPDATE");
        }

        // SAfe We have to synchronize this, in order to avoid mixing up our
        //      actions with another CursorResultSet's and eating up its
        //      results.
        synchronized( conn.mainTdsMonitor )
        {
            Tds tds = conn.allocateTds(true);
            try
            {
                stmt.internalExecute(query.toString(), tds, warningChain);
                stmt.internalExecute("OPEN " + cursorName, tds, warningChain);
            }
            finally
            {
                stmt.skipToEnd();
                try{ conn.freeTds(tds); } catch( TdsException ex ){ throw new SQLException(ex.getMessage()); }
            }
        }
        open = true;
    }

    private static synchronized String getNewCursorName()
    {
        count++;
        return "cursor_" + count;
    }

    public PacketRowResult currentRow()
    {
        return current;
    }

    public void insertRow()
    {
        throw new java.lang.UnsupportedOperationException("Method moveToInsertRow() not yet implemented.");
    }

    public void updateRow() throws SQLException
    {
        throw new SQLException("ResultSet is not updateable");
    }

    public void deleteRow() throws SQLException
    {
        throw new SQLException("ResultSet is not updateable");
    }

    public void refreshRow() throws SQLException
    {
        throw new SQLException("ResultSet is not updateable");
    }

    private Columns getColumnInfo()
    {
        return getContext().getColumnInfo();
    }
}
