package freetds;

import java.sql.*;

/**
 * @created    March 16, 2001
 * @version    1.0
 */

public class ScrollableResultSet {

    String cursorName = "test";

    ScrollableStatement stmt;
    ResultSet current;

    String sql;
    boolean open = false;

    public ScrollableResultSet(ScrollableStatement stmt, String sql)
             throws SQLException
    {
        this.stmt = stmt;
        this.sql = sql;

        createCursor();

        open = true;
    }


    public void first()
             throws SQLException
    {
        closeCurrent();
        current = stmt.stmt.executeQuery("FETCH FIRST FROM " + cursorName);
        current.next();
    }


    public void last()
             throws SQLException
    {
        closeCurrent();
        current = stmt.stmt.executeQuery("FETCH LAST FROM " + cursorName);
        current.next();
    }

    public void absolute( int row )
             throws SQLException
    {
        closeCurrent();
        current = stmt.stmt.executeQuery("FETCH ABSOLUTE " + row + " FROM " + cursorName);
        current.next();
    }


    public void close()
             throws SQLException
    {
        if (open) {
            stmt.stmt.execute("CLOSE " + cursorName);
            stmt.stmt.execute("DEALLOCATE " + cursorName);
            open = false;
        }
    }


    protected void finalize()
    {
        try {
            close();
        }
        catch (SQLException ex) {
        }
    }


    private void createCursor()
             throws SQLException
    {
        cursorName = "test";
        stmt.stmt.execute("DECLARE " + cursorName + " SCROLL CURSOR FOR " + sql);
        stmt.stmt.execute("OPEN " + cursorName);
        open = true;
    }


    private void closeCurrent()
    throws SQLException
    {
        if (current != null) {
            current.close();
            current = null;
        }
    }
}
