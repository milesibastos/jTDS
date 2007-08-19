package net.sourceforge.jtds.test;

import java.sql.*;

/**
 * Some simple tests just to make sure everything is working properly.
 *
 * @created    9 August 2001
 * @version    1.0
 */

public class SanityTest extends TestBase {
    public SanityTest(String name) {
        super(name);
    }

    /**
     * A simple test to make sure everything seems to be OK
     */
    public void testSanity() throws Exception {
        Statement stmt = con.createStatement();
        makeTestTables(stmt);
        makeObjects(stmt, 5);
        stmt.close();
    }

    /**
     * Basic test of cursor mechanisms.
     */
    public void testCursorStatements() throws Exception {
        Statement stmt = con.createStatement();
        makeTestTables(stmt);
        makeObjects(stmt, 5);

        ResultSet rs;

        assertEquals("Expected an update count", false,
                     stmt.execute( "DECLARE cursor1 SCROLL CURSOR FOR"
                                   + "\nSELECT * FROM #test"));

        assertEquals("Expected an update count", false,
                     stmt.execute("OPEN cursor1"));

        rs = stmt.executeQuery("FETCH LAST FROM cursor1");
        dump(rs);
        rs.close();

        rs = stmt.executeQuery("FETCH FIRST FROM cursor1");
        dump(rs);
        rs.close();

        stmt.execute("CLOSE cursor1");
        stmt.execute("DEALLOCATE cursor1");
        stmt.close();
    }

    public void testCursorRSCreate() throws Exception {
        Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                             ResultSet.CONCUR_READ_ONLY);

        makeTestTables(stmt);
        makeObjects(stmt, 5);

        ResultSet rs = stmt.executeQuery("Select * from #test order by 1");

        // Move to last row (5)
        rs.last();
        assertEquals(5, rs.getRow());
        assertEquals(4, rs.getInt(1));
        assertEquals(false, rs.isBeforeFirst());
        assertEquals(false, rs.isFirst());
        assertEquals(true, rs.isLast());
        assertEquals(false, rs.isAfterLast());

        // Move before first row
        rs.beforeFirst();
        assertEquals(0, rs.getRow());

        try {
            rs.getInt(1);
            fail("There should be no current row.");
        } catch (SQLException ex) {
            // This is ok, there's no row
        }

        assertEquals(true, rs.isBeforeFirst());
        assertEquals(false, rs.isFirst());
        assertEquals(false, rs.isLast());
        assertEquals(false, rs.isAfterLast());

        // Try to move 3 rows ahead (should end up on row 3 -- the spec,
        // says that relative(1) is identical to next() and relative(-1) is
        // identical to previous(), but the Javadoc says that they are
        // different). Weird stuff...
        rs.relative(3);
        assertEquals(3, rs.getRow());
        assertEquals(2, rs.getInt(1));
        assertEquals(false, rs.isBeforeFirst());
        assertEquals(false, rs.isFirst());
        assertEquals(false, rs.isLast());
        assertEquals(false, rs.isAfterLast());

        // Move after last row
        rs.afterLast();
        assertEquals(0, rs.getRow());

        try {
            rs.getInt(1);
            fail("There should be no current row.");
        } catch (SQLException ex) {
            // This is ok, there's no row
        }

        assertEquals(false, rs.isBeforeFirst());
        assertEquals(false, rs.isFirst());
        assertEquals(false, rs.isLast());
        assertEquals(true, rs.isAfterLast());

        // Move to first row
        rs.first();
        assertEquals(1, rs.getRow());
        assertEquals(0, rs.getInt(1));
        assertEquals(false, rs.isBeforeFirst());
        assertEquals(true, rs.isFirst());
        assertEquals(false, rs.isLast());
        assertEquals(false, rs.isAfterLast());

        // Move to row 4
        rs.absolute(4);
        assertEquals(4, rs.getRow());
        assertEquals(3, rs.getInt(1));
        assertEquals(false, rs.isBeforeFirst());
        assertEquals(false, rs.isFirst());
        assertEquals(false, rs.isLast());
        assertEquals(false, rs.isAfterLast());

        // Move 2 rows back
        rs.relative(-2);
        assertEquals(2, rs.getRow());
        assertEquals(1, rs.getInt(1));
        assertEquals(false, rs.isBeforeFirst());
        assertEquals(false, rs.isFirst());
        assertEquals(false, rs.isLast());
        assertEquals(false, rs.isAfterLast());

        rs.close();
        stmt.close();
    }

    public void testCursorRSScroll() throws Exception {
        Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                             ResultSet.CONCUR_READ_ONLY);

        makeTestTables(stmt);
        makeObjects(stmt, 5);

        ResultSet rs = stmt.executeQuery("Select * from #test");

        while (rs.next());

        rs.close();
        stmt.close();
    }

    /*
     * Check that image fields that have once been set to a non
     * null value return null when updated to null.
     * Fix bug [1774322] Sybase nulled text fields return not null.
     */
    public void testNullImage() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #TEST (id int primary key not null, img image null)");
        stmt.executeUpdate("INSERT INTO #TEST VALUES (1, null)");
        ResultSet rs = stmt.executeQuery("SELECT * FROM #TEST");
        rs.next();
        assertTrue(rs.getBytes(2) == null);
        stmt.executeUpdate("UPDATE #TEST SET img = '0x0123' WHERE id = 1");
        rs = stmt.executeQuery("SELECT * FROM #TEST");
        rs.next();
        assertTrue(rs.getBytes(2) != null);
        stmt.executeUpdate("UPDATE #TEST SET img = null WHERE id = 1");
        rs = stmt.executeQuery("SELECT * FROM #TEST");
        rs.next();
        assertTrue(rs.getBytes(2) == null);
        stmt.close();      
    }

    /*
     * Check that text fields that have once been set to a non
     * null value return null when updated to null.
     * Fix bug [1774322] Sybase nulled text fields return not null.
     */
    public void testNullText() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #TEST (id int primary key not null, txt text null)");
        stmt.executeUpdate("INSERT INTO #TEST VALUES (1, null)");
        ResultSet rs = stmt.executeQuery("SELECT * FROM #TEST");
        rs.next();
        assertTrue(rs.getString(2) == null);
        stmt.executeUpdate("UPDATE #TEST SET txt = ' ' WHERE id = 1");
        rs = stmt.executeQuery("SELECT * FROM #TEST");
        rs.next();
        assertTrue(rs.getString(2) != null);
        stmt.executeUpdate("UPDATE #TEST SET txt = null WHERE id = 1");
        rs = stmt.executeQuery("SELECT * FROM #TEST");
        rs.next();
        assertTrue(rs.getString(2) == null);
        stmt.close();      
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(SanityTest.class);
    }
}
