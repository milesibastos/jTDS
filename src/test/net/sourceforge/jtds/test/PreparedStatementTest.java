package net.sourceforge.jtds.test;

import java.sql.*;

/**
 * @version 1.0
 */

public class PreparedStatementTest
extends TestBase
{

    public PreparedStatementTest( String name )
    {
        super(name);
    }

    public void testPreparedStatement()
        throws Exception
    {

        PreparedStatement pstmt
            = con.prepareStatement( "SELECT * FROM #test" );

        makeTestTables( pstmt );
        makeObjects( pstmt, 10 );

        ResultSet rs = pstmt.executeQuery();

        dump( rs );

        rs.close();

    }

    public void testScrollablePreparedStatement()
        throws Exception
    {
        Statement stmt = con.createStatement();
        makeTestTables( stmt );
        makeObjects( stmt, 10 );
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement(
            "SELECT * FROM #test",
            ResultSet.TYPE_SCROLL_SENSITIVE,
            ResultSet.CONCUR_READ_ONLY );

        ResultSet rs = pstmt.executeQuery();

        assertTrue( rs.isBeforeFirst() );
        while ( rs.next() ) {}
        assertTrue( rs.isAfterLast() );

        //This currently fails because the PreparedStatement
        //Doesn't know it needs to create a cursored ResultSet.
        //Needs some refactoring!!
        // SAfe Not any longer. ;o)
        while ( rs.previous() ) {}
        assertTrue( rs.isBeforeFirst() );

        rs.close();

    }

    public void testPreparedStatementAddBatch1()
        throws Exception {
        int count = 50;

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #psbatch1 (f_int INT)");
        stmt.close();

        int sum = 0;

        con.setAutoCommit(false);
        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #psbatch1 (f_int) VALUES (?)");

        for (int i = 0; i < count; i++) {
            pstmt.setInt(1, i);
            pstmt.addBatch();
            sum += i;
        }

        int[] results = pstmt.executeBatch();

        assertTrue(results.length == count);

        for (int i = 0; i < count; i++) {
            assertTrue(results[i] == 1);
        }

        pstmt.close();

        con.commit();
        con.setAutoCommit(true);

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT SUM(f_int) FROM #psbatch1");

        assertTrue(rs.next());
        System.out.println(rs.getInt(1));
        assertTrue(rs.getInt(1) == sum);
        stmt2.close();
        rs.close();
    }

    /**
     * Test for [924030] EscapeProcesser problem with "{}" brackets
     */
    public void testPreparedStatementParsing1() throws Exception {
        String data = "New {order} plus {1} more";
        Statement stmt = con.createStatement();

        stmt.execute("CREATE TABLE #psp1 (data VARCHAR(32))");
        stmt.close();

        stmt = con.createStatement();
        stmt.execute("create procedure #sp_psp1 @data VARCHAR(32) as INSERT INTO #psp1 (data) VALUES(@data)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("{call #sp_psp1('" + data + "')}");

        pstmt.execute();
        pstmt.close();

        stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT data FROM #psp1");

        assertTrue(rs.next());

        assertTrue(data.equals(rs.getString(1)));

        assertTrue(!rs.next());
        rs.close();
    }

    /**
     * Test for [931090] ArrayIndexOutOfBoundsException in rollback()
     */
    public void testPreparedStatementRollback1() throws Exception {
        try {
            Statement stmt = con.createStatement();

            stmt.execute("CREATE TABLE #psr1 (data BIT)");
            stmt.close();

            con.setAutoCommit(false);
            PreparedStatement pstmt = con.prepareStatement("INSERT INTO #psr1 (data) VALUES (?)");

            pstmt.setBoolean(1, true);
            assertTrue(pstmt.executeUpdate() == 1);
            pstmt.close();

            con.rollback();

            stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT data FROM #psr1");

            assertTrue(!rs.next());

            rs.close();

            disconnect();

            try {
                con.rollback();
                assertTrue(false);
            } catch (Exception e) {
                assertTrue(e instanceof SQLException);
            }
        } finally {
            connect();
        }
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(PreparedStatementTest.class);
    }
}