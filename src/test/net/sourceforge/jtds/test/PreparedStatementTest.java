package net.sourceforge.jtds.test;

import java.math.*;
import java.sql.*;

/**
 * @version 1.0
 */

public class PreparedStatementTest extends TestBase {

    public PreparedStatementTest(String name) {
        super(name);
    }

    public void testPreparedStatement() throws Exception {
        PreparedStatement pstmt = con.prepareStatement("SELECT * FROM #test");

        makeTestTables(pstmt);
        makeObjects(pstmt, 10);

        ResultSet rs = pstmt.executeQuery();

        dump(rs);

        rs.close();

    }

    public void testScrollablePreparedStatement() throws Exception {
        Statement stmt = con.createStatement();
        makeTestTables(stmt);
        makeObjects(stmt, 10);
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement(
            "SELECT * FROM #test",
            ResultSet.TYPE_SCROLL_SENSITIVE,
            ResultSet.CONCUR_READ_ONLY);

        ResultSet rs = pstmt.executeQuery();

        assertTrue(rs.isBeforeFirst());
        while (rs.next()) {}
        assertTrue(rs.isAfterLast());

        //This currently fails because the PreparedStatement
        //Doesn't know it needs to create a cursored ResultSet.
        //Needs some refactoring!!
        // SAfe Not any longer. ;o)
        while (rs.previous()) {}
        assertTrue(rs.isBeforeFirst());

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
            con.close();

            try {
                con.commit();
                assertTrue(false);
            } catch (Exception e) {
                assertTrue(e instanceof SQLException);
            }

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

    /**
     * Test for bug [938494] setObject(i, o, NUMERIC/DECIMAL) cuts off decimal places
     */
    public void testPreparedStatementSetObject1() throws Exception {
        BigDecimal data = new BigDecimal(3.7D);

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #psso1 (data MONEY)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #psso1 (data) VALUES (?)");

        pstmt.setObject(1, data);
        assertTrue(pstmt.executeUpdate() == 1);
        pstmt.close();
    
        stmt = con.createStatement();

        ResultSet rs = stmt.executeQuery("SELECT data FROM #psso1");

        assertTrue(rs.next());
        assertTrue(data.doubleValue() == rs.getDouble(1));
        assertTrue(!rs.next());
        rs.close();
    }

    /**
     * Test for bug [938494] setObject(i, o, NUMERIC/DECIMAL) cuts off decimal places
     */
    public void testPreparedStatementSetObject2() throws Exception {
        BigDecimal data = new BigDecimal(3.7D);

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #psso2 (data MONEY)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #psso2 (data) VALUES (?)");

        pstmt.setObject(1, data, Types.NUMERIC);
        assertTrue(pstmt.executeUpdate() == 1);
        pstmt.close();
    
        stmt = con.createStatement();

        ResultSet rs = stmt.executeQuery("SELECT data FROM #psso2");

        assertTrue(rs.next());
        assertTrue(data.doubleValue() == rs.getDouble(1));
        assertTrue(!rs.next());
        rs.close();
    }

    /**
     * Test for bug [938494] setObject(i, o, NUMERIC/DECIMAL) cuts off decimal places
     */
    public void testPreparedStatementSetObject3() throws Exception {
        BigDecimal data = new BigDecimal(3.7D);

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #psso3 (data MONEY)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #psso3 (data) VALUES (?)");

        pstmt.setObject(1, data, Types.DECIMAL);
        assertTrue(pstmt.executeUpdate() == 1);
        pstmt.close();
    
        stmt = con.createStatement();

        ResultSet rs = stmt.executeQuery("SELECT data FROM #psso3");

        assertTrue(rs.next());
        assertTrue(data.doubleValue() == rs.getDouble(1));
        assertTrue(!rs.next());
        rs.close();
    }

    /**
     * Test for bug [938494] setObject(i, o, NUMERIC/DECIMAL) cuts off decimal places
     */
    public void testPreparedStatementSetObject4() throws Exception {
        BigDecimal data = new BigDecimal(3.7D);

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #psso4 (data MONEY)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #psso4 (data) VALUES (?)");

        pstmt.setObject(1, data, Types.NUMERIC, 4);
        assertTrue(pstmt.executeUpdate() == 1);
        pstmt.close();
    
        stmt = con.createStatement();

        ResultSet rs = stmt.executeQuery("SELECT data FROM #psso4");

        assertTrue(rs.next());
        assertTrue(data.doubleValue() == rs.getDouble(1));
        assertTrue(!rs.next());
        rs.close();
    }

    /**
     * Test for bug [938494] setObject(i, o, NUMERIC/DECIMAL) cuts off decimal places
     */
    public void testPreparedStatementSetObject5() throws Exception {
        BigDecimal data = new BigDecimal(3.7D);

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #psso5 (data MONEY)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #psso5 (data) VALUES (?)");

        pstmt.setObject(1, data, Types.DECIMAL, 4);
        assertTrue(pstmt.executeUpdate() == 1);
        pstmt.close();
    
        stmt = con.createStatement();

        ResultSet rs = stmt.executeQuery("SELECT data FROM #psso5");

        assertTrue(rs.next());
        assertTrue(data.doubleValue() == rs.getDouble(1));
        assertTrue(!rs.next());
        rs.close();
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(PreparedStatementTest.class);
    }
}