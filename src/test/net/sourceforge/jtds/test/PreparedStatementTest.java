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

        Statement stmt = con.createStatement();
        makeTestTables(stmt);
        makeObjects(stmt, 10);
        stmt.close();

        ResultSet rs = pstmt.executeQuery();
        dump(rs);

        rs.close();
        pstmt.close();
    }

    public void testScrollablePreparedStatement() throws Exception {
        Statement stmt = con.createStatement();
        makeTestTables(stmt);
        makeObjects(stmt, 10);
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("SELECT * FROM #test",
                                                       ResultSet.TYPE_SCROLL_SENSITIVE,
                                                       ResultSet.CONCUR_READ_ONLY);

        ResultSet rs = pstmt.executeQuery();

        assertTrue(rs.isBeforeFirst());

        while (rs.next()) {
        }

        assertTrue(rs.isAfterLast());

        //This currently fails because the PreparedStatement
        //Doesn't know it needs to create a cursored ResultSet.
        //Needs some refactoring!!
        // SAfe Not any longer. ;o)
        while (rs.previous()) {
        }

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

        assertEquals(results.length, count);

        for (int i = 0; i < count; i++) {
            assertEquals(results[i], 1);
        }

        pstmt.close();

        con.commit();
        con.setAutoCommit(true);

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT SUM(f_int) FROM #psbatch1");

        assertTrue(rs.next());
        System.out.println(rs.getInt(1));
        assertEquals(rs.getInt(1), sum);
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

        assertFalse(rs.next());
        rs.close();
    }

    /**
     * Test for bug [1008882] Some queries with parameters cannot be executed with 0.9-rc1
     */
    public void testPreparedStatementParsing2() throws Exception {
        PreparedStatement pstmt = con.prepareStatement(" SELECT ?");

        pstmt.setString(1, "TEST");

        ResultSet rs = pstmt.executeQuery();

        assertTrue(rs.next());
        assertEquals("TEST", rs.getString(1));
        assertFalse(rs.next());

        pstmt.close();
        rs.close();
    }

    /**
     * Test for [931090] ArrayIndexOutOfBoundsException in rollback()
     */
    public void testPreparedStatementRollback1() throws Exception {
        Connection localCon = getConnection();
        Statement stmt = localCon.createStatement();

        stmt.execute("CREATE TABLE #psr1 (data BIT)");

        localCon.setAutoCommit(false);
        PreparedStatement pstmt = localCon.prepareStatement("INSERT INTO #psr1 (data) VALUES (?)");

        pstmt.setBoolean(1, true);
        assertEquals(1, pstmt.executeUpdate());
        pstmt.close();

        localCon.rollback();

        ResultSet rs = stmt.executeQuery("SELECT data FROM #psr1");
        assertFalse(rs.next());
        rs.close();

        localCon.close();

        try {
            localCon.commit();
            fail("Expecting commit to fail, connection was closed");
        } catch (SQLException ex) {
            assertEquals("HY010", ex.getSQLState());
        }

        try {
            localCon.rollback();
            fail("Expecting rollback to fail, connection was closed");
        } catch (SQLException ex) {
            assertEquals("HY010", ex.getSQLState());
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
        assertEquals(1, pstmt.executeUpdate());
        pstmt.close();

        stmt = con.createStatement();

        ResultSet rs = stmt.executeQuery("SELECT data FROM #psso1");

        assertTrue(rs.next());
        assertEquals(data.doubleValue(), rs.getDouble(1), 0);
        assertFalse(rs.next());
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
        assertEquals(1, pstmt.executeUpdate());
        pstmt.close();

        stmt = con.createStatement();

        ResultSet rs = stmt.executeQuery("SELECT data FROM #psso2");

        assertTrue(rs.next());
        assertEquals(data.doubleValue(), rs.getDouble(1), 0);
        assertFalse(rs.next());
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
        assertEquals(1, pstmt.executeUpdate());
        pstmt.close();

        stmt = con.createStatement();

        ResultSet rs = stmt.executeQuery("SELECT data FROM #psso3");

        assertTrue(rs.next());
        assertEquals(data.doubleValue(), rs.getDouble(1), 0);
        assertFalse(rs.next());
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
        assertEquals(1, pstmt.executeUpdate());
        pstmt.close();

        stmt = con.createStatement();

        ResultSet rs = stmt.executeQuery("SELECT data FROM #psso4");

        assertTrue(rs.next());
        assertEquals(data.doubleValue(), rs.getDouble(1), 0);
        assertFalse(rs.next());
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
        assertEquals(1, pstmt.executeUpdate());
        pstmt.close();

        stmt = con.createStatement();

        ResultSet rs = stmt.executeQuery("SELECT data FROM #psso5");

        assertTrue(rs.next());
        assertEquals(data.doubleValue(), rs.getDouble(1), 0);
        assertFalse(rs.next());
        rs.close();
    }

    /**
     * Test for bug [985754] row count is always 0
     */
    public void testUpdateCount1() throws Exception {
    	int count = 500;

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #updateCount1 (data INT)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #updateCount1 (data) VALUES (?)");

        for (int i = 1; i <= count; i++) {
            pstmt.setInt(1, i);
            assertEquals(1, pstmt.executeUpdate());
        }

        pstmt.close();

        stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM #updateCount1");

        assertTrue(rs.next());
        assertEquals(count, rs.getInt(1));
        assertFalse(rs.next());

        stmt.close();
        rs.close();

        pstmt = con.prepareStatement("DELETE FROM #updateCount1");
        assertEquals(count, pstmt.executeUpdate());
        pstmt.close();

    }

    /**
     * Test for parameter markers in function escapes.
     */
    public void testEscapedParams() throws Exception {
        PreparedStatement pstmt = con.prepareStatement("SELECT {fn left(?, 2)}");

        pstmt.setString(1, "TEST");

        ResultSet rs = pstmt.executeQuery();

        assertTrue(rs.next());
        assertEquals("TE", rs.getString(1));
        assertFalse(rs.next());

        rs.close();
        pstmt.close();
    }

    /**
     * Test for bug [1022968] Long SQL expression error.
     * NB. Test must be run with TDS=7.0 to fail.
     */
    public void testLongStatement() throws Exception {
        Statement stmt = con.createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);

        stmt.execute("CREATE TABLE #longStatement (id int primary key, data varchar(8000))");

        StringBuffer buf = new StringBuffer(4096);
        buf.append("SELECT * FROM #longStatement WHERE data = '");

        for (int i = 0; i < 4000; i++) {
            buf.append('X');
        }

        buf.append("'");

        ResultSet rs = stmt.executeQuery(buf.toString());

        assertNotNull(rs);
        assertFalse(rs.next());

        rs.close();
        stmt.close();
    }

    /**
     * Test for bug [1010660] 0.9-rc1 setMaxRows causes unlimited temp stored
     * procedures. This test has to be run with logging enabled or while
     * monitoring it with SQL Profiler to see whether the temporary stored
     * procedure is executed or the SQL is executed directly.
     */
    public void testMaxRows() throws SQLException {
//        net.sourceforge.jtds.util.Logger.setActive(true);
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #maxRows (val int)"
                + " INSERT INTO #maxRows VALUES (1)"
                + " INSERT INTO #maxRows VALUES (2)");

        PreparedStatement pstmt = con.prepareStatement(
                "SELECT * FROM #maxRows WHERE val<? ORDER BY val");
        pstmt.setInt(1, 100);
        pstmt.setMaxRows(1);

        ResultSet rs = pstmt.executeQuery();

        assertNotNull(rs);
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertFalse(rs.next());

        rs.close();
        pstmt.close();

        stmt.executeUpdate("DROP TABLE #maxRows");
        stmt.close();
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(PreparedStatementTest.class);
    }
}
