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

import java.math.BigDecimal;
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
        pstmt.close();
    }

    public void testPreparedStatementAddBatch1()
    throws Exception {
        int count = 50;

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #psbatch1 (f_int INT)");

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

        ResultSet rs = stmt.executeQuery("SELECT SUM(f_int) FROM #psbatch1");

        assertTrue(rs.next());
        System.out.println(rs.getInt(1));
        assertEquals(rs.getInt(1), sum);
        rs.close();
        stmt.close();
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
        stmt.close();
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
        stmt.close();

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

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #psso1 (data) VALUES (?)");

        pstmt.setObject(1, data);
        assertEquals(1, pstmt.executeUpdate());
        pstmt.close();

        ResultSet rs = stmt.executeQuery("SELECT data FROM #psso1");

        assertTrue(rs.next());
        assertEquals(data.doubleValue(), rs.getDouble(1), 0);
        assertFalse(rs.next());
        rs.close();
        stmt.close();
    }

    /**
     * Test for bug [938494] setObject(i, o, NUMERIC/DECIMAL) cuts off decimal places
     */
    public void testPreparedStatementSetObject2() throws Exception {
        BigDecimal data = new BigDecimal(3.7D);

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #psso2 (data MONEY)");

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #psso2 (data) VALUES (?)");

        pstmt.setObject(1, data, Types.NUMERIC);
        assertEquals(1, pstmt.executeUpdate());
        pstmt.close();

        ResultSet rs = stmt.executeQuery("SELECT data FROM #psso2");

        assertTrue(rs.next());
        assertEquals(data.doubleValue(), rs.getDouble(1), 0);
        assertFalse(rs.next());
        rs.close();
        stmt.close();
    }

    /**
     * Test for bug [938494] setObject(i, o, NUMERIC/DECIMAL) cuts off decimal places
     */
    public void testPreparedStatementSetObject3() throws Exception {
        BigDecimal data = new BigDecimal(3.7D);

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #psso3 (data MONEY)");

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #psso3 (data) VALUES (?)");

        pstmt.setObject(1, data, Types.DECIMAL);
        assertEquals(1, pstmt.executeUpdate());
        pstmt.close();

        ResultSet rs = stmt.executeQuery("SELECT data FROM #psso3");

        assertTrue(rs.next());
        assertEquals(data.doubleValue(), rs.getDouble(1), 0);
        assertFalse(rs.next());
        rs.close();
        stmt.close();
    }

    /**
     * Test for bug [938494] setObject(i, o, NUMERIC/DECIMAL) cuts off decimal places
     */
    public void testPreparedStatementSetObject4() throws Exception {
        BigDecimal data = new BigDecimal(3.7D);

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #psso4 (data MONEY)");

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #psso4 (data) VALUES (?)");

        pstmt.setObject(1, data, Types.NUMERIC, 4);
        assertEquals(1, pstmt.executeUpdate());
        pstmt.close();

        ResultSet rs = stmt.executeQuery("SELECT data FROM #psso4");

        assertTrue(rs.next());
        assertEquals(data.doubleValue(), rs.getDouble(1), 0);
        assertFalse(rs.next());
        rs.close();
        stmt.close();
    }

    /**
     * Test for bug [938494] setObject(i, o, NUMERIC/DECIMAL) cuts off decimal places
     */
    public void testPreparedStatementSetObject5() throws Exception {
        BigDecimal data = new BigDecimal(3.7D);

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #psso5 (data MONEY)");

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #psso5 (data) VALUES (?)");

        pstmt.setObject(1, data, Types.DECIMAL, 4);
        assertEquals(1, pstmt.executeUpdate());
        pstmt.close();

        ResultSet rs = stmt.executeQuery("SELECT data FROM #psso5");

        assertTrue(rs.next());
        assertEquals(data.doubleValue(), rs.getDouble(1), 0);
        assertFalse(rs.next());
        rs.close();
        stmt.close();
    }

    /**
     * Test for bug [985754] row count is always 0
     */
    public void testUpdateCount1() throws Exception {
    	int count = 50;

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #updateCount1 (data INT)");

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #updateCount1 (data) VALUES (?)");

        for (int i = 1; i <= count; i++) {
            pstmt.setInt(1, i);
            assertEquals(1, pstmt.executeUpdate());
        }

        pstmt.close();

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
     * Test for bug [ 1059916 ] whitespace needed in preparedStatement.
     */
    public void testMissingWhitespace() throws Exception
    {
        PreparedStatement pstmt = con.prepareStatement(
            "SELECT name from master..syscharsets where description like?and?between csid and 10");
        pstmt.setString(1, "ISO%");
        pstmt.setInt(2, 0);
        ResultSet rs = pstmt.executeQuery();
        assertNotNull(rs);
        assertTrue(rs.next());
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
     * test for bug [ 1047330 ] prep statement with more than 2100 params fails.
     */
    public void testManyParametersStatement() throws Exception {

        Statement stmt = con.createStatement();
        makeTestTables(stmt);
        makeObjects(stmt, 10);
        stmt.close();
        StringBuffer sb = new StringBuffer(12000);
        sb.append("SELECT * FROM #test WHERE f_int in ( ?");
        for (int j = 0; j < 2110; j++)
        {
            sb.append(", ?");
        }
        sb.append(")");
        try {
            con.prepareStatement(sb.toString());
            fail("Too many parameters Exception expected");
        } catch (SQLException e)
        {
            assertTrue(e.getMessage().startsWith("Prepared or callable"));
        }
    }

    /**
     * Test for bug [1010660] 0.9-rc1 setMaxRows causes unlimited temp stored
     * procedures. This test has to be run with logging enabled or while
     * monitoring it with SQL Profiler to see whether the temporary stored
     * procedure is executed or the SQL is executed directly.
     */
    public void testMaxRows() throws SQLException {
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

    /**
     * Test for bug [1050660] PreparedStatement.getMetaData() clears resultset.
     */
    public void testMetaDataClearsResultSet() throws Exception {
        Statement stmt = con.createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);

        stmt.executeUpdate(
                "CREATE TABLE #metaDataClearsResultSet (id int primary key, data varchar(8000))");
        stmt.executeUpdate("INSERT INTO #metaDataClearsResultSet (id, data)"
                + " VALUES (1, '1')");
        stmt.executeUpdate("INSERT INTO #metaDataClearsResultSet (id, data)"
                + " VALUES (2, '2')");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement(
                "SELECT * FROM #metaDataClearsResultSet ORDER BY id");
        ResultSet rs = pstmt.executeQuery();

        assertNotNull(rs);

        ResultSetMetaData rsmd = pstmt.getMetaData();
        assertEquals(2, rsmd.getColumnCount());
        assertEquals("id", rsmd.getColumnName(1));
        assertEquals("data", rsmd.getColumnName(2));
        assertEquals(8000, rsmd.getColumnDisplaySize(2));

        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("1", rs.getString(2));

        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("2", rs.getString(2));

        assertFalse(rs.next());

        rs.close();
        pstmt.close();
    }

    /**
     * Test for bad truncation in prepared statements on metadata retrieval
     * (patch [1076383] ResultSetMetaData for more complex statements for SQL
     * Server).
     */
    public void testMetaData() throws Exception {
        Statement stmt = con.createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);

        stmt.executeUpdate("CREATE TABLE #metaData (id int, data varchar(8000))");
        stmt.executeUpdate("INSERT INTO #metaData (id, data)"
                + " VALUES (1, 'Data1')");
        stmt.executeUpdate("INSERT INTO #metaData (id, data)"
                + " VALUES (1, 'Data2')");
        stmt.executeUpdate("INSERT INTO #metaData (id, data)"
                + " VALUES (2, 'Data3')");
        stmt.executeUpdate("INSERT INTO #metaData (id, data)"
                + " VALUES (2, 'Data4')");
        stmt.close();

        // test simple statement
        PreparedStatement pstmt = con.prepareStatement("SELECT id " +
                "FROM #metaData " +
                "WHERE data=? GROUP BY id");

        ResultSetMetaData rsmd = pstmt.getMetaData();

        assertNotNull("No meta data returned for simple statement", rsmd);

        assertEquals(1, rsmd.getColumnCount());
        assertEquals("id", rsmd.getColumnName(1));

        pstmt.close();

        // test more complex statement
        pstmt = con.prepareStatement("SELECT id, count(*) as count " +
                "FROM #metaData " +
                "WHERE data=? GROUP BY id");

        rsmd = pstmt.getMetaData();

        assertNotNull("No metadata returned for complex statement", rsmd);

        assertEquals(2, rsmd.getColumnCount());
        assertEquals("id", rsmd.getColumnName(1));
        assertEquals("count", rsmd.getColumnName(2));

        pstmt.close();
    }

    /**
     * Test for bug [1071397] Error in prepared statement (parameters in outer
     * join escapes are not recognized).
     */
    public void testOuterJoinParameters() throws SQLException {
        Statement stmt = con.createStatement();
        stmt.executeUpdate(
                "CREATE TABLE #outerJoinParameters (id int primary key)");
        stmt.executeUpdate(
                "INSERT #outerJoinParameters (id) values (1)");
        stmt.close();

        // Real dumb join, the idea is to see the parser works fine
        PreparedStatement pstmt = con.prepareStatement(
                "select * from "
                + "{oj #outerJoinParameters a left outer join #outerJoinParameters b on a.id = ?}"
                + "where b.id = ?");
        pstmt.setInt(1, 1);
        pstmt.setInt(2, 1);
        ResultSet rs = pstmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(1, rs.getInt(2));
        assertFalse(rs.next());
        rs.close();
        pstmt.close();

        pstmt = con.prepareStatement("select {fn round(?, 0)}");
        pstmt.setDouble(1, 1.2);
        rs = pstmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getDouble(1), 0);
        assertFalse(rs.next());
        rs.close();
        pstmt.close();
    }

    /**
     * Inner class used by {@link PreparedStatementTest#testMultiThread} to
     * test concurrency.
     */
    static class TestMultiThread extends Thread {
        static Connection con;
        static final int THREAD_MAX = 10;
        static final int LOOP_MAX = 10;
        static final int ROWS_MAX = 10;
        static int live;
        static Exception error;

        int threadId;

        TestMultiThread(int n) {
            threadId = n;
        }

        public void run() {
            try {
                con.rollback();
                PreparedStatement pstmt = con.prepareStatement(
                        "SELECT id, data FROM #TEST WHERE id = ?",
                        ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

                for (int i = 1; i <= LOOP_MAX; i++) {
                    pstmt.clearParameters();
                    pstmt.setInt(1, i);
                    ResultSet rs = pstmt.executeQuery();

                    while (rs.next()) {
                        rs.getInt(1);
                        rs.getString(2);
                    }

                }

                pstmt.close();
            } catch (Exception e) {
                System.err.print("ID=" + threadId + ' ');
                e.printStackTrace();
                error = e;
            }

            synchronized (this.getClass()) {
                live--;
            }
        }

        static void startThreads(Connection con) throws Exception {
            TestMultiThread.con = con;
            con.setAutoCommit(false);

            Statement stmt = con.createStatement();
            stmt.execute("CREATE TABLE #TEST (id int identity primary key, data varchar(255))");

            for (int i = 0; i < ROWS_MAX; i++) {
                stmt.executeUpdate("INSERT INTO #TEST (data) VALUES('This is line " + i + "')");
            }

            stmt.close();
            con.commit();

            live = THREAD_MAX;
            for (int i = 0; i < THREAD_MAX; i++) {
                new TestMultiThread(i).start();
            }
            while (live > 0) {
                sleep(1);
            }

            if (error != null) {
                throw error;
            }
        }
    }

    /**
     * Test <code>Connection</code> concurrency by running
     * <code>PreparedStatement</code>s and rollbacks at the same time to see
     * whether handles are not lost in the process.
     */
    public void testMultiThread() throws Exception {
        TestMultiThread.startThreads(con);
    }

    /**
     * Test for bug [1094621] Decimal conversion error:  A prepared statement
     * with a decimal parameter that is -1E38 will fail as a result of the
     * driver generating a parameter specification of decimal(38,10) rather
     * than decimal(38,0).
     */
    public void testBigDecBadParamSpec() throws Exception
    {
        Statement stmt = con.createStatement();
        stmt.execute(
                "create table #test (id int primary key, val decimal(38,0))");
        BigDecimal bd =
                new BigDecimal("99999999999999999999999999999999999999");
        PreparedStatement pstmt =
                con.prepareStatement("insert into #test values(?,?)");
        pstmt.setInt(1, 1);
        pstmt.setBigDecimal(2, bd);
        assertEquals(1, pstmt.executeUpdate()); // Worked OK
        pstmt.setInt(1, 2);
        pstmt.setBigDecimal(2, bd.negate());
        assertEquals(1, pstmt.executeUpdate()); // Failed
    }

    /**
     * Test for bug [1111516 ] Illegal Parameters in PreparedStatement.
     */
    public void testIllegalParameters() throws Exception
    {
        Statement stmt = con.createStatement();
        stmt.execute("create table #test (id int)");
        PreparedStatement pstmt =
                con.prepareStatement("select top ? from #test");
        pstmt.setInt(1, 10);
        try {
            pstmt.executeQuery();
            fail("Expecting an exception to be thrown.");
        } catch (SQLException ex) {
            assertEquals("37000", ex.getSQLState());
        }
        pstmt.close();
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(PreparedStatementTest.class);
    }
}
