package net.sourceforge.jtds.test;

import java.sql.*;

/**
 * @version 1.0
 */
public class CallableStatementTest extends TestBase {
    public CallableStatementTest(String name) {
        super(name);
    }

    public void testCallableStatement() throws Exception {
        CallableStatement stmt = con.prepareCall("{call sp_who}");

        stmt.close();
    }

    public void testCallableStatement1() throws Exception {
        CallableStatement stmt = con.prepareCall("sp_who");

        makeTestTables(stmt);
        makeObjects(stmt, 8);

        ResultSet rs = stmt.executeQuery();

        dump(rs);

        stmt.close();
        rs.close();
    }

    public void testCallableStatementCall1() throws Exception {
        CallableStatement stmt = con.prepareCall("{call sp_who}");

        makeTestTables(stmt);
        makeObjects(stmt, 8);

        ResultSet rs = stmt.executeQuery();

        dump(rs);

        stmt.close();
        rs.close();
    }

    public void testCallableStatementCall2() throws Exception {
        CallableStatement stmt = con.prepareCall("{CALL sp_who}");

        makeTestTables(stmt);
        makeObjects(stmt, 8);

        ResultSet rs = stmt.executeQuery();

        dump(rs);

        stmt.close();
        rs.close();
    }

    public void testCallableStatementCall3() throws Exception {
        CallableStatement stmt = con.prepareCall("{cAlL sp_who}");

        makeTestTables(stmt);
        makeObjects(stmt, 8);

        ResultSet rs = stmt.executeQuery();

        dump(rs);

        stmt.close();
        rs.close();
    }

    public void testCallableStatementExec1() throws Exception {
        CallableStatement stmt = con.prepareCall("exec sp_who");

        makeTestTables(stmt);
        makeObjects(stmt, 8);

        ResultSet rs = stmt.executeQuery();

        dump(rs);

        stmt.close();
        rs.close();
    }

    public void testCallableStatementExec2() throws Exception {
        CallableStatement stmt = con.prepareCall("EXEC sp_who");

        makeTestTables(stmt);
        makeObjects(stmt, 8);

        ResultSet rs = stmt.executeQuery();

        dump(rs);

        stmt.close();
        rs.close();
    }

    public void testCallableStatementExec3() throws Exception {
        CallableStatement stmt = con.prepareCall("execute sp_who");

        makeTestTables(stmt);
        makeObjects(stmt, 8);

        ResultSet rs = stmt.executeQuery();

        dump(rs);

        stmt.close();
        rs.close();
    }

    public void testCallableStatementExec4() throws Exception {
        CallableStatement stmt = con.prepareCall("EXECUTE sp_who");

        makeTestTables(stmt);
        makeObjects(stmt, 8);

        ResultSet rs = stmt.executeQuery();

        dump(rs);

        stmt.close();
        rs.close();
    }

    public void testCallableStatementExec5() throws Exception {
        CallableStatement stmt = con.prepareCall("eXeC sp_who");

        makeTestTables(stmt);
        makeObjects(stmt, 8);

        ResultSet rs = stmt.executeQuery();

        dump(rs);

        stmt.close();
        rs.close();
    }

    public void testCallableStatementExec6() throws Exception {
        CallableStatement stmt = con.prepareCall("ExEcUtE sp_who");

        makeTestTables(stmt);
        makeObjects(stmt, 8);

        ResultSet rs = stmt.executeQuery();

        dump(rs);

        stmt.close();
        rs.close();
    }

    public void testCallableStatementExec7() throws Exception {
        CallableStatement stmt = con.prepareCall("execute \"master\"..sp_who");

        makeTestTables(stmt);
        makeObjects(stmt, 8);

        ResultSet rs = stmt.executeQuery();

        dump(rs);

        stmt.close();
        rs.close();
    }

    public void testCallableStatementExec8() throws Exception {
        Statement stmt;

        try {
            stmt = con.createStatement();
            stmt.execute("create procedure _test as SELECT COUNT(*) FROM sysobjects");
            stmt.close();

            CallableStatement cstmt = con.prepareCall("execute _test");

            makeTestTables(cstmt);
            makeObjects(cstmt, 8);

            ResultSet rs = cstmt.executeQuery();

            dump(rs);

            cstmt.close();
            rs.close();
        } finally {
            stmt = con.createStatement();
            stmt.execute("drop procedure _test");
            stmt.close();
        }
    }

    public void testCallableStatementParsing1() throws Exception {
        String data = "New {order} plus {1} more";
        Statement stmt = con.createStatement();

        stmt.execute("CREATE TABLE #csp1 (data VARCHAR(32))");
        stmt.close();

        stmt = con.createStatement();
        stmt.execute("create procedure #sp_csp1 @data VARCHAR(32) as INSERT INTO #csp1 (data) VALUES(@data)");
        stmt.close();

        CallableStatement cstmt = con.prepareCall("{call #sp_csp1(?)}");

        cstmt.setString(1, data);
        cstmt.execute();
        cstmt.close();

        stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT data FROM #csp1");

        assertTrue(rs.next());

        assertTrue(data.equals(rs.getString(1)));

        assertTrue(!rs.next());
        rs.close();
    }

    /**
     * Test fro bug [938632] String index out of bounds error in 0.8rc1
     */
    public void testCallableStatementParsing2() throws Exception {
        try {
            Statement stmt = con.createStatement();

            stmt.execute("create procedure load_smtp_in_1gr_ls804192 as SELECT name FROM sysobjects");
            stmt.close();

            CallableStatement cstmt = con.prepareCall("{?=call load_smtp_in_1gr_ls804192}");

            cstmt.execute();
            cstmt.close();
        } finally {
            Statement stmt = con.createStatement();

            stmt.execute("drop procedure load_smtp_in_1gr_ls804192");
            stmt.close();
        }
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(CallableStatementTest.class);
    }
}