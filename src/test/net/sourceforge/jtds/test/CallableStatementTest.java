package net.sourceforge.jtds.test;

import java.sql.*;
//
// MJH - Changes for new jTDS version
// Added registerOutParameter to testCallableStatementParsing2
//
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

    /**
     * Test for bug [978175] 0.8: Stored Procedure call doesn't work anymore
     */
    public void testCallableStatementExec9() throws Exception {
        CallableStatement stmt = con.prepareCall("{call sp_who}");

        assertTrue(stmt.execute());

        ResultSet rs = stmt.getResultSet();

        assertTrue(rs != null);

        dump(rs);

        stmt.close();
        rs.close();
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
     * Test for bug [938632] String index out of bounds error in 0.8rc1
     */
    public void testCallableStatementParsing2() throws Exception {
        try {
            Statement stmt = con.createStatement();

            stmt.execute("create procedure load_smtp_in_1gr_ls804192 as SELECT name FROM sysobjects");
            stmt.close();

            CallableStatement cstmt = con.prepareCall("{?=call load_smtp_in_1gr_ls804192}");
            cstmt.registerOutParameter(1, java.sql.Types.INTEGER); // MJH 01/05/04
            cstmt.execute();
            cstmt.close();
        } finally {
            Statement stmt = con.createStatement();

            stmt.execute("drop procedure load_smtp_in_1gr_ls804192");
            stmt.close();
        }
    }

    /**
     * Test for reature request [956800] setNull(): Not implemented
     */
    public void testCallableSetNull1() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #callablesetnull1 (data CHAR(1) NULL)");
        stmt.close();
    	
        try {
            stmt = con.createStatement();
            stmt.execute("create procedure callableSetNull1 @data char(1) "
            		+ "as INSERT INTO #callablesetnull1 (data) VALUES (@data)");
            stmt.close();

            CallableStatement cstmt = con.prepareCall("{call callableSetNull1(?)}");
            // Test CallableStatement.setNull(int,Types.NULL)
            cstmt.setNull(1, Types.NULL);
            cstmt.execute();
            cstmt.close();
            
            stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT data FROM #callablesetnull1");

            assertTrue(rs.next());

            // Test ResultSet.getString()
            assertTrue(rs.getString(1) == null);
            assertTrue(rs.wasNull());

            assertTrue(!rs.next());
            stmt.close();
            rs.close();
        } finally {
            stmt = con.createStatement();
            stmt.execute("drop procedure callableSetNull1");
            stmt.close();
        }
    }
    
    /**
     * Test for bug [946171] null boolean in CallableStatement bug
     */
    public void testCallableRegisterOutParameter1() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("create procedure TEST_BOOL @bool bit, @whatever int OUTPUT as\r\n "
                     + "begin\r\n"
                     + "set @whatever = 1\r\n"
                     + "end");
        stmt.close();
    	
        try {
            CallableStatement cstmt = con.prepareCall("{call TEST_BOOL(?,?)}");

            cstmt.setNull(1, Types.BOOLEAN);
            cstmt.registerOutParameter(2, Types.INTEGER);
            cstmt.execute();

            assertTrue(cstmt.getInt(2) == 1);
            cstmt.close();
        } finally {
            stmt = con.createStatement();
            stmt.execute("drop procedure TEST_BOOL");
            stmt.close();
        }
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(CallableStatementTest.class);
    }
}