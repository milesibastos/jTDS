package net.sourceforge.jtds.test;

import java.sql.*;
import junit.framework.TestCase;

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

    public void testCallableStatementCall1() throws Exception {
        CallableStatement stmt = con.prepareCall("{call sp_who}");

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

    public static void main(String[] args) {
        junit.textui.TestRunner.run(CallableStatementTest.class);
    }
}