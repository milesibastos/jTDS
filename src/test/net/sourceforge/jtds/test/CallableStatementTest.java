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
        CallableStatement cstmt = con.prepareCall("{call sp_who}");

        cstmt.close();
    }

    public void testCallableStatement1() throws Exception {
        CallableStatement cstmt = con.prepareCall("sp_who");

        makeTestTables(cstmt);
        makeObjects(cstmt, 8);

        ResultSet rs = cstmt.executeQuery();

        dump(rs);

        cstmt.close();
        rs.close();
    }

    public void testCallableStatementCall1() throws Exception {
        CallableStatement cstmt = con.prepareCall("{call sp_who}");

        makeTestTables(cstmt);
        makeObjects(cstmt, 8);

        ResultSet rs = cstmt.executeQuery();

        dump(rs);

        cstmt.close();
        rs.close();
    }

    public void testCallableStatementCall2() throws Exception {
        CallableStatement cstmt = con.prepareCall("{CALL sp_who}");

        makeTestTables(cstmt);
        makeObjects(cstmt, 8);

        ResultSet rs = cstmt.executeQuery();

        dump(rs);

        cstmt.close();
        rs.close();
    }

    public void testCallableStatementCall3() throws Exception {
        CallableStatement cstmt = con.prepareCall("{cAlL sp_who}");

        makeTestTables(cstmt);
        makeObjects(cstmt, 8);

        ResultSet rs = cstmt.executeQuery();

        dump(rs);

        cstmt.close();
        rs.close();
    }

    /**
     * Test for bug [974801] stored procedure error in Northwind
     */
    public void testCallableStatementCall4() throws Exception {
        Statement stmt;

        try {
            stmt = con.createStatement();
            stmt.execute("create procedure \"test space\" as SELECT COUNT(*) FROM sysobjects");
            stmt.close();

            CallableStatement cstmt = con.prepareCall("{call \"test space\"}");

            makeTestTables(cstmt);
            makeObjects(cstmt, 8);

            ResultSet rs = cstmt.executeQuery();

            dump(rs);

            cstmt.close();
            rs.close();
        } finally {
            stmt = con.createStatement();
            stmt.execute("drop procedure \"test space\"");
            stmt.close();
        }
    }

    public void testCallableStatementExec1() throws Exception {
        CallableStatement cstmt = con.prepareCall("exec sp_who");

        makeTestTables(cstmt);
        makeObjects(cstmt, 8);

        ResultSet rs = cstmt.executeQuery();

        dump(rs);

        cstmt.close();
        rs.close();
    }

    public void testCallableStatementExec2() throws Exception {
        CallableStatement cstmt = con.prepareCall("EXEC sp_who");

        makeTestTables(cstmt);
        makeObjects(cstmt, 8);

        ResultSet rs = cstmt.executeQuery();

        dump(rs);

        cstmt.close();
        rs.close();
    }

    public void testCallableStatementExec3() throws Exception {
        CallableStatement cstmt = con.prepareCall("execute sp_who");

        makeTestTables(cstmt);
        makeObjects(cstmt, 8);

        ResultSet rs = cstmt.executeQuery();

        dump(rs);

        cstmt.close();
        rs.close();
    }

    public void testCallableStatementExec4() throws Exception {
        CallableStatement cstmt = con.prepareCall("EXECUTE sp_who");

        makeTestTables(cstmt);
        makeObjects(cstmt, 8);

        ResultSet rs = cstmt.executeQuery();

        dump(rs);

        cstmt.close();
        rs.close();
    }

    public void testCallableStatementExec5() throws Exception {
        CallableStatement cstmt = con.prepareCall("eXeC sp_who");

        makeTestTables(cstmt);
        makeObjects(cstmt, 8);

        ResultSet rs = cstmt.executeQuery();

        dump(rs);

        cstmt.close();
        rs.close();
    }

    public void testCallableStatementExec6() throws Exception {
        CallableStatement cstmt = con.prepareCall("ExEcUtE sp_who");

        makeTestTables(cstmt);
        makeObjects(cstmt, 8);

        ResultSet rs = cstmt.executeQuery();

        dump(rs);

        cstmt.close();
        rs.close();
    }

    public void testCallableStatementExec7() throws Exception {
        CallableStatement cstmt = con.prepareCall("execute \"master\"..sp_who");

        makeTestTables(cstmt);
        makeObjects(cstmt, 8);

        ResultSet rs = cstmt.executeQuery();

        dump(rs);

        cstmt.close();
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
        CallableStatement cstmt = con.prepareCall("{call sp_who}");

        assertTrue(cstmt.execute());

        ResultSet rs = cstmt.getResultSet();

        assertTrue(rs != null);

        dump(rs);

        cstmt.close();
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
     * Test for bug [1006845] Stored procedure with 18 parameters
     */
    public void testCallableStatementParsing3() throws Exception {
        CallableStatement cstmt = con.prepareCall("{Call Test(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)}");
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
            assertNull(rs.getString(1));
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
     * Test for bug [974284] retval on callable statement isn't handled correctly
     */
    public void testCallableRegisterOutParameter1() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("create procedure #rop1 @a varchar(1), @b varchar(1) as\r\n "
                     + "begin\r\n"
                     + "return 1\r\n"
                     + "end");
        stmt.close();

        CallableStatement cstmt = con.prepareCall("{? = call #rop1(?, ?)}");

        cstmt.registerOutParameter(1, Types.INTEGER);
        cstmt.setString(2, "a");
        cstmt.setString(3, "b");
        cstmt.execute();

        assertEquals(1, cstmt.getInt(1));
        assertEquals("1", cstmt.getString(1));

        cstmt.close();
    }

    /**
     * Test for bug [994888] Callable statement and Float output parameter
     */
    public void testCallableRegisterOutParameter2() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("create procedure #rop2 @data float OUTPUT as\r\n "
                     + "begin\r\n"
                     + "set @data = 1.1\r\n"
                     + "end");
        stmt.close();

        CallableStatement cstmt = con.prepareCall("{call #rop2(?)}");

        cstmt.registerOutParameter(1, Types.FLOAT);
        cstmt.execute();

        assertTrue(cstmt.getFloat(1) == 1.1f);
        cstmt.close();
    }

    /**
     * Test for bug [994988] Network error when null is returned via int output parm
     */
    public void testCallableRegisterOutParameter3() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("create procedure #rop3 @data int OUTPUT as\r\n "
                     + "begin\r\n"
                     + "set @data = null\r\n"
                     + "end");
        stmt.close();

        CallableStatement cstmt = con.prepareCall("{call #rop3(?)}");

        cstmt.registerOutParameter(1, Types.INTEGER);
        cstmt.execute();

        cstmt.getInt(1);
        assertTrue(cstmt.wasNull());
        cstmt.close();
    }

    /**
     * Test for bug [983432] Prepared call doesn't work with jTDS 0.8
     */
    public void testCallableRegisterOutParameter4() throws Exception {
        CallableStatement cstmt = con.prepareCall("{call sp_addtype T_INTEGER, int, 'NULL'}");

        cstmt.execute();
        cstmt.close();

        Statement stmt = con.createStatement();
        stmt.execute("create procedure rop4 @data T_INTEGER OUTPUT as\r\n "
                     + "begin\r\n"
                     + "set @data = 1\r\n"
                     + "end");
        stmt.close();

        cstmt = con.prepareCall("{call rop4(?)}");

        cstmt.registerOutParameter(1, Types.VARCHAR);
        cstmt.execute();

        assertEquals(cstmt.getInt(1), 1);
        assertTrue(!cstmt.wasNull());
        cstmt.close();

        stmt = con.createStatement();
        stmt.execute("drop procedure rop4");
        stmt.close();

        cstmt = con.prepareCall("{call sp_droptype 'T_INTEGER'}");
        cstmt.execute();
        cstmt.close();
    }

    /**
     * Test for bug [991640] java.sql.Date error and RAISERROR problem
     */
    public void testCallableError1() throws Exception {
        String text = "test message";

        Statement stmt = con.createStatement();
        stmt.execute("create procedure #ce1 as\r\n "
                     + "begin\r\n"
                     + "RAISERROR('" + text + "', 16, 1 )\r\n"
                     + "end");
        stmt.close();

        CallableStatement cstmt = con.prepareCall("{call #ce1}");

        try {
            cstmt.execute();
            assertTrue(false);
        } catch (SQLException e) {
            assertTrue(e.getMessage().equals(text));
        }

        cstmt.close();
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(CallableStatementTest.class);
    }
}