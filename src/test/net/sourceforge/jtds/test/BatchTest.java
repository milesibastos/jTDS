//jTDS JDBC Driver for Microsoft SQL Server and Sybase
//Copyright (C) 2004 The jTDS Project
//
//This library is free software; you can redistribute it and/or
//modify it under the terms of the GNU Lesser General Public
//License as published by the Free Software Foundation; either
//version 2.1 of the License, or (at your option) any later version.
//
//This library is distributed in the hope that it will be useful,
//but WITHOUT ANY WARRANTY; without even the implied warranty of
//MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
//Lesser General Public License for more details.
//
//You should have received a copy of the GNU Lesser General Public
//License along with this library; if not, write to the Free Software
//Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
//
package net.sourceforge.jtds.test;

import java.sql.BatchUpdateException;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.Statement;

/**
 * Simple test suite to exercise batch execution.
 *
 * @version $Id: BatchTest.java,v 1.2 2004-12-20 17:14:20 alin_sinpalean Exp $
 */
public class BatchTest extends DatabaseTestCase {
    // Constants to use instead of the JDBC 3.0-only Statement constants
    private static int SUCCESS_NO_INFO = -2;
    private static int EXECUTE_FAILED  = -3;

    public BatchTest(String name) {
        super(name);
    }

    /**
     * This test should generate an error as the second statement in the batch
     * returns a result set.
     */
    public void testResultSetError() throws Exception {
        Statement stmt = con.createStatement();
        stmt.addBatch("create table #testbatch (id int, data varchar(255))");
        stmt.addBatch("insert into #testbatch VALUES(1, 'Test line')");
        stmt.addBatch("SELECT 'This is an error'");
        int x[];
        try {
            x = stmt.executeBatch();
            fail("Expecting BatchUpdateException");
        } catch (BatchUpdateException e) {
            x = e.getUpdateCounts();
        }
        assertEquals(2, x.length);
        assertEquals(1, x[1]);
    }

    /**
     * The first statement in this batch does not return an update count.
     * SUCCESS_NO_INFO is expected instead.
     */
    public void testNoCount() throws Exception {
        Statement stmt = con.createStatement();
        stmt.addBatch("create table #testbatch (id int, data varchar(255))");
        stmt.addBatch("insert into #testbatch VALUES(1, 'Test line')");
        int x[] = stmt.executeBatch();
        assertEquals(2, x.length);
        assertEquals(SUCCESS_NO_INFO, x[0]);
        assertEquals(1, x[1]);
    }

    /**
     * Test batched statements.
     */
    public void testBatch() throws Exception {
//        DriverManager.setLogStream(System.out);
        Statement stmt = con.createStatement();
        stmt.execute("create table #testbatch (id int, data varchar(255))");
        for (int i = 0; i < 5; i++) {
            if (i == 2) {
                // This statement will generate an error
                stmt.addBatch("INSERT INTO #testbatch VALUES ('xx', 'This is line " + i + "')");
            } else {
                stmt.addBatch("INSERT INTO #testbatch VALUES (" + i + ", 'This is line " + i + "')");
            }
        }
        int x[];
        try {
            x = stmt.executeBatch();
        } catch (BatchUpdateException e) {
            x = e.getUpdateCounts();
        }
        if (con.getMetaData().getDatabaseProductName().toLowerCase().startsWith("microsoft")) {
            assertEquals(3, x.length);
            assertEquals(1, x[0]);
            assertEquals(1, x[1]);
            assertEquals(EXECUTE_FAILED, x[2]);
        } else {
            assertEquals(5, x.length);
            assertEquals(1, x[0]);
            assertEquals(1, x[1]);
            assertEquals(EXECUTE_FAILED, x[2]);
            assertEquals(1, x[3]);
            assertEquals(1, x[4]);
        }
        // Now without errors
        stmt.execute("TRUNCATE TABLE #testbatch");
        for (int i = 0; i < 5; i++) {
            stmt.addBatch("INSERT INTO #testbatch VALUES (" + i + ", 'This is line " + i + "')");
        }
        x = stmt.executeBatch();
        assertEquals(5, x.length);
        assertEquals(1, x[0]);
        assertEquals(1, x[1]);
        assertEquals(1, x[2]);
        assertEquals(1, x[3]);
        assertEquals(1, x[4]);
    }

    /**
     * Test batched prepared statements.
     */
    public void testPrepStmtBatch() throws Exception {
//        DriverManager.setLogStream(System.out);
        Statement stmt = con.createStatement();
        stmt.execute("create table #testbatch (id int, data varchar(255))");
        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #testbatch VALUES (?, ?)");
        for (int i = 0; i < 5; i++) {
            if (i == 2) {
                pstmt.setString(1, "xxx");
            } else {
                pstmt.setInt(1, i);
            }
            pstmt.setString(2, "This is line " + i);
            pstmt.addBatch();
        }
        int x[];
        try {
            x = pstmt.executeBatch();
        } catch (BatchUpdateException e) {
            x = e.getUpdateCounts();
        }
        if (con.getMetaData().getDatabaseProductName().toLowerCase().startsWith("microsoft")) {
            assertEquals(3, x.length);
            assertEquals(1, x[0]);
            assertEquals(1, x[1]);
            assertEquals(EXECUTE_FAILED, x[2]);
        } else {
            assertEquals(5, x.length);
            assertEquals(1, x[0]);
            assertEquals(1, x[1]);
            assertEquals(EXECUTE_FAILED, x[2]);
            assertEquals(1, x[3]);
            assertEquals(1, x[4]);
        }
        // Now without errors
        stmt.execute("TRUNCATE TABLE #testbatch");
        for (int i = 0; i < 5; i++) {
            pstmt.setInt(1, i);
            pstmt.setString(2, "This is line " + i);
            pstmt.addBatch();
        }
        x = pstmt.executeBatch();
        assertEquals(5, x.length);
        assertEquals(1, x[0]);
        assertEquals(1, x[1]);
        assertEquals(1, x[2]);
        assertEquals(1, x[3]);
        assertEquals(1, x[4]);
    }

    /**
     * Test batched callable statements.
     */
    public void testCallStmtBatch() throws Exception {
//        DriverManager.setLogStream(System.out);
        dropProcedure("jTDS_PROC");
        try {
            Statement stmt = con.createStatement();
            stmt.execute("create table #testbatch (id int, data varchar(255))");
            stmt.execute("create proc jTDS_PROC @p1 varchar(10), @p2 varchar(255) as " +
                    "INSERT INTO #testbatch VALUES (convert(int, @p1), @p2)");
            CallableStatement cstmt = con.prepareCall("{call jTDS_PROC (?, ?)}");
            for (int i = 0; i < 5; i++) {
                if (i == 2) {
                    cstmt.setString(1, "XXX");
                } else {
                    cstmt.setString(1, Integer.toString(i));
                }
                cstmt.setString(2, "This is line " + i);
                cstmt.addBatch();
            }
            int x[];
            try {
                x = cstmt.executeBatch();
            } catch (BatchUpdateException e) {
                x = e.getUpdateCounts();
            }
            assertEquals(3, x.length);
            assertEquals(1, x[0]);
            assertEquals(1, x[1]);
            assertEquals(EXECUTE_FAILED, x[2]);
            // Now without errors
            stmt.execute("TRUNCATE TABLE #testbatch");
            for (int i = 0; i < 5; i++) {
                cstmt.setString(1, Integer.toString(i));
                cstmt.setString(2, "This is line " + i);
                cstmt.addBatch();
            }
            try {
                x = cstmt.executeBatch();
            } catch (BatchUpdateException e) {
                x = e.getUpdateCounts();
            }
            assertEquals(5, x.length);
            assertEquals(1, x[0]);
            assertEquals(1, x[1]);
            assertEquals(1, x[2]);
            assertEquals(1, x[3]);
            assertEquals(1, x[4]);
        } finally {
            dropProcedure("jTDS_PROC");
        }
    }

    /**
     * Test batched callable statements where the call includes literal parameters which prevent the use of RPC calls.
     */
    public void testCallStmtBatch2() throws Exception {
//        DriverManager.setLogStream(System.out);
        dropProcedure("jTDS_PROC");
        try {
            Statement stmt = con.createStatement();
            stmt.execute("create table #testbatch (id int, data varchar(255))");
            stmt.execute("create proc jTDS_PROC @p1 varchar(10), @p2 varchar(255) as " +
                    "INSERT INTO #testbatch VALUES (convert(int, @p1), @p2)");
            CallableStatement cstmt = con.prepareCall("{call jTDS_PROC (?, 'literal parameter')}");
            for (int i = 0; i < 5; i++) {
                if (i == 2) {
                    cstmt.setString(1, "XXX");
                } else {
                    cstmt.setString(1, Integer.toString(i));
                }
                cstmt.addBatch();
            }
            int x[];
            try {
                x = cstmt.executeBatch();
            } catch (BatchUpdateException e) {
                x = e.getUpdateCounts();
            }
            assertEquals(3, x.length);
            assertEquals(1, x[0]);
            assertEquals(1, x[1]);
            assertEquals(EXECUTE_FAILED, x[2]);
            // Now without errors
            stmt.execute("TRUNCATE TABLE #testbatch");
            for (int i = 0; i < 5; i++) {
                cstmt.setString(1, Integer.toString(i));
                cstmt.addBatch();
            }
            try {
                x = cstmt.executeBatch();
            } catch (BatchUpdateException e) {
                x = e.getUpdateCounts();
            }
            assertEquals(5, x.length);
            assertEquals(1, x[0]);
            assertEquals(1, x[1]);
            assertEquals(1, x[2]);
            assertEquals(1, x[3]);
            assertEquals(1, x[4]);
        } finally {
            dropProcedure("jTDS_PROC");
        }
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(BatchTest.class);
    }
}
