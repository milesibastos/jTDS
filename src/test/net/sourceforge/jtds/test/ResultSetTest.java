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
import java.math.BigDecimal;
import java.io.InputStream;
import java.util.ArrayList;

/**
 * @version 1.0
 */
public class ResultSetTest extends TestBase {
    public ResultSetTest(String name) {
        super(name);
    }

    /**
     * Test BIT data type.
     */
    public void testGetObject1() throws Exception {
        boolean data = true;

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #getObject1 (data BIT, minval BIT, maxval BIT)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #getObject1 (data, minval, maxval) VALUES (?, ?, ?)");

        pstmt.setBoolean(1, data);
        pstmt.setBoolean(2, false);
        pstmt.setBoolean(3, true);
        assertEquals(1, pstmt.executeUpdate());

        pstmt.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data, minval, maxval FROM #getObject1");

        assertTrue(rs.next());

        assertTrue(rs.getBoolean(1));
        assertTrue(rs.getByte(1) == 1);
        assertTrue(rs.getShort(1) == 1);
        assertTrue(rs.getInt(1) == 1);
        assertTrue(rs.getLong(1) == 1);
        assertTrue(rs.getFloat(1) == 1);
        assertTrue(rs.getDouble(1) == 1);
        assertTrue(rs.getBigDecimal(1).byteValue() == 1);
        assertEquals("1", rs.getString(1));

        Object tmpData = rs.getObject(1);

        assertTrue(tmpData instanceof Boolean);
        assertEquals(true, ((Boolean) tmpData).booleanValue());

        ResultSetMetaData resultSetMetaData = rs.getMetaData();

        assertNotNull(resultSetMetaData);
        assertEquals(Types.BIT, resultSetMetaData.getColumnType(1));

        assertFalse(rs.getBoolean(2));
        assertTrue(rs.getBoolean(3));

        assertFalse(rs.next());
        stmt2.close();
        rs.close();
    }

    /**
     * Test TINYINT data type.
     */
    public void testGetObject2() throws Exception {
        byte data = 1;

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #getObject2 (data TINYINT, minval TINYINT, maxval TINYINT)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #getObject2 (data, minval, maxval) VALUES (?, ?, ?)");

        pstmt.setByte(1, data);
        pstmt.setByte(2, Byte.MIN_VALUE);
        pstmt.setByte(3, Byte.MAX_VALUE);
        assertEquals(1, pstmt.executeUpdate());

        pstmt.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data, minval, maxval FROM #getObject2");

        assertTrue(rs.next());

        assertTrue(rs.getBoolean(1));
        assertTrue(rs.getByte(1) == 1);
        assertTrue(rs.getShort(1) == 1);
        assertTrue(rs.getInt(1) == 1);
        assertTrue(rs.getLong(1) == 1);
        assertTrue(rs.getFloat(1) == 1);
        assertTrue(rs.getDouble(1) == 1);
        assertTrue(rs.getBigDecimal(1).byteValue() == 1);
        assertEquals("1", rs.getString(1));

        Object tmpData = rs.getObject(1);

        assertTrue(tmpData instanceof Integer);
        assertEquals(data, ((Integer) tmpData).byteValue());

        ResultSetMetaData resultSetMetaData = rs.getMetaData();

        assertNotNull(resultSetMetaData);
        assertEquals(Types.TINYINT, resultSetMetaData.getColumnType(1));

        assertEquals(rs.getByte(2), Byte.MIN_VALUE);
        assertEquals(rs.getByte(3), Byte.MAX_VALUE);

        assertFalse(rs.next());
        stmt2.close();
        rs.close();
    }

    /**
     * Test SMALLINT data type.
     */
    public void testGetObject3() throws Exception {
        short data = 1;

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #getObject3 (data SMALLINT, minval SMALLINT, maxval SMALLINT)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #getObject3 (data, minval, maxval) VALUES (?, ?, ?)");

        pstmt.setShort(1, data);
        pstmt.setShort(2, Short.MIN_VALUE);
        pstmt.setShort(3, Short.MAX_VALUE);
        assertEquals(1, pstmt.executeUpdate());

        pstmt.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data, minval, maxval FROM #getObject3");

        assertTrue(rs.next());

        assertTrue(rs.getBoolean(1));
        assertTrue(rs.getByte(1) == 1);
        assertTrue(rs.getShort(1) == 1);
        assertTrue(rs.getInt(1) == 1);
        assertTrue(rs.getLong(1) == 1);
        assertTrue(rs.getFloat(1) == 1);
        assertTrue(rs.getDouble(1) == 1);
        assertTrue(rs.getBigDecimal(1).shortValue() == 1);
        assertEquals("1", rs.getString(1));

        Object tmpData = rs.getObject(1);

        assertTrue(tmpData instanceof Integer);
        assertEquals(data, ((Integer) tmpData).shortValue());

        ResultSetMetaData resultSetMetaData = rs.getMetaData();

        assertNotNull(resultSetMetaData);
        assertEquals(Types.SMALLINT, resultSetMetaData.getColumnType(1));

        assertEquals(rs.getShort(2), Short.MIN_VALUE);
        assertEquals(rs.getShort(3), Short.MAX_VALUE);

        assertFalse(rs.next());
        stmt2.close();
        rs.close();
    }

    /**
     * Test INT data type.
     */
    public void testGetObject4() throws Exception {
        int data = 1;

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #getObject4 (data INT, minval INT, maxval INT)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #getObject4 (data, minval, maxval) VALUES (?, ?, ?)");

        pstmt.setInt(1, data);
        pstmt.setInt(2, Integer.MIN_VALUE);
        pstmt.setInt(3, Integer.MAX_VALUE);
        assertEquals(1, pstmt.executeUpdate());

        pstmt.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data, minval, maxval FROM #getObject4");

        assertTrue(rs.next());

        assertTrue(rs.getBoolean(1));
        assertTrue(rs.getByte(1) == 1);
        assertTrue(rs.getShort(1) == 1);
        assertTrue(rs.getInt(1) == 1);
        assertTrue(rs.getLong(1) == 1);
        assertTrue(rs.getFloat(1) == 1);
        assertTrue(rs.getDouble(1) == 1);
        assertTrue(rs.getBigDecimal(1).intValue() == 1);
        assertEquals("1", rs.getString(1));

        Object tmpData = rs.getObject(1);

        assertTrue(tmpData instanceof Integer);
        assertEquals(data, ((Integer) tmpData).intValue());

        ResultSetMetaData resultSetMetaData = rs.getMetaData();

        assertNotNull(resultSetMetaData);
        assertEquals(Types.INTEGER, resultSetMetaData.getColumnType(1));

        assertEquals(rs.getInt(2), Integer.MIN_VALUE);
        assertEquals(rs.getInt(3), Integer.MAX_VALUE);

        assertFalse(rs.next());
        stmt2.close();
        rs.close();
    }

    /**
     * Test BIGINT data type.
     */
    public void testGetObject5() throws Exception {
        long data = 1;

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #getObject5 (data DECIMAL(28, 0), minval DECIMAL(28, 0), maxval DECIMAL(28, 0))");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #getObject5 (data, minval, maxval) VALUES (?, ?, ?)");

        pstmt.setLong(1, data);
        pstmt.setLong(2, Long.MIN_VALUE);
        pstmt.setLong(3, Long.MAX_VALUE);
        assertEquals(1, pstmt.executeUpdate());

        pstmt.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data, minval, maxval FROM #getObject5");

        assertTrue(rs.next());

        assertTrue(rs.getBoolean(1));
        assertTrue(rs.getByte(1) == 1);
        assertTrue(rs.getShort(1) == 1);
        assertTrue(rs.getInt(1) == 1);
        assertTrue(rs.getLong(1) == 1);
        assertTrue(rs.getFloat(1) == 1);
        assertTrue(rs.getDouble(1) == 1);
        assertTrue(rs.getBigDecimal(1).longValue() == 1);
        assertEquals("1", rs.getString(1));

        Object tmpData = rs.getObject(1);

        assertTrue(tmpData instanceof BigDecimal);
        assertEquals(data, ((BigDecimal) tmpData).longValue());

        ResultSetMetaData resultSetMetaData = rs.getMetaData();

        assertNotNull(resultSetMetaData);
        assertEquals(Types.DECIMAL, resultSetMetaData.getColumnType(1));

        assertEquals(rs.getLong(2), Long.MIN_VALUE);
        assertEquals(rs.getLong(3), Long.MAX_VALUE);

        assertFalse(rs.next());
        stmt2.close();
        rs.close();
    }

    /**
     * Test for bug [961594] ResultSet.
     */
    public void testResultSetScroll1() throws Exception {
    	int count = 125;

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #resultSetScroll1 (data INT)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #resultSetScroll1 (data) VALUES (?)");

        for (int i = 1; i <= count; i++) {
            pstmt.setInt(1, i);
            assertEquals(1, pstmt.executeUpdate());
        }

        pstmt.close();

        Statement stmt2 = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
        		ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = stmt2.executeQuery("SELECT data FROM #resultSetScroll1");

        assertTrue(rs.last());
        assertEquals(count, rs.getRow());

        stmt2.close();
        rs.close();
    }

    /**
     * Test for bug [945462] getResultSet() return null if you use scrollable/updatable.
     */
    public void testResultSetScroll2() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #resultSetScroll2 (data INT)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #resultSetScroll2 (data) VALUES (?)");

        pstmt.setInt(1, 1);
        assertEquals(1, pstmt.executeUpdate());

        pstmt.close();

        Statement stmt2 = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_UPDATABLE);
        stmt2.executeQuery("SELECT data FROM #resultSetScroll2");

        ResultSet rs = stmt2.getResultSet();

        assertNotNull(rs);
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertFalse(rs.next());

        stmt2.close();
        rs.close();
    }

    /**
     * Test for bug [1028881] statement.execute() causes wrong ResultSet type.
     */
    public void testResultSetScroll3() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #resultSetScroll3 (data INT)");
        stmt.execute("CREATE PROCEDURE #procResultSetScroll3 AS SELECT data FROM #resultSetScroll3");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #resultSetScroll3 (data) VALUES (?)");
        pstmt.setInt(1, 1);
        assertEquals(1, pstmt.executeUpdate());
        pstmt.close();

        // Test plain Statement
        Statement stmt2 = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
        assertTrue("Was expecting a ResultSet", stmt2.execute("SELECT data FROM #resultSetScroll3"));

        ResultSet rs = stmt2.getResultSet();
        assertEquals("ResultSet not scrollable", ResultSet.TYPE_SCROLL_INSENSITIVE, rs.getType());

        rs.close();
        stmt2.close();

        // Test PreparedStatement
        pstmt = con.prepareStatement("SELECT data FROM #resultSetScroll3", ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
        assertTrue("Was expecting a ResultSet", pstmt.execute());

        rs = pstmt.getResultSet();
        assertEquals("ResultSet not scrollable", ResultSet.TYPE_SCROLL_INSENSITIVE, rs.getType());

        rs.close();
        pstmt.close();

        // Test CallableStatement
        CallableStatement cstmt = con.prepareCall("{call #procResultSetScroll3}",
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        assertTrue("Was expecting a ResultSet", cstmt.execute());

        rs = cstmt.getResultSet();
        assertEquals("ResultSet not scrollable", ResultSet.TYPE_SCROLL_INSENSITIVE, rs.getType());

        rs.close();
        cstmt.close();
    }

    /**
     * Test for bug [1008208] 0.9-rc1 updateNull doesn't work.
     */
    public void testResultSetUpdate1() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #resultSetUpdate1 (id INT PRIMARY KEY, dsi SMALLINT NULL, di INT NULL)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #resultSetUpdate1 (id, dsi, di) VALUES (?, ?, ?)");

        pstmt.setInt(1, 1);
        pstmt.setShort(2, (short) 1);
        pstmt.setInt(3, 1);
        assertEquals(1, pstmt.executeUpdate());

        pstmt.close();

        stmt = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        stmt.executeQuery("SELECT id, dsi, di FROM #resultSetUpdate1");

        ResultSet rs = stmt.getResultSet();

        assertNotNull(rs);
        assertTrue(rs.next());
        rs.updateNull("dsi");
        rs.updateNull("di");
        rs.updateRow();
        rs.moveToInsertRow();
        rs.updateInt(1, 2);
        rs.updateNull("dsi");
        rs.updateNull("di");
        rs.insertRow();

        stmt.close();
        rs.close();

        stmt = con.createStatement();
        stmt.executeQuery("SELECT id, dsi, di FROM #resultSetUpdate1 ORDER BY id");

        rs = stmt.getResultSet();

        assertNotNull(rs);
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        rs.getShort(2);
        assertTrue(rs.wasNull());
        rs.getInt(3);
        assertTrue(rs.wasNull());
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        rs.getShort(2);
        assertTrue(rs.wasNull());
        rs.getInt(3);
        assertTrue(rs.wasNull());
        assertFalse(rs.next());

        stmt.close();
        rs.close();
    }

    /**
     * Test for bug [1009233] ResultSet getColumnName, getColumnLabel return wrong values
     */
    public void testResultSetColumnName1() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #resultSetCN1 (data INT)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #resultSetCN1 (data) VALUES (?)");

        pstmt.setInt(1, 1);
        assertEquals(1, pstmt.executeUpdate());

        pstmt.close();

        Statement stmt2 = con.createStatement();
        stmt2.executeQuery("SELECT data as test FROM #resultSetCN1");

        ResultSet rs = stmt2.getResultSet();

        assertNotNull(rs);
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("test"));
        assertFalse(rs.next());

        stmt2.close();
        rs.close();
    }

    /**
     * Test for fixed bugs in ResultSetMetaData:
     * <ol>
     * <li>isNullable() always returns columnNoNulls.
     * <li>isSigned returns true in error for TINYINT columns.
     * <li>Type names for numeric / decimal have (prec,scale) appended in error.
     * <li>Type names for auto increment columns do not have "identity" appended.
     * </ol>
     * NB: This test assumes getColumnName has been fixed to work as per the suggestion
     * in bug report [1009233].
     *
     * @throws Exception
     */
    public void testResultSetMetaData() throws Exception {
        Statement stmt = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        stmt.execute("CREATE TABLE #TRSMD (id INT IDENTITY NOT NULL, byte TINYINT NOT NULL, num DECIMAL(28,10) NULL)");
        ResultSetMetaData rsmd = stmt.executeQuery("SELECT id as idx, byte, num FROM #TRSMD").getMetaData();
        assertNotNull(rsmd);
        // Check id
        assertEquals("idx", rsmd.getColumnName(1)); // no longer returns base name
        assertEquals("idx", rsmd.getColumnLabel(1));
        assertTrue(rsmd.isAutoIncrement(1));
        assertTrue(rsmd.isSigned(1));
        assertEquals(ResultSetMetaData.columnNoNulls, rsmd.isNullable(1));
        assertEquals("int identity", rsmd.getColumnTypeName(1));
        assertEquals(Types.INTEGER, rsmd.getColumnType(1));
        // Check byte
        assertFalse(rsmd.isAutoIncrement(2));
        assertFalse(rsmd.isSigned(2));
        assertEquals(ResultSetMetaData.columnNoNulls, rsmd.isNullable(2));
        assertEquals("tinyint", rsmd.getColumnTypeName(2));
        assertEquals(Types.TINYINT, rsmd.getColumnType(2));
        // Check num
        assertFalse(rsmd.isAutoIncrement(3));
        assertTrue(rsmd.isSigned(3));
        assertEquals(ResultSetMetaData.columnNullable, rsmd.isNullable(3));
        assertEquals("decimal", rsmd.getColumnTypeName(3));
        assertEquals(Types.DECIMAL, rsmd.getColumnType(3));
        stmt.close();
    }

    /**
     * Test for bug [1022445] Cursor downgrade warning not raised.
     */
    public void testCursorWarning() throws Exception
    {
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #TESTCW (id INT PRIMARY KEY, DATA VARCHAR(255))");
        stmt.execute("CREATE PROC #SPTESTCW @P0 INT OUTPUT AS SELECT * FROM #TESTCW");
        stmt.close();
        CallableStatement cstmt = con.prepareCall("{call #SPTESTCW(?)}",
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
        cstmt.registerOutParameter(1, Types.INTEGER);
        ResultSet rs = cstmt.executeQuery();
        // This should generate a ResultSet type/concurrency downgraded error.
        assertNotNull(rs.getWarnings());
        cstmt.close();
    }

    /**
     * Test whether retrieval by name returns the first occurence (that's what
     * the spec requires).
     */
    public void testGetByName() throws Exception
    {
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1 myCol, 2 myCol, 3 myCol");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("myCol"));
        assertFalse(rs.next());
        stmt.close();
    }

    /**
     * Test if COL_INFO packets are processed correctly for
     * <code>ResultSet</code>s with over 255 columns.
     */
    public void testMoreThan255Columns() throws Exception
    {
        Statement stmt = con.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_UPDATABLE);

        // create the table
        int cols = 260;
        StringBuffer create = new StringBuffer("create table #manycolumns (");
        for (int i=0; i<cols; ++i) {
            create.append("col" + i + " char(10), ") ;
        }
        create.append(")");
        stmt.executeUpdate(create.toString());

        String query = "select * from #manycolumns";
        ResultSet rs = stmt.executeQuery(query);
        rs.close();
        stmt.close();
    }

    /**
     * Test that <code>insertRow()</code> works with no values set.
     */
    public void testEmptyInsertRow() throws Exception
    {
        int rows = 10;
        Statement stmt = con.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_UPDATABLE);

        stmt.executeUpdate(
                "create table #emptyInsertRow (id int identity, val int default 10)");
        ResultSet rs = stmt.executeQuery("select * from #emptyInsertRow");

        for (int i=0; i<rows; i++) {
            rs.moveToInsertRow();
            rs.insertRow();
        }
        rs.close();

        rs = stmt.executeQuery("select count(*) from #emptyInsertRow");
        assertTrue(rs.next());
        assertEquals(rows, rs.getInt(1));
        rs.close();

        rs = stmt.executeQuery("select * from #emptyInsertRow order by id");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(10, rs.getInt(2));
        rs.close();
        stmt.close();
    }

    /**
     * Test that inserted rows are visible in a scroll sensitive
     * <code>ResultSet</code> and that they show up at the end.
     */
    public void testInsertRowVisible() throws Exception
    {
        int rows = 10;
        Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                ResultSet.CONCUR_UPDATABLE);

        stmt.executeUpdate(
                "create table #insertRowNotVisible (val int primary key)");
        ResultSet rs = stmt.executeQuery("select * from #insertRowNotVisible");

        for (int i = 1; i <= rows; i++) {
            rs.moveToInsertRow();
            rs.updateInt(1, i);
            rs.insertRow();
            rs.moveToCurrentRow();
            rs.last();
            assertEquals(i, rs.getRow());
        }

        rs.close();
        stmt.close();
    }

    /**
     * Test that updated rows are marked as deleted and the new values inserted
     * at the end of the <code>ResultSet</code> if the primary key is updated.
     */
    public void testUpdateRowDuplicatesRow() throws Exception
    {
        Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                ResultSet.CONCUR_UPDATABLE);

        stmt.executeUpdate(
                "create table #updateRowDuplicatesRow (val int primary key)");
        stmt.executeUpdate(
                "insert into #updateRowDuplicatesRow (val) values (1)");
        stmt.executeUpdate(
                "insert into #updateRowDuplicatesRow (val) values (2)");
        stmt.executeUpdate(
                "insert into #updateRowDuplicatesRow (val) values (3)");

        ResultSet rs = stmt.executeQuery(
                "select val from #updateRowDuplicatesRow order by val");

        for (int i = 0; i < 3; i++) {
            assertTrue(rs.next());
            assertFalse(rs.rowUpdated());
            assertFalse(rs.rowInserted());
            assertFalse(rs.rowDeleted());
            rs.updateInt(1, rs.getInt(1) + 10);
            rs.updateRow();
            assertFalse(rs.rowUpdated());
            assertFalse(rs.rowInserted());
            assertTrue(rs.rowDeleted());
        }

        for (int i = 11; i <= 13; i++) {
            assertTrue(rs.next());
            assertFalse(rs.rowUpdated());
            assertFalse(rs.rowInserted());
            assertFalse(rs.rowDeleted());
            assertEquals(i, rs.getInt(1));
        }

        rs.close();
        stmt.close();
    }

    /**
     * Test that updated rows are modified in place if the primary key is not
     * updated.
     */
    public void testUpdateRowUpdatesRow() throws Exception
    {
        Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                ResultSet.CONCUR_UPDATABLE);

        stmt.executeUpdate(
                "create table #updateRowUpdatesRow (id int primary key, val int)");
        stmt.executeUpdate(
                "insert into #updateRowUpdatesRow (id, val) values (1, 1)");
        stmt.executeUpdate(
                "insert into #updateRowUpdatesRow (id, val) values (2, 2)");
        stmt.executeUpdate(
                "insert into #updateRowUpdatesRow (id, val) values (3, 3)");

        ResultSet rs = stmt.executeQuery(
                "select id, val from #updateRowUpdatesRow order by id");

        for (int i = 0; i < 3; i++) {
            assertTrue(rs.next());
            assertFalse(rs.rowUpdated());
            assertFalse(rs.rowInserted());
            assertFalse(rs.rowDeleted());
            rs.updateInt(2, rs.getInt(2) + 10);
            rs.updateRow();
            assertFalse(rs.rowUpdated());
            assertFalse(rs.rowInserted());
            assertFalse(rs.rowDeleted());
            assertEquals(rs.getInt(1) + 10, rs.getInt(2));
        }

        assertFalse(rs.next());

        rs.close();
        stmt.close();
    }

    /**
     * Test that deleted rows are not removed but rather marked as deleted.
     */
    public void testDeleteRowMarksDeleted() throws Exception
    {
        Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                ResultSet.CONCUR_UPDATABLE);

        stmt.executeUpdate(
                "create table #deleteRowMarksDeleted (val int primary key)");
        stmt.executeUpdate(
                "insert into #deleteRowMarksDeleted (val) values (1)");
        stmt.executeUpdate(
                "insert into #deleteRowMarksDeleted (val) values (2)");
        stmt.executeUpdate(
                "insert into #deleteRowMarksDeleted (val) values (3)");

        ResultSet rs = stmt.executeQuery(
                "select val from #deleteRowMarksDeleted order by val");

        for (int i = 0; i < 3; i++) {
            assertTrue(rs.next());
            assertFalse(rs.rowUpdated());
            assertFalse(rs.rowInserted());
            assertFalse(rs.rowDeleted());
            rs.deleteRow();
            assertFalse(rs.rowUpdated());
            assertFalse(rs.rowInserted());
            assertTrue(rs.rowDeleted());
        }

        assertFalse(rs.next());
        rs.close();
        stmt.close();
    }

    /**
     * Test for bug [1170777] resultSet.updateRow() fails if no row has been
     * changed.
     */
    public void testUpdateRowNoChanges() throws Exception {
        Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                ResultSet.CONCUR_UPDATABLE);

        stmt.executeUpdate(
                "create table #deleteRowMarksDeleted (val int primary key)");
        stmt.executeUpdate(
                "insert into #deleteRowMarksDeleted (val) values (1)");

        ResultSet rs = stmt.executeQuery(
                "select val from #deleteRowMarksDeleted order by val");
        assertTrue(rs.next());
        // This should not crash; it should be a no-op
        rs.updateRow();
        rs.refreshRow();
        assertEquals(1, rs.getInt(1));
        assertFalse(rs.next());

        rs.close();
        stmt.close();
    }

    /**
     * Test the behavior of <code>sp_cursorfetch</code> with fetch sizes
     * greater than 1.
     * <p>
     * <b>Assertions tested:</b>
     * <ul>
     *   <li>The <i>current row</i> is always the first row returned by the
     *     last fetch, regardless of what fetch type was used.
     *   <li>Row number parameter is ignored by fetch types other than absolute
     *     and relative.
     *   <li>Refresh fetch type simply reruns the previous request (it ignores
     *     both row number and number of rows) and will not affect the
     *     <i>current row</i>.
     *   <li>Fetch next returns the packet of rows right after the last row
     *     returned by the last fetch (regardless of what type of fetch that
     *     was).
     *   <li>Fetch previous returns the packet of rows right before the first
     *     row returned by the last fetch (regardless of what type of fetch
     *     that was).
     *   <li>If a fetch previous tries to read before the start of the
     *     <code>ResultSet</code> the requested number of rows is returned,
     *     starting with row 1 and the error code returned is non-zero (2).
     * </ul>
     */
    public void testCursorFetch() throws Exception
    {
        int rows = 10;
        Statement stmt = con.createStatement();
        stmt.executeUpdate(
                "create table #testCursorFetch (id int primary key, val int)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement(
                "insert into #testCursorFetch (id, val) values (?, ?)");
        for (int i = 1; i <= rows; i++) {
            pstmt.setInt(1, i);
            pstmt.setInt(2, i);
            pstmt.executeUpdate();
        }
        pstmt.close();

        //
        // Open cursor
        //
        CallableStatement cstmt = con.prepareCall(
                "{?=call sp_cursoropen(?, ?, ?, ?, ?)}");
        // Return value (OUT)
        cstmt.registerOutParameter(1, Types.INTEGER);
        // Cursor handle (OUT)
        cstmt.registerOutParameter(2, Types.INTEGER);
        // Statement (IN)
        cstmt.setString(3, "select * from #testCursorFetch order by id");
        // Scroll options (INOUT)
        cstmt.setInt(4, 1); // Keyset driven
        cstmt.registerOutParameter(4, Types.INTEGER);
        // Concurrency options (INOUT)
        cstmt.setInt(5, 2); // Scroll locks
        cstmt.registerOutParameter(5, Types.INTEGER);
        // Row count (OUT)
        cstmt.registerOutParameter(6, Types.INTEGER);

        ResultSet rs = cstmt.executeQuery();
        assertEquals(2, rs.getMetaData().getColumnCount());
        assertFalse(rs.next());

        assertEquals(0, cstmt.getInt(1));
        int cursor = cstmt.getInt(2);
        assertEquals(1, cstmt.getInt(4));
        assertEquals(2, cstmt.getInt(5));
        assertEquals(rows, cstmt.getInt(6));

        cstmt.close();

        //
        // Play around with fetch
        //
        cstmt = con.prepareCall("{?=call sp_cursorfetch(?, ?, ?, ?)}");
        // Return value (OUT)
        cstmt.registerOutParameter(1, Types.INTEGER);
        // Cursor handle (IN)
        cstmt.setInt(2, cursor);
        // Fetch type (IN)
        cstmt.setInt(3, 2); // Next row
        // Row number (INOUT)
        cstmt.setInt(4, 1); // Only matters for absolute and relative fetching
        // Number of rows (INOUT)
        cstmt.setInt(5, 2); // Read 2 rows

        // Fetch rows 1-2 (current row is 1)
        rs = cstmt.executeQuery();
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertFalse(rs.next());
        rs.close();
        assertEquals(0, cstmt.getInt(1));

        // Fetch rows 3-4 (current row is 3)
        rs = cstmt.executeQuery();
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));
        assertFalse(rs.next());
        rs.close();
        assertEquals(0, cstmt.getInt(1));

        // Refresh rows 3-4 (current row is 3)
        cstmt.setInt(3, 0x80); // Refresh
        cstmt.setInt(4, 2);    // Try to refresh only 2nd row (will be ignored)
        cstmt.setInt(5, 1);    // Try to refresh only 1 row (will be ignored)
        rs = cstmt.executeQuery();
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));
        assertFalse(rs.next());
        rs.close();
        assertEquals(0, cstmt.getInt(1));

        // Fetch rows 5-6 (current row is 5)
        cstmt.setInt(3, 2); // Next
        cstmt.setInt(4, 1); // Row number 1
        cstmt.setInt(5, 2); // Get 2 rows
        rs = cstmt.executeQuery();
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertEquals(6, rs.getInt(1));
        assertFalse(rs.next());
        rs.close();
        assertEquals(0, cstmt.getInt(1));

        // Fetch previous rows (3-4) (current row is 3)
        cstmt.setInt(3, 4); // Previous
        rs = cstmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));
        assertFalse(rs.next());
        rs.close();
        assertEquals(0, cstmt.getInt(1));

        // Refresh rows 3-4 (current row is 3)
        cstmt.setInt(3, 0x80); // Refresh
        rs = cstmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));
        assertFalse(rs.next());
        rs.close();
        assertEquals(0, cstmt.getInt(1));

        // Fetch previous rows (1-2) (current row is 1)
        cstmt.setInt(3, 4); // Previous
        rs = cstmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertFalse(rs.next());
        rs.close();
        assertEquals(0, cstmt.getInt(1));

        // Fetch next rows (3-4) (current row is 3)
        cstmt.setInt(3, 2); // Next
        rs = cstmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));
        assertFalse(rs.next());
        rs.close();
        assertEquals(0, cstmt.getInt(1));

        // Fetch first rows (1-2) (current row is 1)
        cstmt.setInt(3, 1); // First
        rs = cstmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertFalse(rs.next());
        rs.close();
        assertEquals(0, cstmt.getInt(1));

        // Fetch last rows (9-10) (current row is 9)
        cstmt.setInt(3, 8); // Last
        rs = cstmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(9, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(10, rs.getInt(1));
        assertFalse(rs.next());
        rs.close();
        assertEquals(0, cstmt.getInt(1));

        // Fetch next rows; should not fail (current position is after last)
        cstmt.setInt(3, 2); // Next
        rs = cstmt.executeQuery();
        assertFalse(rs.next());
        rs.close();
        assertEquals(0, cstmt.getInt(1));

        // Fetch absolute starting with 6 (6-7) (current row is 6)
        cstmt.setInt(3, 0x10); // Absolute
        cstmt.setInt(4, 6);
        rs = cstmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(6, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(7, rs.getInt(1));
        assertFalse(rs.next());
        rs.close();
        assertEquals(0, cstmt.getInt(1));

        // Fetch relative -4 (2-3) (current row is 2)
        cstmt.setInt(3, 0x20); // Relative
        cstmt.setInt(4, -4);
        rs = cstmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertFalse(rs.next());
        rs.close();
        assertEquals(0, cstmt.getInt(1));

        // Fetch previous 2 rows; should fail (current row is 1)
        cstmt.setInt(3, 4); // Previous
        rs = cstmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertFalse(rs.next());
        rs.close();
        // Returns 2 on error
        assertEquals(2, cstmt.getInt(1));

        // Fetch next rows (3-4) (current row is 3)
        cstmt.setInt(3, 2); // Next
        rs = cstmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));
        assertFalse(rs.next());
        rs.close();
        assertEquals(0, cstmt.getInt(1));

        cstmt.close();

        //
        // Close cursor
        //
        cstmt = con.prepareCall("{?=call sp_cursorclose(?)}");
        // Return value (OUT)
        cstmt.registerOutParameter(1, Types.INTEGER);
        // Cursor handle (IN)
        cstmt.setInt(2, cursor);
        assertFalse(cstmt.execute());
        assertEquals(0, cstmt.getInt(1));
        cstmt.close();
    }

    /**
     * Test that <code>absolute(-1)</code> works the same as <code>last()</code>.
     */
    public void testAbsoluteMinusOne() throws Exception {
        Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);

        stmt.executeUpdate(
                "create table #absoluteMinusOne (val int primary key)");
        stmt.executeUpdate(
                "insert into #absoluteMinusOne (val) values (1)");
        stmt.executeUpdate(
                "insert into #absoluteMinusOne (val) values (2)");
        stmt.executeUpdate(
                "insert into #absoluteMinusOne (val) values (3)");

        ResultSet rs = stmt.executeQuery(
                "select val from #absoluteMinusOne order by val");

        rs.absolute(-1);
        assertTrue(rs.isLast());
        assertEquals(3, rs.getInt(1));
        assertFalse(rs.next());

        rs.last();
        assertTrue(rs.isLast());
        assertEquals(3, rs.getInt(1));
        assertFalse(rs.next());

        rs.close();
        stmt.close();
    }

    /**
     * Test that <code>read()</code> works ok on the stream returned by
     * <code>ResultSet.getUnicodeStream()</code> (i.e. it doesn't always fill
     * the buffer, regardless of whether there's available data or not).
     */
    public void testUnicodeStream() throws Exception {
        Statement stmt = con.createStatement();
        stmt.executeUpdate("create table #unicodeStream (val varchar(255))");
        stmt.executeUpdate("insert into #unicodeStream (val) values ('test')");
        ResultSet rs = stmt.executeQuery("select val from #unicodeStream");

        if (rs.next()) {
            byte[] buf = new byte[8000];
            InputStream is = rs.getUnicodeStream(1);
            int length = is.read(buf);
            assertEquals(4 * 2, length);
        }

        rs.close();
        stmt.close();
    }

    /**
     * Test that <code>Statement.setMaxRows()</code> works on cursor
     * <code>ResultSet</code>s.
     */
    public void testCursorMaxRows() throws Exception {
        Statement stmt = con.createStatement();
        stmt.executeUpdate("create table #cursorMaxRows (val int)");
        stmt.close();

        // Insert 10 rows
        PreparedStatement pstmt = con.prepareStatement(
                "insert into #cursorMaxRows (val) values (?)");
        for (int i = 0; i < 10; i++) {
            pstmt.setInt(1, i);
            assertEquals(1, pstmt.executeUpdate());
        }
        pstmt.close();

        // Create a cursor ResultSet
        stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        // Set maxRows to 5
        stmt.setMaxRows(5);

        // Select all (should only return 5 rows)
        ResultSet rs = stmt.executeQuery("select * from #cursorMaxRows");
        rs.last();
        assertEquals(5, rs.getRow());
        rs.beforeFirst();

        int cnt = 0;
        while (rs.next()) {
            cnt++;
        }
        assertEquals(5, cnt);

        rs.close();
        stmt.close();
    }

    /**
     * Test for bug [1075977] <code>setObject()</code> causes SQLException.
     * <p>
     * Conversion of <code>float</code> values to <code>String</code> adds
     * grouping to the value, which cannot then be parsed.
     */
    public void testSetObjectScale() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("create table #testsetobj (i int)");
        PreparedStatement pstmt =
                con.prepareStatement("insert into #testsetobj values(?)");
        // next line causes sqlexception
        pstmt.setObject(1, new Float(1234.5667), Types.INTEGER, 0);
        assertEquals(1, pstmt.executeUpdate());
        ResultSet rs = stmt.executeQuery("select * from #testsetobj");
        assertTrue(rs.next());
        assertEquals("1234", rs.getString(1));
    }

    /**
     * Test that <code>ResultSet.previous()</code> works correctly on cursor
     * <code>ResultSet</code>s.
     */
    public void testCursorPrevious() throws Exception {
        Statement stmt = con.createStatement();
        stmt.executeUpdate("create table #cursorPrevious (val int)");
        stmt.close();

        // Insert 10 rows
        PreparedStatement pstmt = con.prepareStatement(
                "insert into #cursorPrevious (val) values (?)");
        for (int i = 0; i < 10; i++) {
            pstmt.setInt(1, i);
            assertEquals(1, pstmt.executeUpdate());
        }
        pstmt.close();

        // Create a cursor ResultSet
        stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        // Set fetch size to 2
        stmt.setFetchSize(2);

        // Select all
        ResultSet rs = stmt.executeQuery("select * from #cursorPrevious");
        rs.last();
        int i = 10;
        do {
            assertEquals(i, rs.getRow());
            assertEquals(--i, rs.getInt(1));
        } while (rs.previous());
        assertTrue(rs.isBeforeFirst());
        assertEquals(0, i);

        rs.close();
        stmt.close();
    }

    /**
     * Test the behavior of the ResultSet/Statement/Connection when the JVM
     * runs out of memory (hopefully) in the middle of a packet.
     * <p/>
     * Previously jTDS was not able to close a ResultSet/Statement/Connection
     * after an OutOfMemoryError because the input stream pointer usually
     * remained inside a packet and further attempts to dump the rest of the
     * response failed because of "protocol confusions".
     */
    public void testOutOfMemory() throws SQLException {
        Statement stmt = con.createStatement();
        stmt.executeUpdate("create table #testOutOfMemory (val binary(8000))");

        // Insert a 8KB value
        byte[] val = new byte[8000];
        PreparedStatement pstmt = con.prepareStatement(
                "insert into #testOutOfMemory (val) values (?)");
        pstmt.setBytes(1, val);
        assertEquals(1, pstmt.executeUpdate());
        pstmt.close();

        // Create a list and keep adding rows to it until we run out of memory
        // Most probably this will happen in the middle of a row packet, when
        // jTDS tries to allocate the array, after reading the data length
        ArrayList results = new ArrayList();
        ResultSet rs = null;
        try {
            while (true) {
                rs = stmt.executeQuery("select val from #testOutOfMemory");
                assertTrue(rs.next());
                results.add(rs.getBytes(1));
                assertFalse(rs.next());
                rs.close();
                rs = null;
            }
        } catch (OutOfMemoryError err) {
            results = null;
            if (rs != null) {
                // This used to fail, because the parser got confused
                rs.close();
            }
        }

        // Make sure the Statement still works
        rs = stmt.executeQuery("select 1");
        assertTrue(rs.next());
        assertFalse(rs.next());
        rs.close();
        stmt.close();
    }

    /**
     * Test return of multiple open result sets from one execute.
     */
    public void testMultipleResults() throws Exception {
        Statement stmt = con.createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        //
        // Create 4 test tables
        //
        for (int rs = 1; rs <= 4; rs++) {
            stmt.execute("CREATE TABLE #TESTRS" + rs + " (id int, data varchar(255))");
            for (int row = 1; row <= 10; row++) {
                assertEquals(1, stmt.executeUpdate("INSERT INTO #TESTRS" + rs +
                        " VALUES(" + row + ", 'TABLE " + rs + " ROW " + row + "')"));
            }
        }

        assertTrue(stmt.execute(
                "SELECT * FROM #TESTRS1\r\n" +
                "SELECT * FROM #TESTRS2\r\n" +
                "SELECT * FROM #TESTRS3\r\n" +
                "SELECT * FROM #TESTRS4\r\n"));
        ResultSet rs = stmt.getResultSet();
        assertTrue(rs.next());
        assertEquals("TABLE 1 ROW 1", rs.getString(2));
        // Get RS 2 keeping RS 1 open
        assertTrue(stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT));
        ResultSet rs2 = stmt.getResultSet();
        assertTrue(rs2.next());
        assertEquals("TABLE 2 ROW 1", rs2.getString(2));
        // Check RS 1 still open and on row 1
        assertEquals("TABLE 1 ROW 1", rs.getString(2));
        // Read a cached row from RS 1
        assertTrue(rs.next());
        assertEquals("TABLE 1 ROW 2", rs.getString(2));
        // Close RS 2 but keep RS 1 open and get RS 3
        assertTrue(stmt.getMoreResults(Statement.CLOSE_CURRENT_RESULT));
        ResultSet rs3 = stmt.getResultSet();
        assertTrue(rs3.next());
        assertEquals("TABLE 3 ROW 1", rs3.getString(2));
        // Check RS 2 is closed
        try {
            assertEquals("TABLE 2 ROW 1", rs2.getString(2));
            fail("Expected RS 2 to be closed!");
        } catch (SQLException e) {
            // Ignore
        }
        // Check RS 1 is still open
        assertEquals("TABLE 1 ROW 2", rs.getString(2));
        // Close all result sets and get RS 4
        assertTrue(stmt.getMoreResults(Statement.CLOSE_ALL_RESULTS));
        ResultSet rs4 = stmt.getResultSet();
        assertTrue(rs4.next());
        assertEquals("TABLE 4 ROW 1", rs4.getString(2));
        // check RS 1 is now closed as well
        try {
            assertEquals("TABLE 1 ROW 2", rs.getString(2));
            fail("Expected RS 1 to be closed!");
        } catch (SQLException e) {
            // Ignore
        }
        assertFalse(stmt.getMoreResults());
        stmt.close();
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(ResultSetTest.class);
    }
}
