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

        //assertTrue(tmpData instanceof Long);
        //assertEquals(data, ((Long) tmpData).longValue());

        ResultSetMetaData resultSetMetaData = rs.getMetaData();

        assertNotNull(resultSetMetaData);
        //assertEquals(Types.BIGINT, resultSetMetaData.getColumnType(1));

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
     * Test for bug [945462] getResultSet() return null if you use scrollable/updatable
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
     * Test for bug [1008208] 0.9-rc1 updateNull doesn't work
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
    
    public static void main(String[] args) {
        junit.textui.TestRunner.run(ResultSetTest.class);
    }
}