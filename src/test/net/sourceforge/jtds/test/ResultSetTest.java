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
        assertEquals(pstmt.executeUpdate(), 1);

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
        assertEquals(rs.getString(1), "1");

        Object tmpData = rs.getObject(1);

        assertTrue(tmpData instanceof Boolean);
        assertEquals(((Boolean) tmpData).booleanValue(), true);

        ResultSetMetaData resultSetMetaData = rs.getMetaData();

        assertNotNull(resultSetMetaData);
        assertEquals(resultSetMetaData.getColumnType(1), Types.BIT);

        assertTrue(!rs.getBoolean(2));
        assertTrue(rs.getBoolean(3));
        
        assertTrue(!rs.next());
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
        assertEquals(pstmt.executeUpdate(), 1);

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
        assertEquals(rs.getString(1), "1");

        Object tmpData = rs.getObject(1);

        assertTrue(tmpData instanceof Integer);
        assertTrue(data == ((Integer) tmpData).byteValue());

        ResultSetMetaData resultSetMetaData = rs.getMetaData();

        assertNotNull(resultSetMetaData);
        assertEquals(resultSetMetaData.getColumnType(1), Types.TINYINT);

        assertEquals(rs.getByte(2), Byte.MIN_VALUE);
        assertEquals(rs.getByte(3), Byte.MAX_VALUE);

        assertTrue(!rs.next());
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
        assertEquals(pstmt.executeUpdate(), 1);

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
        assertEquals(rs.getString(1), "1");

        Object tmpData = rs.getObject(1);

        assertTrue(tmpData instanceof Integer);
        assertTrue(data == ((Integer) tmpData).shortValue());

        ResultSetMetaData resultSetMetaData = rs.getMetaData();

        assertNotNull(resultSetMetaData);
        assertEquals(resultSetMetaData.getColumnType(1), Types.SMALLINT);

        assertEquals(rs.getShort(2), Short.MIN_VALUE);
        assertEquals(rs.getShort(3), Short.MAX_VALUE);
        
        assertTrue(!rs.next());
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
        assertEquals(pstmt.executeUpdate(), 1);

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
        assertEquals(rs.getString(1), "1");

        Object tmpData = rs.getObject(1);

        assertTrue(tmpData instanceof Integer);
        assertTrue(data == ((Integer) tmpData).intValue());

        ResultSetMetaData resultSetMetaData = rs.getMetaData();

        assertNotNull(resultSetMetaData);
        assertEquals(resultSetMetaData.getColumnType(1), Types.INTEGER);

        assertEquals(rs.getInt(2), Integer.MIN_VALUE);
        assertEquals(rs.getInt(3), Integer.MAX_VALUE);
        
        assertTrue(!rs.next());
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
        assertEquals(pstmt.executeUpdate(), 1);

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
        assertEquals(rs.getString(1), "1");

        Object tmpData = rs.getObject(1);

        //assertTrue(tmpData instanceof Long);
        //assertTrue(data == ((Long) tmpData).longValue());

        ResultSetMetaData resultSetMetaData = rs.getMetaData();

        assertNotNull(resultSetMetaData);
        //assertEquals(resultSetMetaData.getColumnType(1), Types.BIGINT);

        assertEquals(rs.getLong(2), Long.MIN_VALUE);
        assertEquals(rs.getLong(3), Long.MAX_VALUE);
        
        assertTrue(!rs.next());
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
            assertEquals(pstmt.executeUpdate(), 1);
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
        assertEquals(pstmt.executeUpdate(), 1);

        pstmt.close();

        Statement stmt2 = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_UPDATABLE);
        stmt2.executeQuery("SELECT data FROM #resultSetScroll2");

        ResultSet rs = stmt2.getResultSet();

        assertNotNull(rs);
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 1);
        assertTrue(!rs.next());

        stmt2.close();
        rs.close();
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(ResultSetTest.class);
    }
}