package net.sourceforge.jtds.test;

import java.sql.*;
import java.math.*;

/**
 * Test case to illustrate use of TDS 8 support
 *
 * @version 1.0
 */
public class Tds8Test extends DatabaseTestCase {
    public Tds8Test(String name) {
        super(name);
    }

    public void testBigInt() throws Exception {
        if (!props.getProperty("TDS", "7.0").equals("8.0")) {
            System.out.println("testBigInt() requires TDS 8");
            return;
        }

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #BIGTEST (num bigint, txt varchar(100))");
        PreparedStatement pstmt = con.prepareStatement(
                                                      "INSERT INTO #BIGTEST (num, txt) VALUES (?, ?)");
        pstmt.setLong(1, 1234567890123L);
        pstmt.setString(2, "1234567890123");
        assertEquals("Insert bigint failed", 1, pstmt.executeUpdate());
        ResultSet rs = stmt.executeQuery("SELECT * FROM #BIGTEST");
        assertNotNull(rs);
        assertTrue(rs.next());
        assertEquals(String.valueOf(rs.getLong(1)), rs.getString(2));
        stmt.close();
        pstmt.close();
    }

    public void testSqlVariant() throws Exception {
        if (!props.getProperty("TDS", "7.0").equals("8.0")) {
            System.out.println("testSqlVariant() requires TDS 8");
            return;
        }

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #VARTEST (id int, data sql_variant)");
        PreparedStatement pstmt = con.prepareStatement(
                                                      "INSERT INTO #VARTEST (id, data) VALUES (?, ?)");
        pstmt.setInt(1, 1);
        pstmt.setString(2, "TEST STRING");
        assertEquals("Insert 1 failed", pstmt.executeUpdate(), 1);
        pstmt.setInt(1, 2);
        pstmt.setInt(2, 255);
        assertEquals("Insert 2 failed", pstmt.executeUpdate(), 1);
        pstmt.setInt(1, 3);
        pstmt.setBigDecimal(2, new BigDecimal("10.23"));
        assertEquals("Insert 3 failed", pstmt.executeUpdate(), 1);
        pstmt.setInt(1, 4);
        byte bytes[] = {'X','X','X'};
        pstmt.setBytes(2, bytes);
        assertEquals("Insert 4 failed", pstmt.executeUpdate(), 1);
        ResultSet rs = stmt.executeQuery("SELECT id, data FROM #VARTEST ORDER BY id");
        assertNotNull(rs);
        assertTrue(rs.next());
        assertEquals("TEST STRING", rs.getString(2));
        assertTrue(rs.next());
        assertEquals(255, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals("java.math.BigDecimal", rs.getObject(2).getClass().getName());
        assertEquals("10.23", rs.getString(2));
        assertTrue(rs.next());
        assertEquals("585858", rs.getString(2));
        stmt.close();
        pstmt.close();
    }

    public void testUserFn() throws Exception {
        if (!props.getProperty("TDS", "7.0").equals("8.0")) {
            System.out.println("testUserFn() requires TDS 8");
            return;
        }

        dropFunction("fn_varret");
        Statement stmt = con.createStatement();
        stmt.execute("CREATE FUNCTION fn_varret(@data varchar(100)) RETURNS sql_variant AS\r\n"+
                     "BEGIN\r\n"+
                     "RETURN 'Test ' + @data\r\n" +
                     "END");
        CallableStatement cstmt = con.prepareCall("{?=call jboss.fn_varret(?)}");
        cstmt.registerOutParameter(1, java.sql.Types.OTHER);
        cstmt.setString(2, "String");
        cstmt.execute();
        assertEquals("Test String", cstmt.getString(1));
        dropFunction("fn_varret");
    }

    public void testMetaData() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("create table #testrsmd (id int, data varchar(10), num decimal(10,2))");
        PreparedStatement pstmt = con.prepareStatement(
                                                      "select * from #testrsmd where id = ?");
        ResultSetMetaData rsmd = pstmt.getMetaData();
        assertNotNull(rsmd);
        assertEquals(3, rsmd.getColumnCount());
        assertEquals("data", rsmd.getColumnName(2));
        assertEquals(2, rsmd.getScale(3));
    }
    
    public static void main(String[] args) {
        junit.textui.TestRunner.run(Tds8Test.class);
    }
}
