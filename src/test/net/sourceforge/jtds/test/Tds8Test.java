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

import junit.framework.Test;
import junit.framework.TestSuite;
import net.sourceforge.jtds.jdbc.DefaultProperties;
import net.sourceforge.jtds.jdbc.Messages;
import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.sql.DriverManager;
import java.io.PrintWriter;



/**
 * Test case to illustrate use of TDS 8 support
 *
 * @version 1.0
 */
public class Tds8Test extends DatabaseTestCase {

    public static Test suite() {

        if (!props.getProperty(Messages.get("prop.tds"), DefaultProperties.TDS_VERSION_80).equals(
                DefaultProperties.TDS_VERSION_80)) {

            return new TestSuite();
        }

        return new TestSuite(Tds8Test.class);
    }

    public Tds8Test(String name) {
        super(name);
    }

    public void testBigInt1() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #bigint1 (num bigint, txt varchar(100))");
        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #bigint1 (num, txt) VALUES (?, ?)");
        pstmt.setLong(1, 1234567890123L);
        pstmt.setString(2, "1234567890123");
        assertEquals("Insert bigint failed", 1, pstmt.executeUpdate());
        ResultSet rs = stmt.executeQuery("SELECT * FROM #bigint1");
        assertNotNull(rs);
        assertTrue(rs.next());
        assertEquals(String.valueOf(rs.getLong(1)), rs.getString(2));
        stmt.close();
        pstmt.close();
    }

    /**
     * Test BIGINT data type.
     * Test for [989963] BigInt becomes Numeric
     */
    public void testBigInt2() throws Exception {
        long data = 1;

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #bigint2 (data BIGINT, minval BIGINT, maxval BIGINT)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #bigint2 (data, minval, maxval) VALUES (?, ?, ?)");

        pstmt.setLong(1, data);
        pstmt.setLong(2, Long.MIN_VALUE);
        pstmt.setLong(3, Long.MAX_VALUE);
        assertEquals(pstmt.executeUpdate(), 1);

        pstmt.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data, minval, maxval FROM #bigint2");

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

        assertTrue(tmpData instanceof Long);
        assertTrue(data == ((Long) tmpData).longValue());

        ResultSetMetaData resultSetMetaData = rs.getMetaData();

        assertNotNull(resultSetMetaData);
        assertEquals(resultSetMetaData.getColumnType(1), Types.BIGINT);

        assertEquals(rs.getLong(2), Long.MIN_VALUE);
        assertEquals(rs.getLong(3), Long.MAX_VALUE);

        assertTrue(!rs.next());
        stmt2.close();
        rs.close();
    }

    public void testSqlVariant() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #VARTEST (id int, data sql_variant)");
        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #VARTEST (id, data) VALUES (?, ?)");

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
        byte bytes[] = {'X', 'X', 'X'};
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
        assertEquals("XXX", rs.getString(2));
        stmt.close();
        pstmt.close();
    }

    public void testUserFn() throws Exception {
        dropFunction("f_varret");
        Statement stmt = con.createStatement();
        stmt.execute(
                "CREATE FUNCTION f_varret(@data varchar(100)) RETURNS sql_variant AS\r\n" +
                "BEGIN\r\n" +
                "RETURN 'Test ' + @data\r\n" +
                "END");
        CallableStatement cstmt = con.prepareCall("{?=call f_varret(?)}");
        cstmt.registerOutParameter(1, java.sql.Types.OTHER);
        cstmt.setString(2, "String");
        cstmt.execute();
        assertEquals("Test String", cstmt.getString(1));
        dropFunction("f_varret");
    }

    public void testMetaData() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("create table #testrsmd (id int, data varchar(10), num decimal(10,2))");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("select * from #testrsmd where id = ?");
        ResultSetMetaData rsmd = pstmt.getMetaData();
        assertNotNull(rsmd);
        assertEquals(3, rsmd.getColumnCount());
        assertEquals("data", rsmd.getColumnName(2));
        assertEquals(2, rsmd.getScale(3));
        pstmt.close();
    }

    /**
     * Test for bug [1042272] jTDS doesn't allow null value into Boolean.
     */
    public void testNullBoolean() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("create table #testNullBoolean (id int, value bit)");

        PreparedStatement pstmt = con.prepareStatement(
                "insert into #testNullBoolean (id, value) values (?, ?)");
        pstmt.setInt(1, 1);
        pstmt.setNull(2, Types.BOOLEAN);
        assertEquals(1, pstmt.executeUpdate());
        pstmt.close();

        ResultSet rs = stmt.executeQuery("select * from #testNullBoolean");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(null, rs.getObject(2));
        assertFalse(rs.next());
        rs.close();
        stmt.close();
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(Tds8Test.class);
    }
}
