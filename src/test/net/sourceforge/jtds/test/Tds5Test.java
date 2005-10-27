package net.sourceforge.jtds.test;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Types;
import java.io.ByteArrayInputStream;
import java.io.CharArrayReader;
import java.math.BigDecimal;

import junit.framework.Test;
import junit.framework.TestSuite;

import net.sourceforge.jtds.jdbc.DefaultProperties;
import net.sourceforge.jtds.jdbc.Messages;
import net.sourceforge.jtds.jdbc.Driver;

/**
 * Test case to illustrate use of TDS 5 support.
 *
 * @version 1.0
 * @author Mike Hutchinson
 */
public class Tds5Test extends TestBase {

    public static Test suite() {

        if (!DefaultProperties.TDS_VERSION_50.equals(
                props.getProperty(Messages.get(Driver.TDS)))) {

            return new TestSuite();
        }

        return new TestSuite(Tds5Test.class);
    }

    public Tds5Test(String name) {
        super(name);
    }

    /**
     * Test the new column meta data made available in Sybase 12
     * @throws Exception
     */
    public void testColMetaData() throws Exception {
        if (!isVersion12orHigher()) {
            return;
        }
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #TESTMD (id numeric(10,0) identity primary key not null, data unichar null)");
        ResultSetMetaData rsmd = stmt.executeQuery("SELECT id, data as aliasname FROM #TESTMD").getMetaData();
        assertEquals(2, rsmd.getColumnCount());
        assertEquals("tempdb", rsmd.getCatalogName(1));
        assertEquals("guest", rsmd.getSchemaName(1));
        assertEquals("#TESTMD", rsmd.getTableName(1));
        assertEquals("id", rsmd.getColumnName(1));
        assertEquals("aliasname", rsmd.getColumnName(2));
        assertEquals(ResultSetMetaData.columnNoNulls, rsmd.isNullable(1));
        assertEquals(ResultSetMetaData.columnNullable, rsmd.isNullable(2));
        assertEquals("numeric identity", rsmd.getColumnTypeName(1));
        assertEquals("unichar", rsmd.getColumnTypeName(2));
        assertTrue(rsmd.isAutoIncrement(1));
        assertFalse(rsmd.isAutoIncrement(2));
        stmt.close();
    }

    /**
     * Test the new date and time data types in Sybase 12+
     * @throws Exception
     */
    public void testDateTime() throws Exception {
        if (!isVersion12orHigher()) {
            return;
        }
        String testDate = "1997-08-31";
        String testTime = "23:59:59";
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #TESTDT (id int, d1 date, d2 date, t1 time, t2 time)");
        stmt.close();
        PreparedStatement pstmt = con.prepareStatement(
                "INSERT INTO #TESTDT VALUES(1, {d " + testDate + "}, ?, {t " + testTime + "}, ?)" );
        pstmt.setDate(1, Date.valueOf(testDate));
        pstmt.setTime(2, Time.valueOf(testTime));
        assertEquals(1, pstmt.executeUpdate());
        pstmt.close();
        stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM #TESTDT");
        assertTrue(rs.next());
        assertEquals(testDate, rs.getDate(2).toString());
        assertEquals(testDate, rs.getDate(3).toString());
        assertEquals(testTime, rs.getTime(4).toString());
        assertEquals(testTime, rs.getTime(5).toString());
        stmt.close();
    }

    /**
     * Test varchar and varbinary fields longer than 255 bytes.
     * Test univarchar columns as well.
     * @throws Exception
     */
    public void testLongData() throws Exception {
        if (!isVersion12orHigher()) {
            return;
        }
        StringBuffer buf = new StringBuffer(300);
        for (int i = 0; i < 300; i++) {
            if (i == 0) {
                buf.append('<');
            } else
            if (i == 299) {
                buf.append('>');
            } else {
                buf.append('X');
            }
        }
        String longString = buf.toString();
        byte longBytes[] = longString.getBytes();
        String unichar = "This is a unicode string \u0441\u043b\u043e\u0432\u043e";
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #TESTLD (id int, vc varchar(300), vb varbinary(300), vu univarchar(300))");
        stmt.close();
        PreparedStatement pstmt = con.prepareStatement(
                "INSERT INTO #TESTLD VALUES(1, ?, ?, ?)" );
        pstmt.setString(1, longString);
        pstmt.setBytes(2, longBytes);
        pstmt.setString(3, unichar);
        assertEquals(1, pstmt.executeUpdate());
        pstmt.close();
        stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM #TESTLD");
        assertTrue(rs.next());
        assertEquals(longString, rs.getString(2));
        assertEquals(longString, new String(rs.getBytes(3)));
        assertEquals(unichar, rs.getString(4));
        stmt.close();
    }

    /**
     * Test for bug [1161609]  Text or image data truncated on Sybase 12.5
     * @throws Exception
     */
    public void testImageText() throws Exception {
        if (!isVersion12orHigher()) {
            return;
        }
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #TESTIT (id int, txt text, img image)");
        StringBuffer data = new StringBuffer(20000);
        for (int i = 0; i < 20000; i++) {
            data.append((char)('A' + (i % 10)));
        }
        PreparedStatement pstmt = con.prepareStatement(
           "INSERT INTO #TESTIT VALUES(?,?,?)");
        pstmt.setInt(1, 1);
        pstmt.setString(2, data.toString());
        pstmt.setBytes(3, data.toString().getBytes());
        assertEquals(1, pstmt.executeUpdate());
        ResultSet rs = stmt.executeQuery("SELECT * FROM #TESTIT");
        assertNotNull(rs);
        assertTrue(rs.next());
        assertEquals(data.length(), rs.getString(2).length());
        assertEquals(data.length(), rs.getBytes(3).length);
        pstmt.close();
        stmt.close();
    }

    /**
     * Test writing image data from InputStream
     * @throws Exception
     */
    public void testStreamImage() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #TESTIS (id int, img image)");
        byte data[] = new byte[20000];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte)('A' + (i % 10));
        }
        PreparedStatement pstmt = con.prepareStatement(
            "INSERT INTO #TESTIS VALUES(?,?)");
        pstmt.setInt(1, 1);
        pstmt.setBinaryStream(2, new ByteArrayInputStream(data), data.length);
        assertEquals(1, pstmt.executeUpdate());
        ResultSet rs = stmt.executeQuery("SELECT * FROM #TESTIS");
        assertNotNull(rs);
        assertTrue(rs.next());
        assertEquals(new String(data), new String(rs.getBytes(2)));
        pstmt.close();
        stmt.close();
    }

    /**
     * Test writing text data from Reader
     * @throws Exception
     */
    public void testStreamText() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #TESTTR (id int, txt text)");
        char data[] = new char[20000];
        for (int i = 0; i < data.length; i++) {
            data[i] = (char)('A' + (i % 10));
        }
        PreparedStatement pstmt = con.prepareStatement(
            "INSERT INTO #TESTTR VALUES(?,?)");
        pstmt.setInt(1, 1);
        pstmt.setCharacterStream(2, new CharArrayReader(data), data.length);
        assertEquals(1, pstmt.executeUpdate());
        ResultSet rs = stmt.executeQuery("SELECT * FROM #TESTTR");
        assertNotNull(rs);
        assertTrue(rs.next());
        assertEquals(new String(data), rs.getString(2));
        pstmt.close();
        stmt.close();
    }

    /**
     * Test writing unitext data from Reader
     */
    public void testStreamUniText() throws Exception {
        if (!isVersion15orHigher()) {
            return;
        }
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #TESTTR (id int, txt unitext)");
        char data[] = new char[20000];
        for (int i = 0; i < data.length; i++) {
            data[i] = (char)('A' + (i % 10));
        }
        data[data.length-1] = '\u0441'; // Force unicode
        PreparedStatement pstmt = con.prepareStatement(
            "INSERT INTO #TESTTR VALUES(?,?)");
        pstmt.setInt(1, 1);
        pstmt.setCharacterStream(2, new CharArrayReader(data), data.length);
        assertEquals(1, pstmt.executeUpdate());
        ResultSet rs = stmt.executeQuery("SELECT * FROM #TESTTR");
        assertNotNull(rs);
        assertTrue(rs.next());
        assertEquals(new String(data), rs.getString(2));
        pstmt.close();
        stmt.close();
    }

    /**
     * Test writing unitext data from memory
     * @throws Exception
     */
    public void testUniText() throws Exception {
        if (!isVersion15orHigher()) {
            return;
        }
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #TESTTR (id int, txt unitext)");
        char data[] = new char[20000];
        for (int i = 0; i < data.length; i++) {
            data[i] = (char)('A' + (i % 10));
        }
        data[data.length-1] = '\u0441'; // Force unicode
        PreparedStatement pstmt = con.prepareStatement(
            "INSERT INTO #TESTTR VALUES(?,?)");
        pstmt.setInt(1, 1);
        pstmt.setString(2, new String(data));
        assertEquals(1, pstmt.executeUpdate());
        ResultSet rs = stmt.executeQuery("SELECT * FROM #TESTTR");
        assertNotNull(rs);
        assertTrue(rs.next());
        assertEquals(new String(data), rs.getString(2));
        ResultSetMetaData rsmd = rs.getMetaData();
        assertEquals("unitext", rsmd.getColumnTypeName(2));
        assertEquals(Types.CLOB, rsmd.getColumnType(2));
        pstmt.close();
        stmt.close();
    }

    /**
     * Test Sybase ASE 15+ bigint data type.
     * @throws Exception
     */
    public void testBigint() throws Exception {
        if (!isVersion15orHigher()) {
            return;
        }
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #TEST (val bigint primary key, val2 bigint null)");
        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #TEST VALUES(?,?)");
        pstmt.setLong(1, Long.MAX_VALUE);
        pstmt.setLong(2, Long.MIN_VALUE);
        pstmt.executeUpdate();
        ResultSet rs = stmt.executeQuery("SELECT * FROM #TEST");
        rs.next();
        assertEquals(Long.MAX_VALUE, rs.getLong(1));
        assertEquals(Long.MIN_VALUE, rs.getLong(2));
        ResultSetMetaData rsmd = rs.getMetaData();
        assertEquals("bigint", rsmd.getColumnTypeName(1));
        assertEquals("bigint", rsmd.getColumnTypeName(2));
        assertEquals(Types.BIGINT, rsmd.getColumnType(1));
        assertEquals(Types.BIGINT, rsmd.getColumnType(2));
    }

    /**
     * Test Sybase ASE 15+ unsigned smallint data type.
     * @throws Exception
     */
    public void testUnsignedSmallInt() throws Exception {
        if (!isVersion15orHigher()) {
            return;
        }
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #TEST (val unsigned smallint primary key, val2 unsigned smallint null)");
        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #TEST VALUES(?,?)");
        pstmt.setInt(1, 65535);
        pstmt.setInt(2, 65535);
        pstmt.executeUpdate();
        ResultSet rs = stmt.executeQuery("SELECT * FROM #TEST");
        rs.next();
        assertEquals(65535, rs.getInt(1));
        assertEquals(65535, rs.getInt(2));
        ResultSetMetaData rsmd = rs.getMetaData();
        assertEquals("unsigned smallint", rsmd.getColumnTypeName(1));
        assertEquals("unsigned smallint", rsmd.getColumnTypeName(2));
        assertEquals(Types.INTEGER, rsmd.getColumnType(1));
        assertEquals(Types.INTEGER, rsmd.getColumnType(2));
    }

    /**
     * Test Sybase ASE 15+ unsigned int data type.
     * @throws Exception
     */
    public void testUnsignedInt() throws Exception {
        if (!isVersion15orHigher()) {
            return;
        }
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #TEST (val unsigned int primary key, val2 unsigned int null)");
        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #TEST VALUES(?,?)");
        pstmt.setLong(1, 4294967295L);
        pstmt.setLong(2, 4294967295L);
        pstmt.executeUpdate();
        ResultSet rs = stmt.executeQuery("SELECT * FROM #TEST");
        rs.next();
        assertEquals(4294967295L, rs.getLong(1));
        assertEquals(4294967295L, rs.getLong(2));
        ResultSetMetaData rsmd = rs.getMetaData();
        assertEquals("unsigned int", rsmd.getColumnTypeName(1));
        assertEquals("unsigned int", rsmd.getColumnTypeName(2));
        assertEquals(Types.BIGINT, rsmd.getColumnType(1));
        assertEquals(Types.BIGINT, rsmd.getColumnType(2));
    }

    /**
     * Test Sybase ASE 15+ unsigned bigint data type.
     * @throws Exception
     */
    public void testUnsignedBigInt() throws Exception {
        if (!isVersion15orHigher()) {
            return;
        }
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #TEST (val unsigned bigint primary key, val2 unsigned bigint null)");
        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #TEST VALUES(?,?)");
        pstmt.setBigDecimal(1, new BigDecimal("18446744073709551615"));
        pstmt.setBigDecimal(2, new BigDecimal("18446744073709551615"));
        pstmt.executeUpdate();
        ResultSet rs = stmt.executeQuery("SELECT * FROM #TEST");
        rs.next();
        assertEquals("18446744073709551615", rs.getString(1));
        assertEquals("18446744073709551615", rs.getString(2));
        ResultSetMetaData rsmd = rs.getMetaData();
        assertEquals("unsigned bigint", rsmd.getColumnTypeName(1));
        assertEquals("unsigned bigint", rsmd.getColumnTypeName(2));
        assertEquals(Types.DECIMAL, rsmd.getColumnType(1));
        assertEquals(Types.DECIMAL, rsmd.getColumnType(2));
    }

    private boolean isVersion12orHigher() throws Exception
    {
        return Integer.parseInt(con.getMetaData().
                getDatabaseProductVersion().substring(0,2)) >= 12;
    }

    private boolean isVersion15orHigher() throws Exception
    {
        return Integer.parseInt(con.getMetaData().
                getDatabaseProductVersion().substring(0,2)) >= 15;
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(Tds5Test.class);
    }
}
