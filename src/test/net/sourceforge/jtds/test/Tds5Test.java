package net.sourceforge.jtds.test;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Time;

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
public class Tds5Test extends DatabaseTestCase {

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

    public static void main(String[] args) {
        junit.textui.TestRunner.run(Tds5Test.class);
    }
}
