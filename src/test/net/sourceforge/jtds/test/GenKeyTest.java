package net.sourceforge.jtds.test;

import java.sql.*;

/**
 * Test case to illustrate JDBC 3 GetGeneratedKeys() function.
 *
 * @version    1.0
 */
public class GenKeyTest extends TestBase {

    public GenKeyTest(String name)
    {
        super(name);
    }

    public void testParams()
    throws Exception
    {
		//
		// Test data
		//
        Statement stmt = con.createStatement( );

        stmt.execute( "CREATE TABLE #gktemp ( id INT IDENTITY (1,1) PRIMARY KEY, dummyx VARCHAR(50))" );

		stmt.close();
		//
		// Test PrepareStatement(sql, int) option
		//
		PreparedStatement pstmt =
			con.prepareStatement("INSERT INTO #gktemp (dummyx) VALUES (?)", Statement.RETURN_GENERATED_KEYS);
		pstmt.setString(1, "TEST01");
		assertEquals("First Insert failed", 1, pstmt.executeUpdate());
		ResultSet rs = pstmt.getGeneratedKeys();
		assertTrue("ResultSet empty", rs.next());
		assertEquals("Bad inserted row ID ", 1, rs.getInt(1));
		rs.close();
		pstmt.close();
		//
		// Test PrepareStatement(sql, int[]) option
		//
		int cols[] = new int[1];
		cols[0] = 1;
		pstmt =
			con.prepareStatement("INSERT INTO #gktemp (dummyx) VALUES (?)", cols);
		pstmt.setString(1, "TEST02");
		assertEquals("Second Insert failed", 1, pstmt.executeUpdate());
		rs = pstmt.getGeneratedKeys();
		assertTrue("ResultSet 2 empty", rs.next());
		assertEquals("Bad inserted row ID ", 2, rs.getInt(1));
		rs.close();
		pstmt.close();
		//
		// Test PrepareStatement(sql, String[]) option
		//
		String colNames[] = new String[1];
		colNames[0] = "ID";
		pstmt =
			con.prepareStatement("INSERT INTO #gktemp (dummyx) VALUES (?)", colNames);
		pstmt.setString(1, "TEST03");
		pstmt.execute();
		assertEquals("Third Insert failed", 1, pstmt.getUpdateCount());
		rs = pstmt.getGeneratedKeys();
		assertTrue("ResultSet 3 empty", rs.next());
		assertEquals("Bad inserted row ID ", 3, rs.getInt(1));
		rs.close();
		pstmt.close();
		//
		// Test CreateStatement()
		//
		stmt = con.createStatement();
		assertEquals("Fourth Insert failed", 1,
			stmt.executeUpdate("INSERT INTO #gktemp (dummyx) VALUES ('TEST04')",
				Statement.RETURN_GENERATED_KEYS));
		rs = stmt.getGeneratedKeys();
		assertTrue("ResultSet 4 empty", rs.next());
		assertEquals("Bad inserted row ID ", 4, rs.getInt(1));
		rs.close();
		stmt.close();

        stmt = con.createStatement( );

        stmt.execute( "DROP TABLE #gktemp" );

        stmt.close();
    }

    public static void main(String[] args)
    {
        junit.textui.TestRunner.run(UpdateTest.class);
    }
}
