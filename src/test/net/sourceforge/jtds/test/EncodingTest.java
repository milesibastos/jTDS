package net.sourceforge.jtds.test;

import java.math.*;
import java.sql.*;

/**
 * @version 1.0
 */
public class EncodingTest extends TestBase {

    public EncodingTest(String name) {
        super(name);
    }

    /**
     * Test for bug [981958] PreparedStatement doesn't work correctly
     */
    public void testEncoding1251Test1() throws Exception {
        String value = "\u0441\u043b\u043e\u0432\u043e"; // String in Cp1251 encoding
        Statement stmt = con.createStatement();
        
        stmt.execute("CREATE TABLE #e1251t1 (data varchar(255) COLLATE Cyrillic_General_BIN)");
        assertEquals(stmt.executeUpdate("INSERT INTO #e1251t1 (data) VALUES (N'" + value + "')"), 1);
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("SELECT data FROM #e1251t1 WHERE data = ?");        
        pstmt.setString(1, value);
        ResultSet rs = pstmt.executeQuery();

        assertTrue(rs.next());
        //assertEquals(value, rs.getString(1));
        assertTrue(!rs.next());
        pstmt.close();
        rs.close();
    }
    
    public static void main(String[] args) {
        junit.textui.TestRunner.run(EncodingTest.class);
    }
}