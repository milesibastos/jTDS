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

    /**
     * Test for bug [101956] updateBytes converted to hex in varchar
     * NB Test assumes server using iso_1 character set.
     */
    public void testBytesToVarchar() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #test (id INT PRIMARY KEY, data VARCHAR(255) NULL)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #test (id, data) VALUES (?, ?)");
        pstmt.setInt(1, 1);
        pstmt.setBytes(2, "This is a test".getBytes("ISO-8859-1"));

        assertEquals(pstmt.executeUpdate(), 1);
        pstmt.setInt(1, 2);
        pstmt.setBytes(2, null);

        assertEquals(pstmt.executeUpdate(), 1);
        pstmt.close();

        stmt = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT * FROM #test");

        while (rs.next()) {
            if (rs.getInt(1) == 2) {
                rs.updateBytes(2, "This is a another test".getBytes("ISO-8859-1"));
                rs.updateRow();
            }
        }

        rs.close();
        stmt.close();

        stmt = con.createStatement();
        rs = stmt.executeQuery("SELECT id, data FROM #test ORDER BY id");

        assertTrue(rs.next());
        assertEquals("This is a test", rs.getString("data"));
        assertTrue(rs.next());
        assertEquals("This is a another test", rs.getString("data"));

        rs.close();
        stmt.close();

    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(EncodingTest.class);
    }
}