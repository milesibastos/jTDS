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

import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Test case to illustrate use of Cached cursor result set.
 *
 * @version 1.0
 * @author Mike Hutchinson
 */
public class ClientSideCursorTest extends DatabaseTestCase {
    public ClientSideCursorTest(String name) {
        super(name);
    }

    public void testCachedCursor() throws Exception {
        try {
            dropTable("jTDS_CachedCursorTest");
            Statement stmt = con.createStatement();
            stmt.execute("CREATE TABLE jTDS_CachedCursorTest " +
                    "(key1 int NOT NULL, key2 char(4) NOT NULL," +
                    "data varchar(255))\r\n" +
                    "ALTER TABLE jTDS_CachedCursorTest " +
                    "ADD CONSTRAINT PK_jTDS_CachedCursorTest PRIMARY KEY CLUSTERED" +
                    "( key1, key2)");
            for (int i = 1; i <= 16; i++) {
                assertEquals(1, stmt.executeUpdate("INSERT INTO jTDS_CachedCursorTest VALUES(" + i + ", 'XXXX','LINE " + i + "')"));
            }
            stmt.close();
            stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
            ResultSet rs = stmt.executeQuery("SELECT * FROM jTDS_CachedCursorTest ORDER BY key1");
            assertNotNull(rs);
            assertEquals(null, stmt.getWarnings());
            assertTrue(rs.isBeforeFirst());
            assertTrue(rs.first());
            assertEquals(1, rs.getInt(1));
            assertTrue(rs.isFirst());
            assertTrue(rs.last());
            assertEquals(16, rs.getInt(1));
            assertTrue(rs.isLast());
            assertFalse(rs.next());
            assertTrue(rs.isAfterLast());
            rs.beforeFirst();
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            rs.afterLast();
            assertTrue(rs.previous());
            assertEquals(16, rs.getInt(1));
            assertTrue(rs.absolute(8));
            assertEquals(8, rs.getInt(1));
            assertTrue(rs.relative(-1));
            assertEquals(7, rs.getInt(1));
            rs.updateString(3, "New line 7");
            rs.updateRow();
    //        assertTrue(rs.rowUpdated());
            rs.moveToInsertRow();
            rs.updateInt(1, 17);
            rs.updateString(2, "XXXX");
            rs.updateString(3, "LINE 17");
            rs.insertRow();
            rs.moveToCurrentRow();
            rs.last();
    //        assertTrue(rs.rowInserted());
            Statement stmt2 = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            ResultSet rs2 = stmt2.executeQuery("SELECT * FROM jTDS_CachedCursorTest ORDER BY key1");
            rs.updateString(3, "NEW LINE 17");
            rs.updateRow();
            assertTrue(rs2.last());
            assertEquals(17, rs2.getInt(1));
            assertEquals("NEW LINE 17", rs2.getString(3));
            rs.deleteRow();
            rs2.refreshRow();
            assertTrue(rs2.rowDeleted());
            rs2.close();
            stmt2.close();
            rs.close();
            stmt.close();
        } finally {
            dropTable("jTDS_CachedCursorTest");
        }
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(ClientSideCursorTest.class);
    }
}
