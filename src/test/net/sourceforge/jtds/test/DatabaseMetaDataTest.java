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
public class DatabaseMetaDataTest extends TestBase {
    public DatabaseMetaDataTest(String name) {
        super(name);
    }

    /**
     * Test for bug [974036] Bug in 0.8rc1 DatabaseMetaData method getTableTypes()
     */
    public void testGetTableTypesOrder() throws Exception {
    	DatabaseMetaData dmd = con.getMetaData();
    	ResultSet rs = dmd.getTableTypes();
    	String previousType = "";

    	while (rs.next()) {
    		String type = rs.getString(1);

    		assertTrue(type.compareTo(previousType) >= 0);
			previousType = type;
    	}

    	rs.close();
    }

    /**
     * Test for bug [998765] Exception with Sybase and metaData.getTables()
     */
    public void testGetTables() throws Exception {
        DatabaseMetaData dmd = con.getMetaData();
        ResultSet rs = dmd.getTables(null, null, null, null);

        assertNotNull(rs);

        rs.close();
    }

    /**
     * Test for bug [1023984] Protocol error processing table meta data.
     * <p>
     * Test to demonstrate failure to process the TDS table name token
     * correctly. Must be run with TDS=8.0.
     * @throws Exception
     */
    public void testTableMetaData() throws Exception {
        Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = stmt.executeQuery("SELECT * FROM master.dbo.sysdatabases");

        assertNotNull(rs);
        ResultSetMetaData rsmd = rs.getMetaData();

        assertEquals("master", rsmd.getCatalogName(1));
        assertEquals("dbo", rsmd.getSchemaName(1));
        assertEquals("sysdatabases", rsmd.getTableName(1));

        stmt.close();
        rs.close();
    }

    public void testColumnClassName() throws SQLException {
        byte[] bytes = new byte[] {1, 2, 3};

        // Create a table w/ pretty much all the possible types
        String tabdef = "CREATE TABLE #testColumnClassName("
                + "colByte TINYINT,"
                + "colShort SMALLINT,"
                + "colInt INTEGER,"
                + "colBigint DECIMAL(29,0),"
                + "colFloat REAL,"
                + "colDouble FLOAT,"
                + "colDecimal DECIMAL(29,10),"
                + "colBit BIT,"
                + "colByteArray VARBINARY(255),"
                + "colTimestamp DATETIME,"
                + "colBlob IMAGE,"
                + "colClob TEXT,"
                + "colString VARCHAR(255),"
                + "colGuid UNIQUEIDENTIFIER"
                + ")";
        Statement stmt = con.createStatement();
        stmt.executeUpdate(tabdef);

        // Insert a row into the table
        PreparedStatement pstmt = con.prepareStatement(
                "INSERT INTO #testColumnClassName ("
                + "colByte,colShort,colInt,colBigint,colFloat,colDouble,"
                + "colDecimal,colBit,colByteArray,colTimestamp,colBlob,colClob,"
                + "colString,colGuid) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
        pstmt.setByte(1, (byte) 1);
        pstmt.setShort(2, (short) 2222);
        pstmt.setInt(3, 123456);
        pstmt.setInt(4, 123456);
        pstmt.setFloat(5, 0.111f);
        pstmt.setDouble(6, 0.111);
        pstmt.setDouble(7, 0.111111);
        pstmt.setBoolean(8, true);
        pstmt.setBytes(9, bytes);
        pstmt.setTimestamp(10, new Timestamp(System.currentTimeMillis()));
        pstmt.setBytes(11, bytes);
        pstmt.setString(12, "Test");
        pstmt.setString(13, "Test");
        pstmt.setString(14, "ebd558a0-0c68-11d9-9669-0800200c9a66");
        assertEquals("No row inserted", 1, pstmt.executeUpdate());

        // Select the row and check that getColumnClassName matches the actual
        // class
        ResultSet rs = stmt.executeQuery("SELECT * FROM #testColumnClassName");
        assertTrue("No rows in ResultSet", rs.next());
        ResultSetMetaData meta = rs.getMetaData();
        for (int i=1; i<=meta.getColumnCount(); i++) {
            Object obj = rs.getObject(i);
            assertNotNull("Expecting non-null value", obj);
            String metaClass = meta.getColumnClassName(i);
            Class c = null;
            try {
                c = Class.forName(metaClass);
            } catch (ClassNotFoundException ex) {
                fail("Class returned by getColumnClassName() not found: " + metaClass);
                return;
            }
            if (!c.isAssignableFrom(obj.getClass())) {
                fail("getColumnClassName() returned " + metaClass + " but the actual class is "
                        + obj.getClass().getName());
            }
        }
        stmt.close();
    }
}
