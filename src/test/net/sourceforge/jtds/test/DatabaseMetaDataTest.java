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
}