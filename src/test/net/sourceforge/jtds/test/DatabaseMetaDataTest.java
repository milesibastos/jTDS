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
}