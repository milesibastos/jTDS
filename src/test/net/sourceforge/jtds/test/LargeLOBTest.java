package net.sourceforge.jtds.test;

import java.io.*;
import java.sql.*;

/**
 * @version 1.0
 */
public class LargeLOBTest extends TestBase {
    private static final int LOB_LENGTH = 100000000;

    public LargeLOBTest(String name) {
        super(name);
    }

    /*************************************************************************
     *************************************************************************
     **                          BLOB TESTS                                 **
     *************************************************************************
     *************************************************************************/

    /**
     * Test for bug [945507] closing statement after selecting a large IMAGE - Exception
     */
    public void testLargeBlob1() throws Exception {
    	File data = File.createTempFile("blob", ".tmp");
    	FileOutputStream fos = new FileOutputStream(data);
    	BufferedOutputStream bos = new BufferedOutputStream(fos);
    	
    	data.deleteOnExit();

    	for (long i = 0; i < LOB_LENGTH; i++) {
    		bos.write((byte) i % 255);
    	}
    	
    	bos.close();
    	
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #largeblob1 (data IMAGE)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #largeblob1 (data) VALUES (?)");
    	FileInputStream fis = new FileInputStream(data);
    	BufferedInputStream bis = new BufferedInputStream(fis);
        
        // Test PreparedStatement.setBinaryStream()
        pstmt.setBinaryStream(1, bis, LOB_LENGTH);
        assertTrue(pstmt.executeUpdate() == 1);

        pstmt.close();
        bis.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data FROM #largeblob1");

        assertTrue(rs.next());

    	fis = new FileInputStream(data);
    	bis = new BufferedInputStream(fis);
        
        // Test ResultSet.getBinaryStream()
        compareInputStreams(bis, rs.getBinaryStream(1));
        bis.close();

        assertTrue(!rs.next());
        stmt2.close();
        rs.close();
        
        assertTrue(data.delete());
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(LOBTest.class);
    }
}