package net.sourceforge.jtds.test;

import java.io.*;
import java.sql.*;
import java.util.*;

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
        byte[] data = getBlobTestData();

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #largeblob1 (data IMAGE)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #largeblob1 (data) VALUES (?)");

        // Test PreparedStatement.setBytes()
        pstmt.setBytes(1, data);
        assertTrue(pstmt.executeUpdate() == 1);

        pstmt.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data FROM #largeblob1");

        assertTrue(rs.next());

        // Test ResultSet.getBytes()
        assertTrue(Arrays.equals(data, rs.getBytes(1)));

        assertTrue(!rs.next());
        stmt2.close();
        rs.close();
    }

    private byte[] getBlobTestData() {
        byte[] data = new byte[LOB_LENGTH];

        for (int i = 0; i < LOB_LENGTH; i++) {
           data[i] = (byte) (Math.random() * 255);
       }

        return data;
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(LOBTest.class);
    }
}