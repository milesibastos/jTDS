package net.sourceforge.jtds.test;

import junit.framework.TestCase;
import java.io.*;
import java.sql.*;
import java.util.*;

/**
 * @version 1.0
 */

public class LOBTest extends TestBase {
    public LOBTest(String name) {
        super(name);
    }

    public void testBlobGet() throws Exception {
        byte[] data = getBlobTestData();

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #blobget (data BINARY(8000))");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #blobget (data) VALUES (?)");

        // Test PreparedStatement.setBytes()
        pstmt.setBytes(1, data);
        assertTrue(pstmt.executeUpdate() == 1);

        pstmt.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data FROM #blobget");

        assertTrue(rs.next());

        // Test ResultSet.getBytes()
        assertTrue(Arrays.equals(data, rs.getBytes(1)));

        // Test ResultSet.getBinaryStream()
        InputStream is = rs.getBinaryStream(1);
        byte[] isTmpData = new byte[data.length];

        assertTrue(is.read(isTmpData) == data.length);
        assertTrue(is.read() == -1);
        assertTrue(Arrays.equals(data, isTmpData));

        // Test ResultSet.getBlob()
        Blob blob = rs.getBlob(1);

        assertTrue(blob != null);

        // Test Blob.length()
        assertTrue(blob.length() == data.length);

        // Test Blob.getBytes()
        byte[] tmpData2 = blob.getBytes((long) 1, (int) blob.length());

        assertTrue(Arrays.equals(data, tmpData2));

        // Test Blob.getBinaryStream()
        InputStream is2 = blob.getBinaryStream();
        byte[] isTmpData2 = new byte[data.length];

        assertTrue(is2.read(isTmpData2) == data.length);
        assertTrue(is2.read() == -1);
        assertTrue(Arrays.equals(data, isTmpData2));

        assertTrue(!rs.next());
        stmt2.close();
        rs.close();
    }

    public void testBlobSet1() throws Exception {
        byte[] data = getBlobTestData();

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #blobset1 (data BINARY(8000))");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #blobset1 (data) VALUES (?)");

        // Test PreparedStatement.setBinaryStream()
        pstmt.setBinaryStream(1, new ByteArrayInputStream(data), data.length);
        assertTrue(pstmt.executeUpdate() == 1);

        pstmt.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data FROM #blobset1");

        assertTrue(rs.next());

        // Test ResultSet.getBytes()
        assertTrue(Arrays.equals(data, rs.getBytes(1)));

        assertTrue(!rs.next());
        stmt2.close();
        rs.close();
    }

    public void testBlobSet2() throws Exception {
        byte[] data = getBlobTestData();

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #blobset2 (data BINARY(8000))");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #blobset2 (data) VALUES (?)");

        // Test PreparedStatement.setBytes()
        pstmt.setBytes(1, data);
        assertTrue(pstmt.executeUpdate() == 1);

        pstmt.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data FROM #blobset2");

        assertTrue(rs.next());

        Blob blob = rs.getBlob(1);

        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (Math.random() * 255);
        }

        // Test Blob.setBytes()
        blob.setBytes((long) 1, data);

        assertTrue(Arrays.equals(data, blob.getBytes((long) 1, (int) blob.length())));

        assertTrue(!rs.next());

        PreparedStatement pstmt2 = con.prepareStatement("UPDATE #blobset2 SET data = ?");

        // Test PreparedStatement.setBlob()
        pstmt2.setBlob(1, blob);
        assertTrue(pstmt2.executeUpdate() == 1);

        pstmt2.close();

        stmt2.close();
        rs.close();
    
        Statement stmt3 = con.createStatement();
        ResultSet rs2 = stmt3.executeQuery("SELECT data FROM #blobset2");

        assertTrue(rs2.next());

        // Test ResultSet.getBytes()
        assertTrue(Arrays.equals(data, rs2.getBytes(1)));

        assertTrue(!rs2.next());
        stmt3.close();
        rs2.close();
    }

    public void testBlobSet3() throws Exception {
        byte[] data = getBlobTestData();

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #blobset3 (data BINARY(8000))");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #blobset3 (data) VALUES (?)");

        // Test PreparedStatement.setObject(int,byte[])
        pstmt.setObject(1, data);
        assertTrue(pstmt.executeUpdate() == 1);

        pstmt.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data FROM #blobset3");

        assertTrue(rs.next());

        // Test ResultSet.getBytes()
        assertTrue(Arrays.equals(data, rs.getBytes(1)));

        assertTrue(!rs.next());
        stmt2.close();
        rs.close();
    }

    public void testBlobSet4() throws Exception {
        byte[] data = getBlobTestData();

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #blobset4 (data BINARY(8000))");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #blobset4 (data) VALUES (?)");

        // Test PreparedStatement.setBytes()
        pstmt.setBytes(1, data);
        assertTrue(pstmt.executeUpdate() == 1);

        pstmt.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data FROM #blobset4");

        assertTrue(rs.next());

        Blob blob = rs.getBlob(1);

        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (Math.random() * 255);
        }

        // Test Blob.setBytes()
        blob.setBytes((long) 1, data);

        assertTrue(Arrays.equals(data, blob.getBytes((long) 1, (int) blob.length())));

        assertTrue(!rs.next());

        PreparedStatement pstmt2 = con.prepareStatement("UPDATE #blobset4 SET data = ?");

        // Test PreparedStatement.setObject(int,Blob)
        pstmt2.setObject(1, blob);
        assertTrue(pstmt2.executeUpdate() == 1);

        pstmt2.close();

        stmt2.close();
        rs.close();
    
        Statement stmt3 = con.createStatement();
        ResultSet rs2 = stmt3.executeQuery("SELECT data FROM #blobset4");

        assertTrue(rs2.next());

        // Test ResultSet.getBytes()
        assertTrue(Arrays.equals(data, rs2.getBytes(1)));

        assertTrue(!rs2.next());
        stmt3.close();
        rs2.close();
    }

    public void testBlobSet5() throws Exception {
        byte[] data = getBlobTestData();

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #blobset5 (data BINARY(8000))");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #blobset5 (data) VALUES (?)");

        // Test PreparedStatement.setObject(int,byte[],int)
        pstmt.setObject(1, data, Types.BINARY);
        assertTrue(pstmt.executeUpdate() == 1);

        pstmt.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data FROM #blobset5");

        assertTrue(rs.next());

        // Test ResultSet.getBytes()
        assertTrue(Arrays.equals(data, rs.getBytes(1)));

        assertTrue(!rs.next());
        stmt2.close();
        rs.close();
    }

    public void testBlobSet6() throws Exception {
        byte[] data = getBlobTestData();

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #blobset6 (data BINARY(8000))");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #blobset6 (data) VALUES (?)");

        // Test PreparedStatement.setObject(int,byte[],int)
        pstmt.setObject(1, data, Types.VARBINARY);
        assertTrue(pstmt.executeUpdate() == 1);

        pstmt.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data FROM #blobset6");

        assertTrue(rs.next());

        // Test ResultSet.getBytes()
        assertTrue(Arrays.equals(data, rs.getBytes(1)));

        assertTrue(!rs.next());
        stmt2.close();
        rs.close();
    }

    public void testBlobSet7() throws Exception {
        byte[] data = getBlobTestData();

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #blobset7 (data BINARY(8000))");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #blobset7 (data) VALUES (?)");

        // Test PreparedStatement.setBytes()
        pstmt.setBytes(1, data);
        assertTrue(pstmt.executeUpdate() == 1);

        pstmt.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data FROM #blobset7");

        assertTrue(rs.next());

        Blob blob = rs.getBlob(1);

        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (Math.random() * 255);
        }

        // Test Blob.setBytes()
        blob.setBytes((long) 1, data);

        assertTrue(Arrays.equals(data, blob.getBytes((long) 1, (int) blob.length())));

        assertTrue(!rs.next());

        PreparedStatement pstmt2 = con.prepareStatement("UPDATE #blobset7 SET data = ?");

        // Test PreparedStatement.setObject(int,Blob,int)
        pstmt2.setObject(1, blob, Types.BLOB);
        assertTrue(pstmt2.executeUpdate() == 1);

        pstmt2.close();

        stmt2.close();
        rs.close();
    
        Statement stmt3 = con.createStatement();
        ResultSet rs2 = stmt3.executeQuery("SELECT data FROM #blobset7");

        assertTrue(rs2.next());

        // Test ResultSet.getBytes()
        assertTrue(Arrays.equals(data, rs2.getBytes(1)));

        assertTrue(!rs2.next());
        stmt3.close();
        rs2.close();
    }

    public void testClobGet() throws Exception {
        String data = getClobTestData();

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #clobget (data TEXT)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #clobget (data) VALUES (?)");

        pstmt.setString(1, data);
        assertTrue(pstmt.executeUpdate() == 1);

        pstmt.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data FROM #clobget");

        assertTrue(rs.next());

        // Test ResultSet.getString()
        assertTrue(data.equals(rs.getString(1)));

        // Test ResultSet.getAsciiStream()
        InputStream is = rs.getAsciiStream(1);
        byte[] isTmpData = new byte[data.length()];

        assertTrue(is.read(isTmpData) == data.length());
        assertTrue(is.read() == -1);
        assertTrue(data.equals(new String(isTmpData, "ASCII")));

        // Test ResultSet.getUnicodeStream(()
        InputStream is2 = rs.getUnicodeStream(1);
        byte[] isTmpData2 = new byte[data.length()];

        assertTrue(is2.read(isTmpData2) == data.length());
        assertTrue(is2.read() == -1);
        assertTrue(data.equals(new String(isTmpData2, "UTF-8")));

        // Test ResultSet.getCharacterStream()
        Reader rdr = rs.getCharacterStream(1);
        char[] rdrTmpData = new char[data.length()];

        assertTrue(rdr.read(rdrTmpData) == data.length());
        assertTrue(rdr.read() == -1);
        assertTrue(data.equals(new String(rdrTmpData)));

        // Test ResultSet.getClob()
        Clob clob = rs.getClob(1);

        assertTrue(clob != null);

        // Test Clob.length()
        assertTrue(clob.length() == data.length());

        // Test Clob.getSubString()
        assertTrue(data.equals(clob.getSubString((long) 1, (int) clob.length())));

        // Test Clob.getAsciiStream()
        InputStream is3 = clob.getAsciiStream();
        byte[] isTmpData3 = new byte[data.length()];

        assertTrue(is3.read(isTmpData3) == data.length());
        assertTrue(is3.read() == -1);
        assertTrue(data.equals(new String(isTmpData3, "ASCII")));

        // Test Clob.getCharacterStream()
        Reader rdr2 = rs.getCharacterStream(1);
        char[] rdrTmpData2 = new char[data.length()];

        assertTrue(rdr2.read(rdrTmpData2) == data.length());
        assertTrue(rdr2.read() == -1);
        assertTrue(data.equals(new String(rdrTmpData2)));

        assertTrue(!rs.next());
        stmt2.close();
        rs.close();
    }

    public void testClobSet1() throws Exception {
        String data = getClobTestData();

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #clobset1 (data TEXT)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #clobset1 (data) VALUES (?)");

        // Test PreparedStatement.setAsciiStream()
        pstmt.setAsciiStream(1, new ByteArrayInputStream(data.getBytes("ASCII")), data.length());
        assertTrue(pstmt.executeUpdate() == 1);

        pstmt.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data FROM #clobset1");

        assertTrue(rs.next());

        // Test ResultSet.getString()
        assertTrue(data.equals(rs.getString(1)));

        assertTrue(!rs.next());
        stmt2.close();
        rs.close();
    }

    public void testClobSet2() throws Exception {
        String data = getClobTestData();

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #clobset2 (data TEXT)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #clobset2 (data) VALUES (?)");

        // Test PreparedStatement.setCharacterStream()
        pstmt.setCharacterStream(1, new StringReader(data), data.length());
        assertTrue(pstmt.executeUpdate() == 1);

        pstmt.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data FROM #clobset2");

        assertTrue(rs.next());

        // Test ResultSet.getString()
        assertTrue(data.equals(rs.getString(1)));

        assertTrue(!rs.next());
        stmt2.close();
        rs.close();
    }

    public void testClobSet3() throws Exception {
        String data = getClobTestData();

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #clobset3 (data TEXT)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #clobset3 (data) VALUES (?)");

        // Test PreparedStatement.setString()
        pstmt.setString(1, data);
        assertTrue(pstmt.executeUpdate() == 1);

        pstmt.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data FROM #clobset3");

        assertTrue(rs.next());

        Clob clob = rs.getClob(1);
        int length = data.length();

        data = "";

        for (int i = 0; i < length; i++) {
            data += (char) (Math.random() * 90) + 32;
        }

        // Test Clob.setBytes()
        clob.setString((long) 1, data);

        assertTrue(data.equals(clob.getSubString((long) 1, (int) clob.length())));

        assertTrue(!rs.next());

        PreparedStatement pstmt2 = con.prepareStatement("UPDATE #clobset3 SET data = ?");

        // Test PreparedStatement.setClob()
        pstmt2.setClob(1, clob);
        assertTrue(pstmt2.executeUpdate() == 1);

        pstmt2.close();

        stmt2.close();
        rs.close();
    
        Statement stmt3 = con.createStatement();
        ResultSet rs2 = stmt3.executeQuery("SELECT data FROM #clobset3");

        assertTrue(rs2.next());

        // Test ResultSet.getString()
        assertTrue(data.equals(rs2.getString(1)));

        assertTrue(!rs2.next());
        stmt3.close();
        rs2.close();
    }

    public void testClobSet4() throws Exception {
        String data = getClobTestData();

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #clobset4 (data TEXT)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #clobset4 (data) VALUES (?)");

        // Test PreparedStatement.setUnicodeStream()
        pstmt.setUnicodeStream(1, new ByteArrayInputStream(data.getBytes("UTF-8")), data.length());
        assertTrue(pstmt.executeUpdate() == 1);

        pstmt.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data FROM #clobset4");

        assertTrue(rs.next());

        // Test ResultSet.getString()
        assertTrue(data.equals(rs.getString(1)));

        assertTrue(!rs.next());
        stmt2.close();
        rs.close();
    }

    public void testClobSet5() throws Exception {
        String data = getClobTestData();

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #clobset5 (data TEXT)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #clobset5 (data) VALUES (?)");

        // Test PreparedStatement.setObject(int,String)
        pstmt.setObject(1, data);
        assertTrue(pstmt.executeUpdate() == 1);

        pstmt.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data FROM #clobset5");

        assertTrue(rs.next());

        // Test ResultSet.getString()
        assertTrue(data.equals(rs.getString(1)));

        assertTrue(!rs.next());
        stmt2.close();
        rs.close();
    }

    public void testClobSet6() throws Exception {
        String data = getClobTestData();

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #clobset6 (data TEXT)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #clobset6 (data) VALUES (?)");

        // Test PreparedStatement.setString()
        pstmt.setString(1, data);
        assertTrue(pstmt.executeUpdate() == 1);

        pstmt.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data FROM #clobset6");

        assertTrue(rs.next());

        Clob clob = rs.getClob(1);
        int length = data.length();

        data = "";

        for (int i = 0; i < length; i++) {
            data += (char) (Math.random() * 90) + 32;
        }

        // Test Clob.setBytes()
        clob.setString((long) 1, data);

        assertTrue(data.equals(clob.getSubString((long) 1, (int) clob.length())));

        assertTrue(!rs.next());

        PreparedStatement pstmt2 = con.prepareStatement("UPDATE #clobset6 SET data = ?");

        // Test PreparedStatement.setObject(int,Clob)
        pstmt2.setObject(1, clob);
        assertTrue(pstmt2.executeUpdate() == 1);

        pstmt2.close();

        stmt2.close();
        rs.close();
    
        Statement stmt3 = con.createStatement();
        ResultSet rs2 = stmt3.executeQuery("SELECT data FROM #clobset6");

        assertTrue(rs2.next());

        // Test ResultSet.getString()
        assertTrue(data.equals(rs2.getString(1)));

        assertTrue(!rs2.next());
        stmt3.close();
        rs2.close();
    }

    public void testClobSet7() throws Exception {
        String data = getClobTestData();

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #clobset7 (data TEXT)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #clobset7 (data) VALUES (?)");

        // Test PreparedStatement.setObject(int,String,int)
        pstmt.setObject(1, data, Types.LONGVARCHAR);
        assertTrue(pstmt.executeUpdate() == 1);

        pstmt.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data FROM #clobset7");

        assertTrue(rs.next());

        // Test ResultSet.getString()
        assertTrue(data.equals(rs.getString(1)));

        assertTrue(!rs.next());
        stmt2.close();
        rs.close();
    }

    public void testClobSet8() throws Exception {
        String data = getClobTestData();

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #clobset8 (data TEXT)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #clobset8 (data) VALUES (?)");

        // Test PreparedStatement.setString()
        pstmt.setString(1, data);
        assertTrue(pstmt.executeUpdate() == 1);

        pstmt.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data FROM #clobset8");

        assertTrue(rs.next());

        Clob clob = rs.getClob(1);
        int length = data.length();

        data = "";

        for (int i = 0; i < length; i++) {
            data += (char) (Math.random() * 90) + 32;
        }

        // Test Clob.setBytes()
        clob.setString((long) 1, data);

        assertTrue(data.equals(clob.getSubString((long) 1, (int) clob.length())));

        assertTrue(!rs.next());

        PreparedStatement pstmt2 = con.prepareStatement("UPDATE #clobset8 SET data = ?");

        // Test PreparedStatement.setObject(int,Clob,int)
        pstmt2.setObject(1, clob, Types.CLOB);
        assertTrue(pstmt2.executeUpdate() == 1);

        pstmt2.close();

        stmt2.close();
        rs.close();
    
        Statement stmt3 = con.createStatement();
        ResultSet rs2 = stmt3.executeQuery("SELECT data FROM #clobset8");

        assertTrue(rs2.next());

        // Test ResultSet.getString()
        assertTrue(data.equals(rs2.getString(1)));

        assertTrue(!rs2.next());
        stmt3.close();
        rs2.close();
    }

    private byte[] getBlobTestData() {
        byte[] data = new byte[8000];

        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (Math.random() * 255);
        }

        return data;
    }

    private String getClobTestData() {
        StringBuffer data = new StringBuffer();

        for (int i = 0; i < 8000; i++) {
            data.append((char) (Math.random() * 90) + 32);
        }

        return data.toString();
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(LOBTest.class);
    }
}