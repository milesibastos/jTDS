package net.sourceforge.jtds.test;

import java.io.*;
import java.sql.*;
import java.util.*;

/**
 * @version 1.0
 */

public class LOBTest extends TestBase {
    private static final int LOB_LENGTH = 8000;
    private static final byte[] blobData = new byte[LOB_LENGTH];
    private static final byte[] newBlobData = new byte[LOB_LENGTH];
    private static final String clobData;
    private static final String newClobData;

    static {
        for (int i = 0; i < blobData.length; i++) {
            blobData[i] = (byte) (Math.random() * 255);
            newBlobData[i] = (byte) (Math.random() * 255);
        }

        StringBuffer data = new StringBuffer();
        StringBuffer newData = new StringBuffer();

        for (int i = 0; i < LOB_LENGTH; i++) {
            data.append((char) (Math.random() * 58) + 32);
            newData.append((char) (Math.random() * 58) + 32);
        }

        clobData = data.toString();
        newClobData = newData.toString();
    }

    public LOBTest(String name) {
        super(name);
    }

    /*************************************************************************
     *************************************************************************
     **                          BLOB TESTS                                 **
     *************************************************************************
     *************************************************************************/

    public void testBlobGet1() throws Exception {
        byte[] data = getBlobTestData();

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #blobget1 (data IMAGE)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #blobget1 (data) VALUES (?)");

        // Test PreparedStatement.setBytes()
        pstmt.setBytes(1, data);
        assertTrue(pstmt.executeUpdate() == 1);

        pstmt.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data FROM #blobget1");

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
        byte[] tmpData2 = blob.getBytes(1L, (int) blob.length());

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

    public void testBlobGet2() throws Exception {
        byte[] data = getBlobTestData();

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #blobget2 (data IMAGE)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #blobget2 (data) VALUES (?)");

        // Test PreparedStatement.setBinaryStream()
        pstmt.setBinaryStream(1, new ByteArrayInputStream(data), data.length);
        assertTrue(pstmt.executeUpdate() == 1);

        pstmt.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data FROM #blobget2");

        assertTrue(rs.next());

        // Test ResultSet.getObject() - Blob
        Object result = rs.getObject(1);

        assertTrue(result instanceof Blob);

        Blob blob = (Blob) result;

        assertTrue(data.length == blob.length());

        // Test Blob.getBytes()
        assertTrue(Arrays.equals(data, blob.getBytes(1L, (int) blob.length())));

        assertTrue(!rs.next());
        stmt2.close();
        rs.close();
    }

    public void testBlobSet1() throws Exception {
        byte[] data = getBlobTestData();

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #blobset1 (data IMAGE)");
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
        stmt.execute("CREATE TABLE #blobset2 (data IMAGE)");
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

        data = getNewBlobTestData();

        // Test Blob.setBytes()
        blob.setBytes((long) 1, data);

        assertTrue(Arrays.equals(data, blob.getBytes(1L, (int) blob.length())));

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
        stmt.execute("CREATE TABLE #blobset3 (data IMAGE)");
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
        stmt.execute("CREATE TABLE #blobset4 (data IMAGE)");
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

        data = getNewBlobTestData();

        // Test Blob.setBytes()
        blob.setBytes((long) 1, data);

        assertTrue(Arrays.equals(data, blob.getBytes(1L, (int) blob.length())));

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
        stmt.execute("CREATE TABLE #blobset5 (data IMAGE)");
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
        stmt.execute("CREATE TABLE #blobset6 (data IMAGE)");
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
        stmt.execute("CREATE TABLE #blobset7 (data IMAGE)");
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

        data = getNewBlobTestData();

        // Test Blob.setBytes()
        blob.setBytes((long) 1, data);

        assertTrue(Arrays.equals(data, blob.getBytes(1L, (int) blob.length())));

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

    public void testBlobUpdate1() throws Exception {
        byte[] data = getBlobTestData();

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #blobupdate1 (id NUMERIC IDENTITY, data IMAGE, "
                     + "CONSTRAINT pk_blobupdate1 PRIMARY KEY CLUSTERED (id))");
        stmt.close();

        stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT id, data FROM #blobupdate1");

        rs.moveToInsertRow();

        // Test ResultSet.updateBytes()
        rs.updateBytes(2, data);

        rs.insertRow();

        stmt.close();
        rs.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs2 = stmt2.executeQuery("SELECT data FROM #blobupdate1");

        assertTrue(rs2.next());

        // Test ResultSet.getBytes()
        assertTrue(Arrays.equals(data, rs2.getBytes(1)));

        assertTrue(!rs2.next());
        stmt2.close();
        rs2.close();
    }

    public void testBlobUpdate2() throws Exception {
        byte[] data = getBlobTestData();

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #blobupdate2 (id NUMERIC IDENTITY, data IMAGE, "
                     + "CONSTRAINT pk_blobupdate2 PRIMARY KEY CLUSTERED (id))");
        stmt.close();


        stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT id, data FROM #blobupdate2");

        rs.moveToInsertRow();

        // Test ResultSet.updateBinaryStream()
        rs.updateBinaryStream(2, new ByteArrayInputStream(data), data.length);

        rs.insertRow();

        stmt.close();
        rs.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs2 = stmt2.executeQuery("SELECT data FROM #blobupdate2");

        assertTrue(rs2.next());

        // Test ResultSet.getBytes()
        assertTrue(Arrays.equals(data, rs2.getBytes(1)));

        assertTrue(!rs2.next());
        stmt2.close();
        rs2.close();
    }

    public void testBlobUpdate3() throws Exception {
        byte[] data = getBlobTestData();

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #blobupdate3 (id NUMERIC IDENTITY, data IMAGE, "
                     + "CONSTRAINT pk_blobupdate3 PRIMARY KEY CLUSTERED (id))");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #blobupdate3 (data) VALUES (?)");

        // Test PreparedStatement.setBytes()
        pstmt.setBytes(1, data);
        assertTrue(pstmt.executeUpdate() == 1);

        pstmt.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data FROM #blobupdate3");

        assertTrue(rs.next());

        Blob blob = rs.getBlob(1);

        data = getNewBlobTestData();

        // Test Blob.setBytes()
        blob.setBytes((long) 1, data);

        assertTrue(Arrays.equals(data, blob.getBytes(1L, (int) blob.length())));

        assertTrue(!rs.next());

        Statement stmt3 = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs2 = stmt3.executeQuery("SELECT id, data FROM #blobupdate3");

        assertTrue(rs2.next());

        // Test ResultSet.updateBlob()
        rs2.updateBlob(2, blob);

        rs2.updateRow();

        assertTrue(!rs2.next());

        stmt2.close();
        rs.close();

        stmt3.close();
        rs2.close();
    
        Statement stmt4 = con.createStatement();
        ResultSet rs3 = stmt4.executeQuery("SELECT data FROM #blobupdate3");

        assertTrue(rs3.next());

        // Test ResultSet.getBytes()
        assertTrue(Arrays.equals(data, rs3.getBytes(1)));

        assertTrue(!rs3.next());
        stmt4.close();
        rs3.close();
    }

    public void testBlobUpdate4() throws Exception {
        byte[] data = getBlobTestData();

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #blobupdate4 (id NUMERIC IDENTITY, data IMAGE, "
                     + "CONSTRAINT pk_blobupdate4 PRIMARY KEY CLUSTERED (id))");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #blobupdate4 (data) VALUES (?)");

        // Test PreparedStatement.setBytes()
        pstmt.setBytes(1, data);
        assertTrue(pstmt.executeUpdate() == 1);

        pstmt.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data FROM #blobupdate4");

        assertTrue(rs.next());

        Blob blob = rs.getBlob(1);

        data = getNewBlobTestData();

        // Test Blob.setBytes()
        blob.setBytes((long) 1, data);

        assertTrue(Arrays.equals(data, blob.getBytes(1L, (int) blob.length())));

        assertTrue(!rs.next());

        Statement stmt3 = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs2 = stmt3.executeQuery("SELECT id, data FROM #blobupdate4");

        assertTrue(rs2.next());

        // Test ResultSet.updateBlob()
        rs2.updateObject(2, blob);

        rs2.updateRow();

        assertTrue(!rs2.next());

        stmt2.close();
        rs.close();

        stmt3.close();
        rs2.close();
    
        Statement stmt4 = con.createStatement();
        ResultSet rs3 = stmt4.executeQuery("SELECT data FROM #blobupdate4");

        assertTrue(rs3.next());

        // Test ResultSet.getBytes()
        assertTrue(Arrays.equals(data, rs3.getBytes(1)));

        assertTrue(!rs3.next());
        stmt4.close();
        rs3.close();
    }

    public void testBlobSetNull1() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #blobsetnull1 (data IMAGE NULL)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #blobsetnull1 (data) VALUES (?)");

        // Test PreparedStatement.setBinaryStream()
        pstmt.setBinaryStream(1, null, 0);
        assertTrue(pstmt.executeUpdate() == 1);

        pstmt.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data FROM #blobsetnull1");

        assertTrue(rs.next());

        // Test ResultSet.getBinaryStream()
        assertTrue(rs.getBinaryStream(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getBlob()
        assertTrue(rs.getBlob(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getBytes()
        assertTrue(rs.getBytes(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getObject()
        assertTrue(rs.getObject(1) == null);
        assertTrue(rs.wasNull());

        assertTrue(!rs.next());
        stmt2.close();
        rs.close();
    }

    public void testBlobSetNull2() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #blobsetnull2 (data IMAGE NULL)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #blobsetnull2 (data) VALUES (?)");

        // Test PreparedStatement.setBlob()
        pstmt.setBlob(1, null);
        assertTrue(pstmt.executeUpdate() == 1);

        pstmt.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data FROM #blobsetnull2");

        assertTrue(rs.next());

        // Test ResultSet.getBinaryStream()
        assertTrue(rs.getBinaryStream(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getBlob()
        assertTrue(rs.getBlob(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getBytes()
        assertTrue(rs.getBytes(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getObject()
        assertTrue(rs.getObject(1) == null);
        assertTrue(rs.wasNull());

        assertTrue(!rs.next());
        stmt2.close();
        rs.close();
    }

    public void testBlobSetNull3() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #blobsetnull3 (data IMAGE NULL)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #blobsetnull3 (data) VALUES (?)");

        // Test PreparedStatement.setBytes()
        pstmt.setBytes(1, null);
        assertTrue(pstmt.executeUpdate() == 1);

        pstmt.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data FROM #blobsetnull3");

        assertTrue(rs.next());

        // Test ResultSet.getBinaryStream()
        assertTrue(rs.getBinaryStream(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getBlob()
        assertTrue(rs.getBlob(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getBytes()
        assertTrue(rs.getBytes(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getObject()
        assertTrue(rs.getObject(1) == null);
        assertTrue(rs.wasNull());

        assertTrue(!rs.next());
        stmt2.close();
        rs.close();
    }

    public void testBlobSetNull4() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #blobsetnull4 (data IMAGE NULL)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #blobsetnull4 (data) VALUES (?)");

        // Test PreparedStatement.setObject(int,Object,int)
        pstmt.setObject(1, null, Types.BLOB);
        assertTrue(pstmt.executeUpdate() == 1);

        pstmt.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data FROM #blobsetnull4");

        assertTrue(rs.next());

        // Test ResultSet.getBinaryStream()
        assertTrue(rs.getBinaryStream(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getBlob()
        assertTrue(rs.getBlob(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getBytes()
        assertTrue(rs.getBytes(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getObject()
        assertTrue(rs.getObject(1) == null);
        assertTrue(rs.wasNull());

        assertTrue(!rs.next());
        stmt2.close();
        rs.close();
    }

    public void testBlobSetNull5() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #blobsetnull5 (data IMAGE NULL)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #blobsetnull5 (data) VALUES (?)");

        // Test PreparedStatement.setNull()
        pstmt.setNull(1, Types.BLOB);
        assertTrue(pstmt.executeUpdate() == 1);

        pstmt.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data FROM #blobsetnull5");

        assertTrue(rs.next());

        // Test ResultSet.getBinaryStream()
        assertTrue(rs.getBinaryStream(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getBlob()
        assertTrue(rs.getBlob(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getBytes()
        assertTrue(rs.getBytes(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getObject()
        assertTrue(rs.getObject(1) == null);
        assertTrue(rs.wasNull());

        assertTrue(!rs.next());
        stmt2.close();
        rs.close();
    }

    public void testBlobSetNull6() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #blobsetnull6 (id NUMERIC IDENTITY, data IMAGE, "
                     + "CONSTRAINT pk_blobsetnull6 PRIMARY KEY CLUSTERED (id))");
        stmt.close();

        stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT id, data FROM #blobsetnull6");

        rs.moveToInsertRow();

        // Test ResultSet.updateBinaryStream()
        rs.updateBinaryStream(2, null, 0);

        rs.insertRow();

        stmt.close();
        rs.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs2 = stmt2.executeQuery("SELECT data FROM #blobsetnull6");

        assertTrue(rs2.next());

        // Test ResultSet.getBinaryStream()
        assertTrue(rs.getBinaryStream(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getBlob()
        assertTrue(rs.getBlob(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getBytes()
        assertTrue(rs.getBytes(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getObject()
        assertTrue(rs.getObject(1) == null);
        assertTrue(rs.wasNull());

        assertTrue(!rs2.next());
        stmt2.close();
        rs2.close();
    }

    public void testBlobSetNull7() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #blobsetnull7 (id NUMERIC IDENTITY, data IMAGE, "
                     + "CONSTRAINT pk_blobsetnull7 PRIMARY KEY CLUSTERED (id))");
        stmt.close();

        stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT id, data FROM #blobsetnull7");

        rs.moveToInsertRow();

        // Test ResultSet.updateBlob()
        rs.updateBlob(2, null);

        rs.insertRow();

        stmt.close();
        rs.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs2 = stmt2.executeQuery("SELECT data FROM #blobsetnull7");

        assertTrue(rs2.next());

        // Test ResultSet.getBinaryStream()
        assertTrue(rs.getBinaryStream(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getBlob()
        assertTrue(rs.getBlob(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getBytes()
        assertTrue(rs.getBytes(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getObject()
        assertTrue(rs.getObject(1) == null);
        assertTrue(rs.wasNull());

        assertTrue(!rs2.next());
        stmt2.close();
        rs2.close();
    }

    public void testBlobSetNull8() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #blobsetnull8 (id NUMERIC IDENTITY, data IMAGE, "
                     + "CONSTRAINT pk_blobsetnull8 PRIMARY KEY CLUSTERED (id))");
        stmt.close();

        stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT id, data FROM #blobsetnull8");

        rs.moveToInsertRow();

        // Test ResultSet.updateBytes()
        rs.updateBytes(2, null);

        rs.insertRow();

        stmt.close();
        rs.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs2 = stmt2.executeQuery("SELECT data FROM #blobsetnull8");

        assertTrue(rs2.next());

        // Test ResultSet.getBinaryStream()
        assertTrue(rs.getBinaryStream(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getBlob()
        assertTrue(rs.getBlob(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getBytes()
        assertTrue(rs.getBytes(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getObject()
        assertTrue(rs.getObject(1) == null);
        assertTrue(rs.wasNull());

        assertTrue(!rs2.next());
        stmt2.close();
        rs2.close();
    }

    public void testBlobSetNull9() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #blobsetnull9 (id NUMERIC IDENTITY, data IMAGE, "
                     + "CONSTRAINT pk_blobsetnull9 PRIMARY KEY CLUSTERED (id))");
        stmt.close();

        stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT id, data FROM #blobsetnull9");

        rs.moveToInsertRow();

        // Test ResultSet.updateObject()
        rs.updateObject(2,  null);

        rs.insertRow();

        stmt.close();
        rs.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs2 = stmt2.executeQuery("SELECT data FROM #blobsetnull9");

        assertTrue(rs2.next());

        // Test ResultSet.getBinaryStream()
        assertTrue(rs.getBinaryStream(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getBlob()
        assertTrue(rs.getBlob(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getBytes()
        assertTrue(rs.getBytes(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getObject()
        assertTrue(rs.getObject(1) == null);
        assertTrue(rs.wasNull());

        assertTrue(!rs2.next());
        stmt2.close();
        rs2.close();
    }

    public void testBlobSetNull10() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #blobsetnull10 (id NUMERIC IDENTITY, data IMAGE, "
                     + "CONSTRAINT pk_blobsetnull10 PRIMARY KEY CLUSTERED (id))");
        stmt.close();

        stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT id, data FROM #blobsetnull10");

        rs.moveToInsertRow();

        // Test ResultSet.updateNull()
        rs.updateNull(2);

        rs.insertRow();

        stmt.close();
        rs.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs2 = stmt2.executeQuery("SELECT data FROM #blobsetnull10");

        assertTrue(rs2.next());

        // Test ResultSet.getBinaryStream()
        assertTrue(rs.getBinaryStream(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getBlob()
        assertTrue(rs.getBlob(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getBytes()
        assertTrue(rs.getBytes(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getObject()
        assertTrue(rs.getObject(1) == null);
        assertTrue(rs.wasNull());

        assertTrue(!rs2.next());
        stmt2.close();
        rs2.close();
    }

    /*************************************************************************
     *************************************************************************
     **                          CLOB TESTS                                 **
     *************************************************************************
     *************************************************************************/

    public void testClobGet1() throws Exception {
        String data = getClobTestData();

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #clobget1 (data TEXT)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #clobget1 (data) VALUES (?)");

        pstmt.setString(1, data);
        assertTrue(pstmt.executeUpdate() == 1);

        pstmt.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data FROM #clobget1");

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
        assertTrue(data.equals(clob.getSubString(1L, (int) clob.length())));

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

    public void testClobGet2() throws Exception {
        String data = getClobTestData();

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #clobget2 (data TEXT)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #clobget2 (data) VALUES (?)");

        // Test PreparedStatement.setCharacterStream()
        pstmt.setCharacterStream(1, new StringReader(data), data.length());
        assertTrue(pstmt.executeUpdate() == 1);

        pstmt.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data FROM #clobget2");

        assertTrue(rs.next());

        // Test ResultSet.getObject() - Clob
        Object result = rs.getObject(1);

        assertTrue(result instanceof Clob);

        Clob clob = (Clob) result;

        assertTrue(data.length() == clob.length());

        // Test Clob.getSubString()
        assertTrue(data.equals(clob.getSubString(1L, (int) clob.length())));

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

        data = getNewClobTestData();

        // Test Clob.setBytes()
        clob.setString((long) 1, data);

        assertTrue(data.equals(clob.getSubString(1L, (int) clob.length())));

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

        data = getNewClobTestData();

        // Test Clob.setBytes()
        clob.setString((long) 1, data);

        assertTrue(data.equals(clob.getSubString(1L, (int) clob.length())));

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

        data = getNewClobTestData();

        // Test Clob.setBytes()
        clob.setString((long) 1, data);

        assertTrue(data.equals(clob.getSubString(1L, (int) clob.length())));

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

    public void testClobUpdate1() throws Exception {
        String data = getClobTestData();

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #clobupdate1 (id NUMERIC IDENTITY, data TEXT, "
                     + "CONSTRAINT pk_clobupdate1 PRIMARY KEY CLUSTERED (id))");
        stmt.close();

        stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT id, data FROM #clobupdate1");

        rs.moveToInsertRow();

        // Test ResultSet.updateString()
        rs.updateString(2, data);

        rs.insertRow();

        stmt.close();
        rs.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs2 = stmt2.executeQuery("SELECT data FROM #clobupdate1");

        assertTrue(rs2.next());

        // Test ResultSet.getString()
        assertTrue(data.equals(rs2.getString(1)));

        assertTrue(!rs2.next());
        stmt2.close();
        rs2.close();
    }

    public void testClobUpdate2() throws Exception {
        String data = getClobTestData();

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #clobupdate2 (id NUMERIC IDENTITY, data TEXT, "
                     + "CONSTRAINT pk_clobupdate2 PRIMARY KEY CLUSTERED (id))");
        stmt.close();

        stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT id, data FROM #clobupdate2");

        rs.moveToInsertRow();

        // Test ResultSet.updateAsciiStream()
        rs.updateAsciiStream(2, new ByteArrayInputStream(data.getBytes("ASCII")), data.length());

        rs.insertRow();

        stmt.close();
        rs.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs2 = stmt2.executeQuery("SELECT data FROM #clobupdate2");

        assertTrue(rs2.next());

        // Test ResultSet.getString()
        assertTrue(data.equals(rs2.getString(1)));

        assertTrue(!rs2.next());
        stmt2.close();
        rs2.close();
    }

    public void testClobUpdate3() throws Exception {
        String data = getClobTestData();

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #clobupdate3 (id NUMERIC IDENTITY, data TEXT, "
                     + "CONSTRAINT pk_clobupdate3 PRIMARY KEY CLUSTERED (id))");
        stmt.close();

        stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT id, data FROM #clobupdate3");

        rs.moveToInsertRow();

        // Test ResultSet.updateCharacterStream()
        rs.updateCharacterStream(2, new StringReader(data), data.length());

        rs.insertRow();

        stmt.close();
        rs.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs2 = stmt2.executeQuery("SELECT data FROM #clobupdate3");

        assertTrue(rs2.next());

        // Test ResultSet.getString()
        assertTrue(data.equals(rs2.getString(1)));

        assertTrue(!rs2.next());
        stmt2.close();
        rs2.close();
    }

    public void testClobUpdate4() throws Exception {
        String data = getClobTestData();

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #clobupdate4 (id NUMERIC IDENTITY, data TEXT, "
                     + "CONSTRAINT pk_clobupdate4 PRIMARY KEY CLUSTERED (id))");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #clobupdate4 (data) VALUES (?)");

        // Test PreparedStatement.setString()
        pstmt.setString(1, data);
        assertTrue(pstmt.executeUpdate() == 1);

        pstmt.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data FROM #clobupdate4");

        assertTrue(rs.next());

        Clob clob = rs.getClob(1);

        data = getNewClobTestData();

        // Test Clob.setBytes()
        clob.setString((long) 1, data);

        assertTrue(data.equals(clob.getSubString(1, (int) clob.length())));

        assertTrue(!rs.next());

        Statement stmt3 = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs2 = stmt3.executeQuery("SELECT id, data FROM #clobupdate4");

        assertTrue(rs2.next());

        // Test ResultSet.updateClob()
        rs2.updateClob(2, clob);

        rs2.updateRow();

        assertTrue(!rs2.next());

        stmt2.close();
        rs.close();

        stmt3.close();
        rs2.close();
    
        Statement stmt4 = con.createStatement();
        ResultSet rs3 = stmt4.executeQuery("SELECT data FROM #clobupdate4");

        assertTrue(rs3.next());

        // Test ResultSet.getString()
        assertTrue(data.equals(rs3.getString(1)));

        assertTrue(!rs3.next());
        stmt4.close();
        rs3.close();
    }

    public void testClobUpdate5() throws Exception {
        String data = getClobTestData();

        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #clobupdate5 (id NUMERIC IDENTITY, data TEXT, "
                     + "CONSTRAINT pk_clobupdate5 PRIMARY KEY CLUSTERED (id))");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #clobupdate5 (data) VALUES (?)");

        // Test PreparedStatement.setString()
        pstmt.setString(1, data);
        assertTrue(pstmt.executeUpdate() == 1);

        pstmt.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data FROM #clobupdate5");

        assertTrue(rs.next());

        Clob clob = rs.getClob(1);

        data = getNewClobTestData();

        // Test Clob.setBytes()
        clob.setString((long) 1, data);

        assertTrue(data.equals(clob.getSubString(1, (int) clob.length())));

        assertTrue(!rs.next());

        Statement stmt3 = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs2 = stmt3.executeQuery("SELECT id, data FROM #clobupdate5");

        assertTrue(rs2.next());

        // Test ResultSet.updateClob()
        rs2.updateClob(2, clob);

        rs2.updateRow();

        assertTrue(!rs2.next());

        stmt2.close();
        rs.close();

        stmt3.close();
        rs2.close();
    
        Statement stmt4 = con.createStatement();
        ResultSet rs3 = stmt4.executeQuery("SELECT data FROM #clobupdate5");

        assertTrue(rs3.next());

        // Test ResultSet.getString()
        assertTrue(data.equals(rs3.getString(1)));

        assertTrue(!rs3.next());
        stmt4.close();
        rs3.close();
    }

    public void testClobSetNull1() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #clobsetnull1 (data TEXT NULL)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #clobsetnull1 (data) VALUES (?)");

        // Test PreparedStatement.setAsciiStream()
        pstmt.setAsciiStream(1, null, 0);
        assertTrue(pstmt.executeUpdate() == 1);

        pstmt.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data FROM #clobsetnull1");

        assertTrue(rs.next());

        // Test ResultSet.getAsciiStream()
        assertTrue(rs.getAsciiStream(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getCharacterStream()
        assertTrue(rs.getCharacterStream(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getClob()
        assertTrue(rs.getClob(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getObject()
        assertTrue(rs.getObject(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getString()
        assertTrue(rs.getString(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getUnicodeStream()
        assertTrue(rs.getUnicodeStream(1) == null);
        assertTrue(rs.wasNull());

        assertTrue(!rs.next());
        stmt2.close();
        rs.close();
    }

    public void testClobSetNull2() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #clobsetnull2 (data TEXT NULL)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #clobsetnull2 (data) VALUES (?)");

        // Test PreparedStatement.setCharacterStream()
        pstmt.setCharacterStream(1, null, 0);
        assertTrue(pstmt.executeUpdate() == 1);

        pstmt.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data FROM #clobsetnull2");

        assertTrue(rs.next());

        // Test ResultSet.getAsciiStream()
        assertTrue(rs.getAsciiStream(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getCharacterStream()
        assertTrue(rs.getCharacterStream(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getClob()
        assertTrue(rs.getClob(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getObject()
        assertTrue(rs.getObject(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getString()
        assertTrue(rs.getString(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getUnicodeStream()
        assertTrue(rs.getUnicodeStream(1) == null);
        assertTrue(rs.wasNull());

        assertTrue(!rs.next());
        stmt2.close();
        rs.close();
    }

    public void testClobSetNull3() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #clobsetnull3 (data TEXT NULL)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #clobsetnull3 (data) VALUES (?)");

        // Test PreparedStatement.setClob()
        pstmt.setClob(1, null);
        assertTrue(pstmt.executeUpdate() == 1);

        pstmt.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data FROM #clobsetnull3");

        assertTrue(rs.next());

        // Test ResultSet.getAsciiStream()
        assertTrue(rs.getAsciiStream(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getCharacterStream()
        assertTrue(rs.getCharacterStream(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getClob()
        assertTrue(rs.getClob(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getObject()
        assertTrue(rs.getObject(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getString()
        assertTrue(rs.getString(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getUnicodeStream()
        assertTrue(rs.getUnicodeStream(1) == null);
        assertTrue(rs.wasNull());

        assertTrue(!rs.next());
        stmt2.close();
        rs.close();
    }

    public void testClobSetNull4() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #clobsetnull4 (data TEXT NULL)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #clobsetnull4 (data) VALUES (?)");

        // Test PreparedStatement.setObject(int,Object)
        pstmt.setObject(1, null);
        assertTrue(pstmt.executeUpdate() == 1);

        pstmt.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data FROM #clobsetnull4");

        assertTrue(rs.next());

        // Test ResultSet.getAsciiStream()
        assertTrue(rs.getAsciiStream(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getCharacterStream()
        assertTrue(rs.getCharacterStream(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getClob()
        assertTrue(rs.getClob(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getObject()
        assertTrue(rs.getObject(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getString()
        assertTrue(rs.getString(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getUnicodeStream()
        assertTrue(rs.getUnicodeStream(1) == null);
        assertTrue(rs.wasNull());

        assertTrue(!rs.next());
        stmt2.close();
        rs.close();
    }

    public void testClobSetNull5() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #clobsetnull5 (data TEXT NULL)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #clobsetnull5 (data) VALUES (?)");

        // Test PreparedStatement.setObject(int,Object,int)
        pstmt.setObject(1, null, Types.CLOB);
        assertTrue(pstmt.executeUpdate() == 1);

        pstmt.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data FROM #clobsetnull5");

        assertTrue(rs.next());

        // Test ResultSet.getAsciiStream()
        assertTrue(rs.getAsciiStream(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getCharacterStream()
        assertTrue(rs.getCharacterStream(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getClob()
        assertTrue(rs.getClob(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getObject()
        assertTrue(rs.getObject(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getString()
        assertTrue(rs.getString(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getUnicodeStream()
        assertTrue(rs.getUnicodeStream(1) == null);
        assertTrue(rs.wasNull());

        assertTrue(!rs.next());
        stmt2.close();
        rs.close();
    }

    public void testClobSetNull6() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #clobsetnull6 (data TEXT NULL)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #clobsetnull6 (data) VALUES (?)");

        // Test PreparedStatement.setString()
        pstmt.setString(1, null);
        assertTrue(pstmt.executeUpdate() == 1);

        pstmt.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data FROM #clobsetnull6");

        assertTrue(rs.next());

        // Test ResultSet.getAsciiStream()
        assertTrue(rs.getAsciiStream(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getCharacterStream()
        assertTrue(rs.getCharacterStream(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getClob()
        assertTrue(rs.getClob(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getObject()
        assertTrue(rs.getObject(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getString()
        assertTrue(rs.getString(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getUnicodeStream()
        assertTrue(rs.getUnicodeStream(1) == null);
        assertTrue(rs.wasNull());

        assertTrue(!rs.next());
        stmt2.close();
        rs.close();
    }

    public void testClobSetNull7() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #clobsetnull7 (data TEXT NULL)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #clobsetnull7 (data) VALUES (?)");

        // Test PreparedStatement.setUnicodeStream()
        pstmt.setUnicodeStream(1, null, 0);
        assertTrue(pstmt.executeUpdate() == 1);

        pstmt.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data FROM #clobsetnull7");

        assertTrue(rs.next());

        // Test ResultSet.getAsciiStream()
        assertTrue(rs.getAsciiStream(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getCharacterStream()
        assertTrue(rs.getCharacterStream(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getClob()
        assertTrue(rs.getClob(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getObject()
        assertTrue(rs.getObject(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getString()
        assertTrue(rs.getString(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getUnicodeStream()
        assertTrue(rs.getUnicodeStream(1) == null);
        assertTrue(rs.wasNull());

        assertTrue(!rs.next());
        stmt2.close();
        rs.close();
    }

    public void testClobSetNull8() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #clobsetnull8 (data TEXT NULL)");
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement("INSERT INTO #clobsetnull8 (data) VALUES (?)");

        // Test PreparedStatement.setNull()
        pstmt.setNull(1, Types.CLOB);
        assertTrue(pstmt.executeUpdate() == 1);

        pstmt.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs = stmt2.executeQuery("SELECT data FROM #clobsetnull8");

        assertTrue(rs.next());

        // Test ResultSet.getAsciiStream()
        assertTrue(rs.getAsciiStream(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getCharacterStream()
        assertTrue(rs.getCharacterStream(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getClob()
        assertTrue(rs.getClob(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getObject()
        assertTrue(rs.getObject(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getString()
        assertTrue(rs.getString(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getUnicodeStream()
        assertTrue(rs.getUnicodeStream(1) == null);
        assertTrue(rs.wasNull());

        assertTrue(!rs.next());
        stmt2.close();
        rs.close();
    }

    public void testClobSetNull9() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #clobsetnull9 (id NUMERIC IDENTITY, data TEXT, "
                     + "CONSTRAINT pk_clobsetnull9 PRIMARY KEY CLUSTERED (id))");
        stmt.close();

        stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT id, data FROM #clobsetnull9");

        rs.moveToInsertRow();

        // Test ResultSet.updateAsciiStream()
        rs.updateAsciiStream(2, null, 0);

        rs.insertRow();

        stmt.close();
        rs.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs2 = stmt2.executeQuery("SELECT data FROM #clobsetnull9");

        assertTrue(rs2.next());

        // Test ResultSet.getAsciiStream()
        assertTrue(rs.getAsciiStream(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getCharacterStream()
        assertTrue(rs.getCharacterStream(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getClob()
        assertTrue(rs.getClob(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getObject()
        assertTrue(rs.getObject(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getString()
        assertTrue(rs.getString(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getUnicodeStream()
        assertTrue(rs.getUnicodeStream(1) == null);
        assertTrue(rs.wasNull());

        assertTrue(!rs2.next());
        stmt2.close();
        rs2.close();
    }

    public void testClobSetNull10() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #clobsetnull10 (id NUMERIC IDENTITY, data TEXT, "
                     + "CONSTRAINT pk_clobsetnull10 PRIMARY KEY CLUSTERED (id))");
        stmt.close();

        stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT id, data FROM #clobsetnull10");

        rs.moveToInsertRow();

        // Test ResultSet.updateCharacterStream()
        rs.updateCharacterStream(2, null, 0);

        rs.insertRow();

        stmt.close();
        rs.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs2 = stmt2.executeQuery("SELECT data FROM #clobsetnull10");

        assertTrue(rs2.next());

        // Test ResultSet.getAsciiStream()
        assertTrue(rs.getAsciiStream(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getCharacterStream()
        assertTrue(rs.getCharacterStream(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getClob()
        assertTrue(rs.getClob(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getObject()
        assertTrue(rs.getObject(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getString()
        assertTrue(rs.getString(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getUnicodeStream()
        assertTrue(rs.getUnicodeStream(1) == null);
        assertTrue(rs.wasNull());

        assertTrue(!rs2.next());
        stmt2.close();
        rs2.close();
    }

    public void testClobSetNull11() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #clobsetnull11 (id NUMERIC IDENTITY, data TEXT, "
                     + "CONSTRAINT pk_clobsetnull11 PRIMARY KEY CLUSTERED (id))");
        stmt.close();

        stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT id, data FROM #clobsetnull11");

        rs.moveToInsertRow();

        // Test ResultSet.updateClob()
        rs.updateClob(2, null);

        rs.insertRow();

        stmt.close();
        rs.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs2 = stmt2.executeQuery("SELECT data FROM #clobsetnull11");

        assertTrue(rs2.next());

        // Test ResultSet.getAsciiStream()
        assertTrue(rs.getAsciiStream(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getCharacterStream()
        assertTrue(rs.getCharacterStream(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getClob()
        assertTrue(rs.getClob(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getObject()
        assertTrue(rs.getObject(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getString()
        assertTrue(rs.getString(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getUnicodeStream()
        assertTrue(rs.getUnicodeStream(1) == null);
        assertTrue(rs.wasNull());

        assertTrue(!rs2.next());
        stmt2.close();
        rs2.close();
    }

    public void testClobSetNull12() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #clobsetnull12 (id NUMERIC IDENTITY, data TEXT, "
                     + "CONSTRAINT pk_clobsetnull12 PRIMARY KEY CLUSTERED (id))");
        stmt.close();

        stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT id, data FROM #clobsetnull12");

        rs.moveToInsertRow();

        // Test ResultSet.updateObject()
        rs.updateObject(2, null);

        rs.insertRow();

        stmt.close();
        rs.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs2 = stmt2.executeQuery("SELECT data FROM #clobsetnull12");

        assertTrue(rs2.next());

        // Test ResultSet.getAsciiStream()
        assertTrue(rs.getAsciiStream(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getCharacterStream()
        assertTrue(rs.getCharacterStream(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getClob()
        assertTrue(rs.getClob(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getObject()
        assertTrue(rs.getObject(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getString()
        assertTrue(rs.getString(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getUnicodeStream()
        assertTrue(rs.getUnicodeStream(1) == null);
        assertTrue(rs.wasNull());

        assertTrue(!rs2.next());
        stmt2.close();
        rs2.close();
    }

    public void testClobSetNull13() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #clobsetnull13 (id NUMERIC IDENTITY, data TEXT, "
                     + "CONSTRAINT pk_clobsetnull13 PRIMARY KEY CLUSTERED (id))");
        stmt.close();

        stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT id, data FROM #clobsetnull13");

        rs.moveToInsertRow();

        // Test ResultSet.updateString()
        rs.updateString(2, null);

        rs.insertRow();

        stmt.close();
        rs.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs2 = stmt2.executeQuery("SELECT data FROM #clobsetnull13");

        assertTrue(rs2.next());

        // Test ResultSet.getAsciiStream()
        assertTrue(rs.getAsciiStream(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getCharacterStream()
        assertTrue(rs.getCharacterStream(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getClob()
        assertTrue(rs.getClob(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getObject()
        assertTrue(rs.getObject(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getString()
        assertTrue(rs.getString(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getUnicodeStream()
        assertTrue(rs.getUnicodeStream(1) == null);
        assertTrue(rs.wasNull());

        assertTrue(!rs2.next());
        stmt2.close();
        rs2.close();
    }

    public void testClobSetNull14() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #clobsetnull14 (id NUMERIC IDENTITY, data TEXT, "
                     + "CONSTRAINT pk_clobsetnull14 PRIMARY KEY CLUSTERED (id))");
        stmt.close();

        stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT id, data FROM #clobsetnull14");

        rs.moveToInsertRow();

        // Test ResultSet.updateNull()
        rs.updateNull(2);

        rs.insertRow();

        stmt.close();
        rs.close();

        Statement stmt2 = con.createStatement();
        ResultSet rs2 = stmt2.executeQuery("SELECT data FROM #clobsetnull14");

        assertTrue(rs2.next());

        // Test ResultSet.getAsciiStream()
        assertTrue(rs.getAsciiStream(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getCharacterStream()
        assertTrue(rs.getCharacterStream(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getClob()
        assertTrue(rs.getClob(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getObject()
        assertTrue(rs.getObject(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getString()
        assertTrue(rs.getString(1) == null);
        assertTrue(rs.wasNull());

        // Test ResultSet.getUnicodeStream()
        assertTrue(rs.getUnicodeStream(1) == null);
        assertTrue(rs.wasNull());

        assertTrue(!rs2.next());
        stmt2.close();
        rs2.close();
    }

    private byte[] getBlobTestData() {
        return blobData;
    }

    private byte[] getNewBlobTestData() {
        return newBlobData;
    }

    private String getClobTestData() {
        return clobData;
    }

    private String getNewClobTestData() {
        return newClobData;
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(LOBTest.class);
    }
}