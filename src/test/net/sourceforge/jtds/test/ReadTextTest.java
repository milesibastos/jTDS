package net.sourceforge.jtds.test;

import java.sql.*;
import java.io.*;

/**
 * Test case to illustrate use of READTEXT for text and image columns.
 *
 * @version 1.0
 */
public class ReadTextTest extends TestBase {
    public ReadTextTest(String name) {
        super(name);
    }

    public void testReadText() throws Exception {
        boolean passed = true;
        byte[] byteBuf = new byte[5000]; // Just enough to require more than one READTEXT

        for (int i = 0; i < byteBuf.length; i++) {
            byteBuf[i] = (byte)i;
        }
        StringBuffer strBuf = new StringBuffer(5000);
        for (int i = 0; i < 100; i++) {
            strBuf.append("This is a test line of text that is 50 chars    ");
            if (i < 10) strBuf.append('0');
            strBuf.append(i);
        }
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #TEST (id int, t1 text, t2 ntext, t3 image)");
        PreparedStatement pstmt = con.prepareStatement(
                                                      "INSERT INTO #TEST (id, t1, t2, t3) VALUES (?, ?, ?, ?)");
        pstmt.setInt(1, 1);
        try {
            pstmt.setAsciiStream(2, new ByteArrayInputStream(strBuf.toString().getBytes("US-ASCII")), strBuf.length());
            pstmt.setCharacterStream(3, new StringReader(strBuf.toString()), strBuf.length());
            pstmt.setBinaryStream(4, new ByteArrayInputStream(byteBuf), byteBuf.length);
        } catch (UnsupportedEncodingException e) {
            // Should never happen
        }
        assertEquals("First insert failed", 1, pstmt.executeUpdate());
        pstmt.setInt(1, 2);
        try {
            pstmt.setCharacterStream(2, new StringReader(strBuf.toString()), strBuf.length());
            pstmt.setAsciiStream(3, new ByteArrayInputStream(strBuf.toString().getBytes("US-ASCII")), strBuf.length());
            pstmt.setBinaryStream(4, new ByteArrayInputStream(byteBuf), byteBuf.length);
        } catch (UnsupportedEncodingException e) {
            // Should never happen
        }
        assertEquals("Second insert failed", 1, pstmt.executeUpdate());
        // Read back the normal way
        ResultSet rs = stmt.executeQuery("SELECT * FROM #TEST");
        assertNotNull(rs);
        while (rs.next()) {
            switch (rs.getInt(1)) {
                case 1:
                    InputStream in = rs.getAsciiStream(2);
                    for (int i = 0; i < strBuf.length(); i++) {
                        if (strBuf.charAt(i) != (char)in.read()) {
                            System.out.println("ascii stream differs at byte " + i);
                            passed = false;
                            break;
                        }
                    }
                    Reader rin = rs.getCharacterStream(3);
                    for (int i = 0; i < strBuf.length(); i++) {
                        if (strBuf.charAt(i) != (char)rin.read()) {
                            System.out.println("character stream differs at byte " + i);
                            passed = false;
                            break;
                        }
                    }
                    in = rs.getBinaryStream(4);
                    for (int i = 0; i < strBuf.length(); i++) {
                        if (byteBuf[i] != (byte)in.read()) {
                            System.out.println("binary stream differs at byte " + i);
                            passed = false;
                            break;
                        }
                    }
                    break;
                case 2:
                    rin = rs.getCharacterStream(2);
                    for (int i = 0; i < strBuf.length(); i++) {
                        if (strBuf.charAt(i) != (char)rin.read()) {
                            System.out.println("character stream differs at byte " + i);
                            passed = false;
                            break;
                        }
                    }
                    in = rs.getAsciiStream(3);
                    for (int i = 0; i < strBuf.length(); i++) {
                        if (strBuf.charAt(i) != (char)in.read()) {
                            System.out.println("ascii stream differs at byte " + i);
                            passed = false;
                            break;
                        }
                    }
                    in = rs.getBinaryStream(4);
                    for (int i = 0; i < strBuf.length(); i++) {
                        if (byteBuf[i] != (byte)in.read()) {
                            System.out.println("binary stream differs at byte " + i);
                            passed = false;
                            break;
                        }
                    }
                    break;
            }
        }
        rs.close();
        // Read back using READTEXT
        stmt.setMaxFieldSize(1); // Trigger use of READTEXT
        rs = stmt.executeQuery("SELECT * FROM #TEST");
        assertNotNull(rs);
        while (rs.next()) {
            switch (rs.getInt(1)) {
                case 1:
                    InputStream in = rs.getAsciiStream(2);
                    for (int i = 0; i < strBuf.length(); i++) {
                        if (strBuf.charAt(i) != (char)in.read()) {
                            System.out.println("ascii stream differs at byte " + i);
                            passed = false;
                            break;
                        }
                    }
                    Reader rin = rs.getCharacterStream(3);
                    for (int i = 0; i < strBuf.length(); i++) {
                        char c1 = (char)rin.read();
                        if (strBuf.charAt(i) != c1) {
                            System.out.println("character stream differs at byte " + i + " expected " +
                                               strBuf.charAt(i) + " got " + c1);
                            passed = false;
                            break;
                        }
                    }
                    in = rs.getBinaryStream(4);
                    for (int i = 0; i < strBuf.length(); i++) {
                        if (byteBuf[i] != (byte)in.read()) {
                            System.out.println("binary stream differs at byte " + i);
                            passed = false;
                            break;
                        }
                    }
                    break;
                case 2:
                    rin = rs.getCharacterStream(2);
                    for (int i = 0; i < strBuf.length(); i++) {
                        if (strBuf.charAt(i) != (char)rin.read()) {
                            System.out.println("character stream differs at byte " + i);
                            passed = false;
                            break;
                        }
                    }
                    in = rs.getAsciiStream(3);
                    for (int i = 0; i < strBuf.length(); i++) {
                        if (strBuf.charAt(i) != (char)in.read()) {
                            System.out.println("ascii stream differs at byte " + i);
                            passed = false;
                            break;
                        }
                    }
                    in = rs.getBinaryStream(4);
                    for (int i = 0; i < strBuf.length(); i++) {
                        if (byteBuf[i] != (byte)in.read()) {
                            System.out.println("binary stream differs at byte " + i);
                            passed = false;
                            break;
                        }
                    }
                    break;
            }
        }
        assertTrue(passed);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(ReadTextTest.class);
    }
}
