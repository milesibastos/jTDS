package net.sourceforge.jtds.test;

import junit.framework.AssertionFailedError;

import java.sql.*;
import java.math.BigDecimal;

/**
 * @version 1.0
 */
public class PerformanceTest extends DatabaseTestCase {
    public PerformanceTest(String name) {
        super(name);
    }

    public void setUp() throws Exception {
        super.setUp();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("CREATE TABLE jTDSPerformanceTest "
                         + "(id int identity(1,1) not null,"
                         + "v_bit bit,"
                         + "v_smallint smallint,"
                         + "v_tinyint tinyint,"
                         + "v_decimal decimal(28,11),"
                         + "v_money money,"
                         + "v_smallmoney smallmoney,"
                         + "v_float float,"
                         + "v_real real,"
                         + "v_datetime datetime,"
                         + "v_smalldatetime smalldatetime,"
                         + "v_char char(255),"
                         + "v_varchar varchar(255),"
                         + "v_binary binary(255),"
                         + "v_varbinary varbinary(255),"
                         + "primary key (id))");
        } catch (SQLException ex) {
            // Ignore it. It just means that the table already exists.
        } finally {
            stmt.close();
        }
    }

    // No better idea for dropping the table. :o(
    public void finalize() throws Exception {
        super.setUp();
        dropTable("jTDSPerformanceTest");
        super.tearDown();
    }

    public void testPreparedStatementODBC() throws Exception {
        try {
            connectODBC();
            populate("ODBC");
        } catch (AssertionFailedError e) {
            if ("Connection properties not found (conf/odbc-connection.properties).".equals(e.getMessage())) {
                System.err.println("Skipping ODBC tests.");
                return;
            } else
                throw e;
        } finally {
            connect();
        }
    }

    public void testPreparedStatement() throws Exception {
        populate("jTDS");
    }

    private void populate(String name) throws SQLException {
        start("Populate " + name);
        PreparedStatement pstmt = con.prepareStatement(
                "insert into jTDSPerformanceTest "
                + "(v_bit, v_smallint, v_tinyint, v_decimal, v_money,"
                + " v_smallmoney, v_float, v_real, v_datetime, v_smalldatetime,"
                + " v_char, v_varchar, v_binary, v_varbinary)"
                + " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)");

        Boolean[] vBit = {Boolean.TRUE, Boolean.FALSE, null};
        BigDecimal dec = new BigDecimal("12374335.34232211212");
        BigDecimal money = new BigDecimal("1234567890.1234");
        BigDecimal smoney = new BigDecimal("123456.1234");
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        byte[] smallBinary = "This is a test.".getBytes();

        for (int i = 0; i < 1000; i++) {
            for (int j = 1; j < 15; j++) {
                if (j == 5 || j == 6) {
                    pstmt.setNull(j, Types.DECIMAL);
                } else if (j == 13 || j == 14) {
                    pstmt.setNull(j, Types.BINARY);
                } else {
                    pstmt.setNull(j, Types.VARCHAR);
                }
            }
            pstmt.setObject(1, vBit[i % vBit.length], Types.BIT);
            pstmt.setShort(2, (short) i);
            pstmt.setByte(3, (byte) i);
            pstmt.setBigDecimal(4, dec);
            pstmt.setBigDecimal(5, money);
            pstmt.setBigDecimal(6, smoney);
            pstmt.setDouble(7, i*13.131313);
            pstmt.setFloat(8, (float) (i*13.131313));
            pstmt.setTimestamp(9, ts);
            pstmt.setTimestamp(10, ts);
            pstmt.setString(11, "This is a test.");
            pstmt.setString(12, "This is a test, too.");
            pstmt.setBytes(13, smallBinary);
            pstmt.setBytes(14, smallBinary);
            assertEquals(1, pstmt.executeUpdate());
            progress();
        }

        pstmt.close();
        end();
    }

    public void testCursorScrollODBC() throws Exception {
        try {
            connectODBC();
            runCursorScroll("ODBC", con);
        } catch (AssertionFailedError e) {
            if ("Connection properties not found (conf/odbc-connection.properties).".equals(e.getMessage())) {
                System.err.println("Skipping ODBC tests.");
                return;
            } else
                throw e;
        } finally {
            connect();
        }
    }

    public void testCursorScroll() throws Exception {
        runCursorScroll("jTDS", con);
    }

    void runCursorScroll(String name, Connection con) throws Exception {
        Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = stmt.executeQuery("Select * from jTDSPerformanceTest");

        start("Cursor scroll " + name);
        int cnt = rs.getMetaData().getColumnCount();

        while (rs.next()) {
            for (int i = 1; i <= cnt; i++)
                rs.getObject(i);
            progress();
        }

        while (rs.previous()) {
            for (int i = 1; i <= cnt; i++)
                rs.getObject(i);
            progress();
        }

        end();

        rs.close();
        stmt.close();
    }

    public void testDiscardODBC() throws Exception {
        try {
            connectODBC();
            runDiscard("ODBC");
        } catch (AssertionFailedError e) {
            if ("Connection properties not found (conf/odbc-connection.properties).".equals(e.getMessage())) {
                System.err.println("Skipping ODBC tests.");
                return;
            } else
                throw e;
        } finally {
            connect();
        }
    }

    public void testDiscard() throws Exception {
        runDiscard("jTDS");
    }

    public void runDiscard(String name) throws Exception {
        Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = stmt.executeQuery("Select * from jTDSPerformanceTest");

        start("Discard " + name);

        while (rs.next()) {
            rs.getObject(1);
            progress();
        }

        end();

        rs.close();
        stmt.close();

    }

    long count;
    long start;
    long duration;

    void start(String name) {
        System.out.print("Starting " + name);
        System.out.flush();
        count = 0;
        start = System.currentTimeMillis();
    }

    void progress() {
        count++;
        if (count % 100 == 0) {
            System.out.print(".");
            System.out.flush();
        }
    }

    void end() {
        duration = System.currentTimeMillis() - start;
        System.out.println("OK");
        System.out.println("Time " + duration + "ms.");
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(PerformanceTest.class);
    }
}
