package net.sourceforge.jtds.test;

import java.io.*;
import java.sql.*;

import java.util.*;
import junit.framework.TestCase;

//
// MJH - Changes made to work with new version of jTDS
// Changed makeTestTables and makeObjects to use their own
// statement objects rather than the one passed in.
// This change is required because this version of jTDS does not
// allow one to invoke say execut(sql) on a prepared or callable statement
// only on a Statement object proper. This seems to be in line with all the
// other drivers I have tested.
//

/**
 * TestBase.java
 *
 * Created on 8. September 2001, 13:34
 *
 * @author  builder
 * @version 1.0
 */
public class TestBase extends TestCase {
    Connection con;
    Properties props;

    public TestBase(String name) {
        super(name);
    }


    public void setUp() throws Exception {
        connect();
    }


    public void tearDown() throws Exception {
        disconnect();
    }


    public Connection getConnection() throws Exception {
        String fileName = "conf/connection.properties";

        props = loadProperties(fileName);
        Class.forName(props.getProperty("driver"));
        String url = props.getProperty("url");
        Connection con = DriverManager.getConnection(url, props);
        showWarnings(con.getWarnings());
        initLanguage(con);

        return con;
    }


    public void showWarnings(SQLWarning w) {
        while (w != null) {
            System.out.println(w.getMessage());
            w = w.getNextWarning();
        }
    }


    private void disconnect() throws Exception {
        if (con != null) {
            con.close();
            con = null;
        }
    }


    protected void connect() throws Exception {
        disconnect();
        con = getConnection();
    }


    protected void connectODBC() throws Exception {
        disconnect();
        con = getConnectionODBC();
    }


    public void dump(ResultSet rs) throws SQLException {
        ResultSetMetaData rsm = rs.getMetaData();
        int cols = rsm.getColumnCount();

        for (int i = 1; i <= cols; i++) {
            if (i > 1) {
                System.out.print(", ");
            }

            System.out.print(rsm.getColumnName(i));
        }

        System.out.println();

        while (rs.next()) {
            dumpRow(rs);
        }
    }


    public void dumpRow(ResultSet rs) throws SQLException {
        ResultSetMetaData rsm = rs.getMetaData();
        int cols = rsm.getColumnCount();

        for (int i = 1; i <= cols; i++) {
            if (i > 1) {
                System.out.print(", ");
            }

            System.out.print(rs.getObject(i));
        }

        System.out.println();
    }


    public Connection getConnectionODBC() throws Exception {
        Class.forName("sun.jdbc.odbc.JdbcOdbcDriver");
        String fileName = "conf/odbc-connection.properties";
        Properties props = loadProperties(fileName);
        String url = props.getProperty("url");
        Connection con = DriverManager.getConnection(url, props);
        showWarnings(con.getWarnings());
        initLanguage(con);

        return con;
    }


    private void initLanguage(Connection con) throws SQLException {
        Statement stmt = con.createStatement();
        stmt.executeUpdate("set LANGUAGE 'us_english'");
        stmt.close();
    }


    private Properties loadProperties(String fileName) throws Exception {
        File propFile = new File(fileName);

        if (!propFile.exists()) {
            fail("Connection properties not found (" + propFile + ").");
        }

        Properties props = new Properties();
        props.load(new FileInputStream(propFile));

        return props;
    }

    protected void makeTestTables(Statement stmt) throws SQLException {
        String sql = "CREATE TABLE #test ("
                     + " f_int INT,"
                     + " f_varchar VARCHAR(255))";
        stmt = con.createStatement();
        stmt.execute(sql);

    }

    public void makeObjects(Statement stmt, int count) throws SQLException {
        stmt = con.createStatement();
        stmt.execute("TRUNCATE TABLE #test");

        for (int i = 0; i < count; i++) {
            String sql = "INSERT INTO #test(f_int, f_varchar)"
                         + " VALUES (" + i + ", 'Row " + i + "')";
            stmt.execute(sql);
        }
    }
    
    public void compareInputStreams(InputStream is1, InputStream is2) throws IOException {
        try {
            if (is1 == null && is2 == null) {
                return;
            } else if (is1 == null && is2 != null) {
                assertTrue("is1 == null && is2 != null", false);
            } else if (is1 != null && is2 == null) {
                assertTrue("is1 != null && is2 == null", false);
            }
            
            long count = 0;
            int value1; 
            int value2; 
            
            while ((value1 = is1.read()) != -1) {
                value2 = is2.read();
                
                if (value2 == -1) {
                    assertTrue("stream 2 EOF at: " + count, false);
                }
                
                assertTrue("stream 1 value [" + value1
                        + "] differs from stream 2 value ["
                        + value2 + "] at: " + count,
                        (value1 == value2));
                
                count++;
            }
        } finally {
            if (is1 != null) {
                is1.close();
            }
            
            if (is2 != null) {
                is2.close();
            }
        }
    }

    public void compareReaders(Reader r1, Reader r2) throws IOException {
        try {
            if (r1 == null && r2 == null) {
                return;
            } else if (r1 == null && r2 != null) {
                assertTrue("r1 == null && r2 != null", false);
            } else if (r1 != null && r2 == null) {
                assertTrue("r1 != null && r2 == null", false);
            }
            
            long count = 0;
            int value1; 
            int value2; 
            
            while ((value1 = r1.read()) != -1) {
                value2 = r2.read();
                
                if (value2 == -1) {
                    assertTrue("reader 2 EOF at: " + count, false);
                }
                
                assertTrue("reader 1 value [" + value1
                        + "] differs from reader 2 value ["
                        + value2 + "] at: " + count,
                        (value1 == value2));
                
                count++;
            }
        } finally {
            if (r1 != null) {
                r1.close();
            }
            
            if (r2 != null) {
                r2.close();
            }
        }
    }
}
