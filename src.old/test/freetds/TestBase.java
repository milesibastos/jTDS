/*
 * TestBase.java
 *
 * Created on 8. September 2001, 13:34
 */
package freetds;
import java.io.*;
import java.sql.*;

import java.util.*;
import junit.framework.TestCase;

/**
 * @author  builder
 * @version 1.0
 */
public class TestBase extends TestCase {

    Connection con;
    Properties props;

    public TestBase( String name )
    {
        super( name );
    }


    public void setUp()
             throws Exception
    {
        connect();
    }


    public void tearDown()
             throws Exception
    {
        disconnect();
    }


    public Connection getConnection()
             throws Exception
    {
        Class.forName( "com.internetcds.jdbc.tds.Driver" );
        String fileName = "conf/connection.properties";

        props = loadProperties( fileName );
        String url = props.getProperty( "url" );
        Connection con = DriverManager.getConnection( url, props );
        showWarnings( con.getWarnings() );
        initLanguage( con );
        return con;
    }


    public void showWarnings( SQLWarning w )
    {
        while ( w != null ) {
            System.out.println( w.getMessage() );
            w = w.getNextWarning();
        }
    }


    private void disconnect()
             throws Exception
    {
        if ( con != null ) {
            con.close();
            con = null;
        }
    }


    protected void connect()
             throws Exception
    {
        disconnect();
        con = getConnection();
    }


    protected void connectODBC()
             throws Exception
    {
        disconnect();
        con = getConnectionODBC();
    }


    public void dump( ResultSet rs )
             throws SQLException
    {
        ResultSetMetaData rsm = rs.getMetaData();
        int cols = rsm.getColumnCount();

        for ( int i = 1; i <= cols; i++ ) {
            if ( i > 1 ) {
                System.out.print( ", " );
            }
            System.out.print( rsm.getColumnName( i ) );
        }
        System.out.println();

        while ( rs.next() ) {
            dumpRow( rs );
        }

    }


    public void dumpRow( ResultSet rs )
             throws SQLException
    {
        ResultSetMetaData rsm = rs.getMetaData();
        int cols = rsm.getColumnCount();

        for ( int i = 1; i <= cols; i++ ) {
            if ( i > 1 ) {
                System.out.print( ", " );
            }
            System.out.print( rs.getObject( i ) );
        }
        System.out.println();
    }


    public Connection getConnectionODBC()
             throws Exception
    {

        Class.forName( "sun.jdbc.odbc.JdbcOdbcDriver" );
        String fileName = "conf/odbc-connection.properties";
        Properties props = loadProperties( fileName );
        String url = props.getProperty( "url" );
        Connection con = DriverManager.getConnection( url, props );
        showWarnings( con.getWarnings() );
        initLanguage( con );
        return con;
    }


    private void initLanguage( Connection con ) throws SQLException
    {
        Statement stmt = con.createStatement();
        stmt.executeUpdate( "set LANGUAGE 'us_english'" );
        stmt.close();
    }


    private Properties loadProperties( String fileName )
        throws Exception
    {
        File propFile = new File( fileName );

        if ( !propFile.exists() ) {
            fail( "Connection properties not found (" + propFile + ")." );
        }

        Properties props = new Properties();
        props.load( new FileInputStream( propFile ) );
        return props;
    }

    protected void makeTestTables( Statement stmt )
        throws SQLException
    {
        String sql = "CREATE TABLE #test ("
            + " f_int INT,"
            + " f_varchar VARCHAR(255) )";

        stmt.execute( sql );

    }

    public void makeObjects( Statement stmt, int count )
        throws SQLException
    {
        stmt.execute( "TRUNCATE TABLE #test" );
        for (int i=0; i<count; i++ ) {
            String sql = "INSERT INTO #test(f_int, f_varchar)"
                + " VALUES (" + i + ", 'Row " + i + "')";
            stmt.execute(sql);
        }
    }

}
