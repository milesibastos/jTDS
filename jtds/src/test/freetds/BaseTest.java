package freetds;
import java.io.*;
import java.sql.*;

import java.util.*;
import junit.framework.TestCase;

/**
 *  Title: Description: Copyright: Copyright (c) 2001 Company:
 *
 *@author
 *@created    9 August 2001
 *@version    1.0
 */

public class BaseTest extends TestCase {

    Properties props;
    Connection con;

    public BaseTest( String name )
    {
        super( name );
    }


    public void setUp()
        throws Exception
    {
        //Load properties
        File propFile = new File( "conf/connection.properties" );
        props = new Properties();
        props.load( new FileInputStream( propFile ) );
        connect(  );
    }

    public void tearDown()
        throws Exception
    {
        disconnect();
    }

    /**
     * A simple test to make sure everything seems to be OK
     */
    public void testSanity()
        throws Exception
    {
        Statement stmt = con.createStatement();
        makeTestTables( stmt );
        makeObjects( stmt, 100 );
        stmt.close();
    }

    /**
     * Basic test of cursor mechanisms.
     */
    public void testCursorStatements()
        throws Exception
    {
        Statement stmt = con.createStatement();
        makeTestTables( stmt );
        makeObjects( stmt, 100 );

        ResultSet rs;

        boolean cursorCreate = stmt.execute(
                "DECLARE cursor1 SCROLL CURSOR FOR"
                 + "\nSELECT * FROM #test" );

        showWarnings( stmt.getWarnings() );

        boolean cursorOpen = stmt.execute( "OPEN cursor1" );

        rs = stmt.executeQuery( "FETCH LAST FROM cursor1" );
        dump( rs );
        rs.close();

        rs = stmt.executeQuery( "FETCH FIRST FROM cursor1" );
        dump( rs );
        rs.close();

        stmt.execute( "CLOSE cursor1" );

        stmt.execute( "DEALLOCATE cursor1" );

        stmt.close();

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

    public static void main(String[] args)
    {
        junit.textui.TestRunner.run( BaseTest.class );
    }

    public Connection getConnection()
             throws SQLException, ClassNotFoundException
    {
        Class.forName( "com.internetcds.jdbc.tds.Driver" );
        String url = props.getProperty( "url" );
        Connection con = DriverManager.getConnection( url, props );

        showWarnings( con.getWarnings() );

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

    public void testCursorRSCreate()
        throws Exception
    {
        Statement stmt = con.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY );

        makeTestTables( stmt );
        makeObjects( stmt, 100 );

        ResultSet rs = stmt.executeQuery( "Select * from #test" );

        rs.first();
        dumpRow( rs );

        rs.last();
        dumpRow( rs );

        rs.absolute( 27 );
        dumpRow( rs );

        rs.close();
        stmt.close();

    }

    public void testCursorRSScroll()
        throws Exception
    {
        Statement stmt = con.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY );

        makeTestTables( stmt );
        makeObjects( stmt, 100 );

        ResultSet rs = stmt.executeQuery( "Select * from #test" );

        while ( rs.next() ) {
        }

        rs.close();
        stmt.close();
    }

    public Connection getConnectionODBC()
             throws SQLException, ClassNotFoundException
    {
        Class.forName( "sun.jdbc.odbc.JdbcOdbcDriver" );
        String url = props.getProperty( "url-odbc" );
        Connection con = DriverManager.getConnection( url, props );

        showWarnings( con.getWarnings() );

        return con;
    }

}
