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
 *
 * @author  builder
 * @version 
 */
public class TestBase extends TestCase {

    Properties props;
    Connection con;

    public TestBase(String name) {
      super(name);
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
