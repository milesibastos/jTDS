package freetds;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase;
import java.sql.*;

/**
 * @version 1.0
 */

public class PerformanceTest extends TestBase {

    public PerformanceTest(String name)
    {
        super( name );
    }

    public void testCursorScrollODBC()
        throws Exception
    {
        try {
            connectODBC();
        } catch (AssertionFailedError e) {
            if ("Connection properties not found (conf/odbc-connection.properties).".equals(e.getMessage())) {
                System.err.println("Skipping ODBC tests.");
                return;
            } else {
                throw e;
            }
        }
        runCursorScroll( "ODBC", con );
    }

    public void testCursorScroll()
        throws Exception
    {
        runCursorScroll( "Freetds", con );
    }

    public void testDiscard()
        throws Exception
    {
        Statement stmt = con.createStatement( ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY );
        ResultSet rs = stmt.executeQuery( "Select Registration from Candidate WHERE registration like '5%' ORDER BY Registration" );

        start( "Discard" );

        while ( rs.next() ) {
            Object obj = rs.getObject( 1 );
            progress();
        }

        end();

        rs.close();
        stmt.close();

    }

    void runCursorScroll( String name, Connection con )
    throws Exception
    {
        Statement stmt = con.createStatement( ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY );
        ResultSet rs = stmt.executeQuery( "Select Registration from Candidate WHERE registration like '5%' ORDER BY Registration" );

        start( name );

        while ( rs.next() ) {
            Object obj = rs.getObject( 1 );
            progress();
        }
//        while ( rs.previous() ) {
//            Object obj = rs.getObject( 1 );
//            progress();
//        }


        end();

        rs.close();
        stmt.close();
    }

    long count;
    long start;
    long duration;

    void start( String name )
    {
        System.out.print( "Starting " + name );
        System.out.flush();
        count = 0;
        start = System.currentTimeMillis();
    }

    void progress()
    {
        count++;
        if ( ( count % 100 ) == 0 ) {
            System.out.print( "." );
            System.out.flush();
        }
    }

    void end()
    {
        duration = System.currentTimeMillis() - start;
        System.out.println( "OK" );
        System.out.println( "Time " + duration + "ms.");
    }

    public static void main(String[] args)
    {
        junit.textui.TestRunner.run( PerformanceTest.class );
    }
}

