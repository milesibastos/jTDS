package net.sourceforge.jtds.test;

import junit.framework.AssertionFailedError;
import java.sql.*;

/**
 * @version 1.0
 */
public class PerformanceTest extends TestBase
{
    public PerformanceTest(String name)
    {
        super( name );
    }

    public void testCursorScrollODBC() throws Exception
    {
        try
        {
            connectODBC();
        }
        catch( AssertionFailedError e )
        {
            if( "Connection properties not found (conf/odbc-connection.properties).".equals(e.getMessage()) )
            {
                System.err.println("Skipping ODBC tests.");
                return;
            }
            else
                throw e;
        }
        runCursorScroll( "ODBC", con );
    }

    public void testCursorScroll() throws Exception
    {
        runCursorScroll( "jTDS", con );
    }

    public void testDiscard() throws Exception
    {
        Statement stmt = con.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY );
        ResultSet rs = stmt.executeQuery( "Select top 10000 * from CustomGoods" );

        start( "Discard" );

        while( rs.next() )
        {
            rs.getObject( 1 );
            progress();
        }

        end();

        rs.close();
        stmt.close();

    }

    void runCursorScroll( String name, Connection con ) throws Exception
    {
        Statement stmt = con.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY );
        ResultSet rs = stmt.executeQuery( "Select top 10000 * from CustomGoods" );

        start( name );
        int cnt = rs.getMetaData().getColumnCount();

        while( rs.next() )
        {
            for( int i=1; i<=cnt; i++ )
                rs.getObject(i);
            progress();
        }

        while( rs.previous() )
        {
            for( int i=1; i<=cnt; i++ )
                rs.getObject(i);
            progress();
        }

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
        if( ( count % 100 ) == 0 )
        {
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
