package freetds;
import java.io.*;
import java.sql.*;

import java.util.*;
import junit.framework.TestCase;

/**
 * Some simple tests just to make sure everything is working properly
 *
 * @created    9 August 2001
 * @version    1.0
 */

public class SanityTest extends TestBase {


    public SanityTest( String name )
    {
        super( name );
    }

    /**
     * A simple test to make sure everything seems to be OK
     */
    public void testSanity()
        throws Exception
    {
        Statement stmt = con.createStatement();
        makeTestTables( stmt );
        makeObjects( stmt, 5 );
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
        makeObjects( stmt, 5 );

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

    public void testCursorRSCreate()
        throws Exception
    {
        Statement stmt = con.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY );

        makeTestTables( stmt );
        makeObjects( stmt, 5 );

        ResultSet rs = stmt.executeQuery( "Select * from #test" );

        rs.first();
        dumpRow( rs );

        rs.last();
        dumpRow( rs );

        rs.absolute( 4 );
        dumpRow( rs );

        rs.close();
        stmt.close();

    }

    public void testCursorRSScroll()
        throws Exception
    {
        Statement stmt = con.createStatement( ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY );

        makeTestTables( stmt );
        makeObjects( stmt, 5 );

        ResultSet rs = stmt.executeQuery( "Select * from #test" );

        while ( rs.next() ) {
        }

        rs.close();
        stmt.close();
    }

    public static void main(String[] args)
    {
        junit.textui.TestRunner.run( SanityTest.class );
    }

}
