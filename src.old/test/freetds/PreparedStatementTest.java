package freetds;

import junit.framework.TestCase;
import java.sql.*;

/**
 * @version 1.0
 */

public class PreparedStatementTest
extends TestBase
{

    public PreparedStatementTest( String name )
    {
        super(name);
    }

    public void testPreparedStatement()
        throws Exception
    {

        PreparedStatement pstmt
            = con.prepareStatement( "SELECT * FROM #test" );

        makeTestTables( pstmt );
        makeObjects( pstmt, 10 );

        ResultSet rs = pstmt.executeQuery();

        dump( rs );

        rs.close();

    }

    public void testScrollablePreparedStatement()
        throws Exception
    {

        Statement stmt = con.createStatement();
        makeTestTables( stmt );
        makeObjects( stmt, 10 );
        stmt.close();

        PreparedStatement pstmt = con.prepareStatement(
            "SELECT * FROM #test",
            ResultSet.TYPE_SCROLL_SENSITIVE,
            ResultSet.CONCUR_READ_ONLY );

        ResultSet rs = pstmt.executeQuery();

        assertTrue( rs.isBeforeFirst() );
        while ( rs.next() ) {}
//        assertTrue( rs.isAfterLast() );

        //This currently fails because the PreparedStatement
        //Doesn't know it needs to create a cursored ResultSet.
        //Needs some refactoring!!
        while ( rs.previous() ) {}
//        assertTrue( rs.isBeforeFirst() );

        rs.close();

    }

    public static void main(String[] args)
    {
        junit.textui.TestRunner.run( PreparedStatementTest.class );
    }
}