package freetds;

import java.sql.*;
import junit.framework.TestCase;

/**
 *  Title: Description: Copyright: Copyright (c) 2001 Company:
 *
 *@author
 *@created    March 16, 2001
 *@version    1.0
 */

public class ScrollableStatementTest extends BaseTest {

    public ScrollableStatementTest( String name )
    {
        super( name );
    }

    public void testCreation()
        throws Exception
    {
        ScrollableStatement stmt = new ScrollableStatement( con.createStatement() );
        stmt.close();
    }

    public void testResultSet()
        throws Exception
    {
        ScrollableStatement stmt = new ScrollableStatement( con.createStatement() );

//        makeObjects( stmt, 100 );
//
//        ScrollableResultSet rs = stmt.executeQuery( "SELECT * FROM #test" );
//        rs.close();

        stmt.close();
    }

    static public void main(String[] args) {
        junit.textui.TestRunner.run( ScrollableStatementTest.class );
    }
}
