package freetds;

import junit.framework.TestCase;
import java.sql.*;

/**
 * Title:
 * Description:
 * Copyright:    Copyright (c) 2001
 * Company:
 * @author
 * @version 1.0
 */

public class PreparedStatementTest
extends BaseTest
{

    public PreparedStatementTest( String name )
    {
        super(name);
    }

    public void testPreparedStatement()
        throws Exception
    {
        PreparedStatement pstmt = con.prepareStatement( "SELECT * FROM #test" );

        makeTestTables( pstmt );
        makeObjects( pstmt, 10 );

        ResultSet rs = pstmt.executeQuery();

        dump( rs );

        rs.close();

    }

    public static void main(String[] args)
    {
        junit.textui.TestRunner.run( PreparedStatementTest.class );
    }
}