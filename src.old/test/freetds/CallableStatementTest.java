package freetds;

import java.sql.*;
import junit.framework.TestCase;

/**
 * @version 1.0
 */

public class CallableStatementTest
extends TestBase
{

    public CallableStatementTest( String name )
    {
        super(name);
    }

    public void testCallableStatement()
        throws Exception
    {
        //For now, just test that we can create a CallableStatement
        CallableStatement stmt = con.prepareCall( "THIS IS A TEST ONLY" );
    }

    public static void main(String[] args)
    {
        junit.textui.TestRunner.run( CallableStatementTest.class );
    }
}