package freetds;

import java.sql.*;
import junit.framework.TestCase;

/**
 * Title:
 * Description:
 * Copyright:    Copyright (c) 2001
 * Company:
 * @author
 * @version 1.0
 */

public class CallableStatementTest
extends BaseTest
{

    public CallableStatementTest( String name )
    {
        super(name);
    }

    public void testCallableStatement()
        throws Exception
    {
    }

    public static void main(String[] args)
    {
        junit.textui.TestRunner.run( CallableStatementTest.class );
    }
}