package net.sourceforge.jtds.test;

import java.sql.*;
import junit.framework.TestCase;

import net.sourceforge.jtds.jdbc.*;

/**
 * @version 1.0
 */

public class PacketRowResultTest extends TestCase {

    public PacketRowResultTest( String name )
    {
        super( name );
    }

    PacketRowResult row;

    public void setUp()
    {
    }
    public void tearDown()
    {
        row = null;
    }

    public void testVarChar()
    {

        try {
            ContextFixture ctx = new ContextFixture( java.sql.Types.VARCHAR );
            row = new PacketRowResult( ctx );

            String test = "This is a test";

            row.setElementAt( 1, test );
            assertEquals( test, row.getElementAt( 1 ) );

        }
        catch ( Exception ex ) {
            ex.printStackTrace();
        }
    }

    public void testInt()
    {

        try {

            ContextFixture ctx = new ContextFixture( java.sql.Types.INTEGER );
            row = new PacketRowResult( ctx );

            Integer test = new Integer( 1 );

            row.setElementAt( 1, test );
            assertEquals(1, ((Long) row.getObjectAs(1, Types.BIGINT)).intValue());
        }
        catch ( Exception ex ) {
            ex.printStackTrace();
        }
    }


    public void doGetAndSet( Object test )
    {
        doGetAndSet( test, test );
    }

    public void doGetAndSet( Object test, Object expect )
    {
    }

    public static void main(String[] args)
    {
        junit.textui.TestRunner.run( PacketRowResultTest.class );
    }
}

class ContextFixture
extends Context
{

    ContextFixture ( int type )
    throws SQLException
    {
        super( new Columns(), null );
        getColumnInfo().setJdbcType( 1, type );
    }

}