/*
 * SAfeTest.java
 *
 * Created on 08/23/2002
 */

package freetds;

import java.sql.*;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.internetcds.util.Logger;

/**
 * @author  Alin Sinpalean
 * @version 1.0
 * @since   0.4
 */
public class SAfeTest extends DatabaseTestCase
{
    /** @todo Add a test to check if CursorResultSets with 0 results work right. */

    public SAfeTest(String name)
    {
        super(name);
    }

    public static void main(String args[])
    {
        Logger.setActive(true);

        if( args.length > 0 )
        {
            junit.framework.TestSuite s = new TestSuite();
            for( int i=0; i<args.length; i++ )
                s.addTest(new SAfeTest(args[i]));
            junit.textui.TestRunner.run(s);
        }
        else
            junit.textui.TestRunner.run(SAfeTest.class);
    }

    /**
     * Test whether NULL values, 0-length strings and single space strings
     * are treated right.
     */
    public void testNullLengthStrings0001() throws Exception
    {
        String types[] = {
            "VARCHAR(50)",
            "TEXT",
            "VARCHAR(350)",
            "NVARCHAR(50)",
            "NTEXT",
        };
        String values[] = {
            null,
            "",
            " ",
            "x"
        };
        Statement stmt = con.createStatement();
        boolean tds70orLater = props.getProperty("TDS")==null
            || props.getProperty("TDS").charAt(0)>='7';
        int typeCnt = tds70orLater ? types.length : 2;

        for( int i=0; i<typeCnt; i++ )
        {
            assertTrue(stmt.executeUpdate("CREATE TABLE #SAfe#1 (val "+types[i]+" NULL)")==0);

            for( int j=0; j<values.length; j++ )
            {
                String insQuery = values[j]==null ?
                    "INSERT INTO #SAfe#1 VALUES (NULL)" :
                    "INSERT INTO #SAfe#1 VALUES ('"+values[j]+"')";
                assertTrue(stmt.executeUpdate(insQuery)==1);
                ResultSet rs = stmt.executeQuery("SELECT val FROM #SAfe#1");
                assertTrue(rs.next());
                if( tds70orLater || !" ".equals(values[j]) )
                    assertEquals(values[j], rs.getString(1));
                else
                    assertEquals("", rs.getObject(1));
                assertTrue(!rs.next());
                assertTrue(stmt.executeUpdate("TRUNCATE TABLE #SAfe#1")==0);
            }

            assertTrue(stmt.executeUpdate("DROP TABLE #SAfe#1")==0);
        }
    }

    /**
     * Test cancelling. Create 2 connections, lock some records on one of them
     * and try to read them using the other one. Then, try executing some other
     * queries on the second connection to make sure it's in a correct state.
     */
    public void testCancel0002() throws Exception
    {
        // Create another connection to make sure the 2 statements use the same
        // physical connection
        Connection con2 = getConnection();

        Logger.setActive(true);

        Statement stmt = con.createStatement();
        assertTrue(!stmt.execute(
            "create table ##cancel_test (id int primary key, val varchar(20) null) "+
            "insert into ##cancel_test values (1, 'Line 1') "+
            "insert into ##cancel_test values (2, 'Line 2')"));
        assertEquals(0, stmt.getUpdateCount());
        assertTrue(!stmt.getMoreResults());
        assertEquals(1, stmt.getUpdateCount());
        assertTrue(!stmt.getMoreResults());
        assertEquals(1, stmt.getUpdateCount());
        assertTrue(!stmt.getMoreResults());
        assertEquals(-1, stmt.getUpdateCount());

        con.setAutoCommit(false);
        // This is where we lock the first line in the table
        stmt.executeUpdate("update ##cancel_test set val='Updated Line' where id=1");

        Statement stmt2 = con2.createStatement();
        stmt2.setQueryTimeout(1);
        try
        {
            stmt2.executeQuery("select * from ##cancel_test");
            fail();
        }
        catch( SQLException ex )
        {
            // SAfe We won't do an ex.getMessage().equals(...) test here
            //      because the message could change and the test would fail.
            //      We'll just assume we got here because of the timeout. ;o)
        }

        // SAfe What should we do with the results if the execution timed out?!

        con.commit();
        con.setAutoCommit(true);

        stmt.execute("drop table ##cancel_test");
        stmt.close();

        // Just run a tiny query to make sure the stream is still in working
        // condition.
        ResultSet rs = stmt2.executeQuery("select 1");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertTrue(!rs.next());
        con2.close();
    }
}
