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
    public SAfeTest(String name)
    {
        super(name);
    }

    public static void main(String args[])
    {
        try
        {
            Logger.setActive(true);
        }
        catch( java.io.IOException ex )
        {
            throw new RuntimeException("Unexpected exception "+ex
                +" occured in main.");
        }

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
    public void testProc1() throws Exception
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
}
