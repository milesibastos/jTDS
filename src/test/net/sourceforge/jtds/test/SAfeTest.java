/*
 * SAfeTest.java
 *
 * Created on 08/23/2002
 */

package net.sourceforge.jtds.test;

import java.sql.*;

import junit.framework.TestSuite;
import net.sourceforge.jtds.util.Logger;

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
            assertTrue(stmt.executeUpdate("CREATE TABLE #SAfe0001 (val "+types[i]+" NULL)")==0);

            for( int j=0; j<values.length; j++ )
            {
                String insQuery = values[j]==null ?
                    "INSERT INTO #SAfe0001 VALUES (NULL)" :
                    "INSERT INTO #SAfe0001 VALUES ('"+values[j]+"')";
                assertTrue(stmt.executeUpdate(insQuery)==1);
                ResultSet rs = stmt.executeQuery("SELECT val FROM #SAfe0001");
                assertTrue(rs.next());
                if( tds70orLater || !" ".equals(values[j]) )
                    assertEquals(values[j], rs.getString(1));
                else
                    assertEquals("", rs.getObject(1));
                assertTrue(!rs.next());
                assertTrue(stmt.executeUpdate("TRUNCATE TABLE #SAfe0001")==0);
            }

            assertTrue(stmt.executeUpdate("DROP TABLE #SAfe0001")==0);
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
            "create table ##SAfe0002 (id int primary key, val varchar(20) null) "+
            "insert into ##SAfe0002 values (1, 'Line 1') "+
            "insert into ##SAfe0002 values (2, 'Line 2')"));
        assertEquals(0, stmt.getUpdateCount());
        assertTrue(!stmt.getMoreResults());
        assertEquals(1, stmt.getUpdateCount());
        assertTrue(!stmt.getMoreResults());
        assertEquals(1, stmt.getUpdateCount());
        assertTrue(!stmt.getMoreResults());
        assertEquals(-1, stmt.getUpdateCount());

        con.setAutoCommit(false);
        // This is where we lock the first line in the table
        stmt.executeUpdate("update ##SAfe0002 set val='Updated Line' where id=1");

        Statement stmt2 = con2.createStatement();
        stmt2.setQueryTimeout(1);
        try
        {
            stmt2.executeQuery("select * from ##SAfe0002");
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

        stmt.execute("drop table ##SAfe0002");
        stmt.close();

        // Just run a tiny query to make sure the stream is still in working
        // condition.
        ResultSet rs = stmt2.executeQuery("select 1");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertTrue(!rs.next());
        con2.close();
    }

    // MT-unsafe!!!
    volatile int started, done;

    /**
     * Test <code>CursorResultSet</code> concurrency. Create a number of threads that execute concurrent queries using
     * scrollable result sets. All requests should be run on the same connection (<code>Tds</code> instance).
     */
    public void testCursorResultSetConcurrency0003() throws Exception
    {
        Statement stmt0 = con.createStatement();
        stmt0.execute("create table #SAfe0003(id int primary key, val varchar(20) null) "+
            "insert into #SAfe0003 values (1, 'Line 1') "+
            "insert into #SAfe0003 values (2, 'Line 2')");
        while( stmt0.getMoreResults() || stmt0.getUpdateCount()!=-1 );

        final Object o1=new Object(), o2=new Object();
        final Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

        int threadCount = 25;
        Thread threads[] = new Thread[threadCount];

        started = done = 0;

        for( int i=0; i<threadCount; i++ )
        {
            threads[i] = new Thread()
            {
                public void run()
                {
                    ResultSet rs = null;

                    try
                    {
                        rs = stmt.executeQuery("SELECT * FROM #SAfe0003");

                        // Synchronize all threads
                        synchronized( o2 )
                        {
                            synchronized( o1 )
                            {
                                started++;
                                o1.notify();
                            }
                            try{ o2.wait(); } catch( InterruptedException e ) {}
                        }

                        assertNotNull("executeQuery should not return null", rs);
                        assertTrue(rs.next());
                        assertTrue(rs.next());
                        assertTrue(!rs.next());
                        assertTrue(rs.previous());
                        assertTrue(rs.previous());
                        assertTrue(!rs.previous());
                    }
                    catch( SQLException e )
                    {
                        e.printStackTrace();
                        fail("An SQL Exception occured: "+e);
                    }
                    finally
                    {
                        if( rs != null )
                            try{ rs.close(); } catch( SQLException e ) {}

                        // Notify that we're done
                        synchronized( o1 )
                        {
                            done++;
                            o1.notify();
                        }
                    }
                }
            };
            threads[i].start();
        }

        while( true )
        {
            synchronized( o1 )
            {
                if( started == threadCount )
                    break;
                o1.wait();
            }
        }

        synchronized( o2 )
        {
            o2.notifyAll();
        }

        boolean passed = true;

        for( int i=0; i<threadCount; i++ )
        {
            stmt0 = con.createStatement();
            ResultSet rs = stmt0.executeQuery("SELECT 1234");
            passed &= rs.next();
            passed &= !rs.next();
            stmt0.close();
        }

        while( true )
        {
            synchronized( o1 )
            {
                if( done == threadCount )
                    break;
                o1.wait();
            }
        }

        for( int i=0; i<threadCount; i++ )
            threads[i].join();

        stmt0.close();
        stmt.close();

        assertTrue(passed);
    }

    /**
     * Check that meta data information is fetched even for empty cursor-based result sets (bug #613199).
     *
     * @throws Exception
     */
    public void testCursorResultSetEmpty0004() throws Exception
    {
        Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = stmt.executeQuery("SELECT 5 Value WHERE 1=0");
        assertEquals("Value", rs.getMetaData().getColumnName(1));
        rs.close();
        stmt.close();
    }

    /**
     * Check that values returned from bit fields are correct (not just 0) (bug #841670).
     *
     * @throws Exception
     */
    public void testBitFields0005() throws Exception
    {
        Statement stmt = con.createStatement();
        stmt.execute("create table #SAfe0005(id int primary key, bit1 bit not null, bit2 bit null) "+
            "insert into #SAfe0005 values (0, 0, 0) "+
            "insert into #SAfe0005 values (1, 1, 1) "+
            "insert into #SAfe0005 values (2, 0, NULL)");
        while( stmt.getMoreResults() || stmt.getUpdateCount()!=-1 );

        ResultSet rs = stmt.executeQuery("SELECT * FROM #SAfe0005");
        while( rs.next() )
        {
            int id = rs.getInt(1);
            int bit1 = rs.getInt(2);
            int bit2 = rs.getInt(3);
            assertTrue("id: "+id+"; bit1: "+bit1+"; bit2: "+bit2, bit1==id%2 && (bit2==id || id==2 && rs.wasNull()));
        }

        rs.close();
        stmt.close();
    }

    /**
     * Test that <code>CallableStatement</code>s with return values work correctly.
     *
     * @throws Exception
     */
    public void testCallableStatement0006() throws Exception
    {
        final int myVal = 13;

        Statement stmt = con.createStatement();
        stmt.execute("CREATE PROCEDURE #SAfe0006 @p1 INT, @p2 VARCHAR(20) OUT AS "+
            "SET @p2=CAST(@p1-1 AS VARCHAR(20)) RETURN @p1+1");
        stmt.close();

        // Try all formats: escaped, w/ exec and w/o exec
        String[] sql = {"{?=call #SAfe0006(?,?)}", "exec ?=#SAfe0006 ?,?", "?=#SAfe0006 ?,?"};

        for( int i=0; i<sql.length; i++ )
        {
            CallableStatement cs = con.prepareCall(sql[i]);
            cs.registerOutParameter(1, Types.INTEGER);
            cs.setInt(2, myVal);
            cs.registerOutParameter(3, Types.VARCHAR);
            cs.execute();

            assertEquals(myVal+1, cs.getInt(1));
            assertEquals(String.valueOf(myVal-1), cs.getString(3));

            cs.close();
        }
    }
}
