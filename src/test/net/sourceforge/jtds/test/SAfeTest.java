/*
 * SAfeTest.java
 *
 * Created on 08/23/2002
 */

package net.sourceforge.jtds.test;

import java.sql.*;
import java.math.BigDecimal;

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
    volatile boolean failed;

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
        failed = false;

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

                        assertEquals(null, rs.getWarnings());
                        assertEquals(null, stmt.getWarnings());

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
                        synchronized (o1) {
                            failed = true;
                        }
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
        assertTrue(!failed);
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
        assertEquals(null, stmt.getWarnings());
        assertEquals(null, rs.getWarnings());
        assertEquals("Value", rs.getMetaData().getColumnName(1));
        assertTrue(!rs.isBeforeFirst());
        assertTrue(!rs.isAfterLast());
        assertTrue(!rs.isFirst());
        assertTrue(!rs.isLast());
        rs.next();
        assertTrue(!rs.isBeforeFirst());
        assertTrue(!rs.isAfterLast());
        assertTrue(!rs.isFirst());
        assertTrue(!rs.isLast());
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
        stmt.execute("CREATE PROCEDURE #SAfe0006 @p1 INT, @p2 VARCHAR(20) OUT AS "
                     + "SELECT @p2=CAST(@p1-1 AS VARCHAR(20)) "
                     + "SELECT @p1 AS value "
                     + "RETURN @p1+1");
        stmt.close();

        // Try all formats: escaped, w/ exec and w/o exec
        String[] sql = {"{?=call #SAfe0006(?,?)}", "exec ?=#SAfe0006 ?,?", "?=#SAfe0006 ?,?"};

        for( int i=0; i<sql.length; i++ )
        {
            // Execute it using executeQuery
            CallableStatement cs = con.prepareCall(sql[i]);
            cs.registerOutParameter(1, Types.INTEGER);
            cs.setInt(2, myVal);
            cs.registerOutParameter(3, Types.VARCHAR);
            cs.executeQuery().close();

            assertEquals(myVal+1, cs.getInt(1));
            assertEquals(String.valueOf(myVal-1), cs.getString(3));

            cs.close();

            // Now use execute
            cs = con.prepareCall(sql[i]);
            cs.registerOutParameter(1, Types.INTEGER);
            cs.setInt(2, myVal);
            cs.registerOutParameter(3, Types.VARCHAR);
            assertTrue(cs.execute());
            cs.getResultSet().close();

            assertEquals(myVal+1, cs.getInt(1));
            assertEquals(String.valueOf(myVal-1), cs.getString(3));

            cs.close();
        }
    }

    /**
     * Helper method for <code>testBigDecimal0007</code>. Inserts a BigDecimal
     * value obtained from a double value.
     *
     * @param stmt      <code>PreparedStatement</code> instance
     * @param val       the <code>double</code> value to insert
     * @param scaleFlag if <code>true</code> scale the value to 4, otherwise
     *                  leave it as it is
     */
    private static void insertBigDecimal(PreparedStatement stmt, double val,
                                         boolean scaleFlag)
            throws Exception {
        BigDecimal bd = new BigDecimal(val);
        if (scaleFlag) {
            bd = bd.setScale(4,
                    BigDecimal.ROUND_HALF_EVEN);
        }

        stmt.setBigDecimal(1, bd);
        int rowCount = stmt.executeUpdate();
        assertEquals(1, rowCount);

        assertTrue(stmt.getMoreResults());
        ResultSet rs = stmt.getResultSet();
        assertTrue(rs.next());

        assertEquals("Values don't match.", val, rs.getDouble(1), 0);
    }

    /**
     * Test <code>BigDecimal</code>s created from double values (i.e with very
     * large scales).
     */
    public void testBigDecimal0007() throws Exception {
        Statement createStmt = con.createStatement();
        createStmt.execute("CREATE TABLE #SAfe0007(value MONEY)");
        createStmt.close();

        PreparedStatement stmt = con.prepareStatement(
                "INSERT INTO #SAfe0007(value) VALUES (?) "
                + "SELECT * FROM #SAfe0007 DELETE #SAfe0007");
        // Now test with certain values.
        insertBigDecimal(stmt, 1.1, false);
        insertBigDecimal(stmt, 0.1, false);
        insertBigDecimal(stmt, 0.1, true);
        insertBigDecimal(stmt, 0.01, false);
        insertBigDecimal(stmt, 0.01, true);
        insertBigDecimal(stmt, 0.02, false);
        insertBigDecimal(stmt, 0.02, true);
        insertBigDecimal(stmt, 0.25, false);

        stmt.close();
    }

    /**
     * Test writing <code>long</code> values to VARCHAR fields. There was a
     * regression introduced in release 0.6 that caused <code>long</code>
     * fields to be sent with non-zero scale and appear with decimals when
     * written into VARCHAR fields.
     */
    public void testLongToVarchar0008() throws Exception {
        long myVal = 13;

        Statement createStmt = con.createStatement();
        createStmt.execute("CREATE TABLE #SAfe0008(value VARCHAR(255))");
        createStmt.close();

        PreparedStatement stmt = con.prepareStatement(
                "INSERT INTO #SAfe0008(value) values (?) "
                + "SELECT * FROM #SAfe0008 DELETE #SAfe0008");

        stmt.setLong(1, myVal);
        int rowCount = stmt.executeUpdate();
        assertEquals(1, rowCount);

        assertTrue(stmt.getMoreResults());
        ResultSet rs = stmt.getResultSet();
        assertTrue(rs.next());

        assertEquals("Values don't match.",
                     String.valueOf(myVal), rs.getString(1));

        stmt.close();
    }

    /**
     * Test <code>ResultSet.deleteRow()</code> on updateable result sets.
     */
    public void testDeleteRow0009() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #SAfe0009(value VARCHAR(255) PRIMARY KEY)");
        stmt.close();

        PreparedStatement insStmt = con.prepareStatement(
                "INSERT INTO #SAfe0009(value) values (?)");
        insStmt.setString(1, "Row 1");
        assertEquals(1, insStmt.executeUpdate());
        insStmt.setString(1, "Row 2");
        assertEquals(1, insStmt.executeUpdate());
        insStmt.close();

        stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                                   ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT * FROM #SAfe0009 ORDER BY 1");
        assertEquals(null, stmt.getWarnings());
        assertEquals(null, rs.getWarnings());
        assertTrue(rs.last());
        assertTrue(!rs.rowDeleted());
        rs.deleteRow();
        assertTrue(rs.rowDeleted());
        rs.close();

        rs = stmt.executeQuery("SELECT * FROM #SAfe0009");
        assertTrue(rs.next());
        assertEquals("Row 1", rs.getString(1));
        assertTrue(!rs.next());
        rs.close();
        stmt.close();
    }

    /**
     * Test VARCHAR output parameters returned by CallableStatements.
     * <p>
     * An issue existed, caused by the fact that the parameter was sent to SQL
     * Server as a short VARCHAR (not XORed with 0x80) limiting its length to
     * 255 characters. See bug [815348] for more details.
     */
    public void testCallableStatementVarchar0010() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("CREATE PROCEDURE #SAfe0010 @p1 VARCHAR(2049) OUT AS "
                     + "SELECT @p1 = @p1 + @p1 "
                     + "SELECT @p1 = @p1 + @p1 "
                     + "SELECT @p1 = @p1 + @p1 "
                     + "SELECT @p1 AS value "
                     + "RETURN LEN(@p1)");
        stmt.close();

        // 256 characters long string
        String myVal = "01234567890123456789012345678901234567890123456789"
                + "01234567890123456789012345678901234567890123456789"
                + "01234567890123456789012345678901234567890123456789"
                + "01234567890123456789012345678901234567890123456789"
                + "01234567890123456789012345678901234567890123456789"
                + "012345";

        // Execute it using executeQuery
        CallableStatement cs = con.prepareCall("{?=call #SAfe0010(?)}");
        cs.registerOutParameter(1, Types.INTEGER);
        cs.setString(2, myVal);
        cs.registerOutParameter(2, Types.VARCHAR);
        ResultSet rs = cs.executeQuery();
        assertTrue(rs.next());
        String rsVal = rs.getString(1);
        rs.close();

        assertEquals(myVal.length() * 8, cs.getInt(1));
        assertEquals(rsVal, cs.getString(2));

        cs.close();
    }

    /**
     * Test <code>ResultSet.updateRow()</code> on updateable result sets.
     */
    public void testUpdateRow0011() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #SAfe0011(value VARCHAR(255) PRIMARY KEY)");
        stmt.close();

        PreparedStatement insStmt = con.prepareStatement(
                "INSERT INTO #SAfe0011(value) values (?)");
        insStmt.setString(1, "Row 1");
        assertEquals(1, insStmt.executeUpdate());
        insStmt.setString(1, "Row 2");
        assertEquals(1, insStmt.executeUpdate());
        insStmt.close();

        stmt = con.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                   ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT * FROM #SAfe0011 ORDER BY 1");
        assertEquals(null, stmt.getWarnings());
        assertEquals(null, rs.getWarnings());
        assertTrue(rs.next());
        assertTrue(rs.next());
        rs.updateString(1, "Row X");
        rs.updateRow();
        rs.next();
        assertEquals("Row X", rs.getString(1));
        rs.close();
    }

    /**
     * Test <code>ResultSet.insertRow()</code> on updateable result sets.
     */
    public void testInsertRow0012() throws Exception {
        Statement stmt = con.createStatement();
        stmt.execute("CREATE TABLE #SAfe0012(value VARCHAR(255) PRIMARY KEY)");
        stmt.close();

        PreparedStatement insStmt = con.prepareStatement(
                "INSERT INTO #SAfe0012(value) values (?)");
        insStmt.setString(1, "Row 1");
        assertEquals(1, insStmt.executeUpdate());
        insStmt.setString(1, "Row 2");
        assertEquals(1, insStmt.executeUpdate());
        insStmt.close();

        stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                                   ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT * FROM #SAfe0012 ORDER BY 1");
        assertEquals(null, stmt.getWarnings());
        assertEquals(null, rs.getWarnings());

        // Insert the new row
        rs.moveToInsertRow();
        rs.updateString(1, "Row X");
        rs.insertRow();

        // Check the ResultSet contents
        rs.moveToCurrentRow();
        rs.next();
        assertEquals("Row 1", rs.getString(1));
        rs.next();
        assertEquals("Row 2", rs.getString(1));
        rs.next();
        assertEquals("Row X", rs.getString(1));
        rs.close();
    }
}
