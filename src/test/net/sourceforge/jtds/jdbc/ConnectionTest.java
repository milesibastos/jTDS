// jTDS JDBC Driver for Microsoft SQL Server and Sybase
// Copyright (C) 2004 The jTDS Project
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either
// version 2.1 of the License, or (at your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
// Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
//

package net.sourceforge.jtds.jdbc;

import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.*;
import java.util.Properties;
import java.util.Enumeration;

import junit.framework.Test;
import junit.framework.TestSuite;

import net.sourceforge.jtds.jdbc.JtdsConnection;
import net.sourceforge.jtds.jdbc.DefaultProperties;
import net.sourceforge.jtds.jdbc.Messages;

/**
 * Unit test for the {@link JtdsConnection} class.
 *
 * @author
 *    David Kilzer, Alin Sinpalean, Holger Rehn
 */
public class ConnectionTest extends UnitTestBase
{

   /**
    * <p> Construct a test suite for this class. </p>
    *
    * The test suite includes the tests in this class, and adds tests from
    * {@link DefaultPropertiesTestLibrary} after creating an anonymous
    * {@link DefaultPropertiesTester} object.
    *
    * @return
    *    The test suite to run.
    */
   public static Test suite()
   {
      final TestSuite testSuite = new TestSuite( ConnectionTest.class );
      testSuite.addTest( ConnectionTest.TestUnpackProperties.suite( "testUnpackProperties" ) );
      return testSuite;
   }

   /**
    * <p> Constructor. </p>
    *
    * @param name
    *    name of the test
    */
   public ConnectionTest( String name )
   {
      super( name );
   }

   /**
    * Test that an {@link java.sql.SQLException} is thrown when parsing invalid
    * integer (and long) properties.
    */
   public void testInvalidIntegerProperty()
   {
      assertSQLExceptionForBadWholeNumberProperty( Driver.PORTNUMBER   );
      assertSQLExceptionForBadWholeNumberProperty( Driver.SERVERTYPE   );
      assertSQLExceptionForBadWholeNumberProperty( Driver.PREPARESQL   );
      assertSQLExceptionForBadWholeNumberProperty( Driver.PACKETSIZE   );
      assertSQLExceptionForBadWholeNumberProperty( Driver.LOGINTIMEOUT );
      assertSQLExceptionForBadWholeNumberProperty( Driver.LOBBUFFER    );
   }

   /**
    * Class used to test
    * <code>net.sourceforge.jtds.jdbc.JtdsConnection.unpackProperties(Properties)</code>
    * .
    */
   public static class TestUnpackProperties extends DefaultPropertiesTestLibrary
   {

      /**
       * Construct a test suite for this library.
       *
       * @param name
       * The name of the tests.
       * @return The test suite.
       */
      public static Test suite( String name )
      {
         return new TestSuite( ConnectionTest.TestUnpackProperties.class, name );
      }

      /**
       * Default constructor.
       */
      public TestUnpackProperties()
      {
         setTester( new DefaultPropertiesTester()
         {

            @Override
            public void assertDefaultProperty( String message, String url, Properties properties, String fieldName, String key, String expected )
            {

               // FIXME: Hack for JtdsConnection
               {
                  if( "sendStringParametersAsUnicode".equals( fieldName ) )
                  {
                     fieldName = "useUnicode";
                  }
                  else if( "cacheMetaData".equals( fieldName ) )
                  {
                     fieldName = "useMetadataCache";
                  }
               }

               Properties parsedProperties = (Properties) invokeStaticMethod( Driver.class, "parseURL", new Class[] { String.class, Properties.class }, new Object[] { url, properties } );
               parsedProperties = (Properties) invokeStaticMethod( DefaultProperties.class, "addDefaultProperties", new Class[] { Properties.class }, new Object[] { parsedProperties } );
               JtdsConnection instance = (JtdsConnection) invokeConstructor( JtdsConnection.class, new Class[] {}, new Object[] {} );
               invokeInstanceMethod( instance, "unpackProperties", new Class[] { Properties.class }, new Object[] { parsedProperties } );

               String actual = String.valueOf( invokeInstanceMethod( instance, "get" + ucFirst( fieldName ), new Class[] {}, new Object[] {} ) );

               // FIXME: Another hack for JtdsConnection
               {
                  if( "tdsVersion".equals( fieldName ) )
                  {
                     expected = String.valueOf( DefaultProperties.getTdsVersion( expected ) );
                  }
               }

               assertEquals( message, expected, actual );
            }
         } );
      }
   }

   /**
    * Test correct behavior of the <code>charset</code> property. Values should
    * be stored and retrieved using the requested charset rather than the
    * server's as long as Unicode is not used.
    */
   public void testForceCharset1() throws Exception
   {
      // Set charset to Cp1251 and Unicode parameters to false
      Properties props = new Properties();
      props.setProperty( Messages.get( Driver.CHARSET ), "Cp1251" );
      props.setProperty( Messages.get( Driver.SENDSTRINGPARAMETERSASUNICODE ), "false" );
      // Obtain connection
      Connection con = getConnectionOverrideProperties( props );

      try
      {
         // Test both sending and retrieving of values
         String value = "\u0410\u0411\u0412";
         PreparedStatement pstmt = con.prepareStatement( "select ?" );
         pstmt.setString( 1, value );
         ResultSet rs = pstmt.executeQuery();
         assertTrue( rs.next() );
         assertEquals( value, rs.getString( 1 ) );
         assertFalse( rs.next() );
         rs.close();

         pstmt.close();
      }
      finally
      {
         con.close();
      }
   }

   /**
    * Test correct behavior of the <code>charset</code> property. Stored
    * procedure output parameters should be decoded using the specified charset
    * rather than the server's as long as they are non-Unicode.
    */
   public void testForceCharset2()
      throws Exception
   {
      dropProcedure( "testForceCharset2" );

      // Set charset to Cp1251 and Unicode parameters to false
      Properties props = new Properties();
      props.setProperty( Messages.get( Driver.CHARSET ), "Cp1251" );
      props.setProperty( Messages.get( Driver.SENDSTRINGPARAMETERSASUNICODE ), "false" );
      // Obtain connection
      Connection con = getConnectionOverrideProperties( props );

      try
      {
         Statement stmt = con.createStatement();
         assertEquals( 0, stmt.executeUpdate( "create procedure testForceCharset2 " + "@inParam varchar(10), @outParam varchar(10) output as " + "set @outParam = @inParam" ) );
         stmt.close();

         // Test both sending and retrieving of parameters
         String value = "\u0410\u0411\u0412";
         CallableStatement cstmt = con.prepareCall( "{call testForceCharset2(?, ?)}" );
         cstmt.setString( 1, value );
         cstmt.registerOutParameter( 2, Types.VARCHAR );
         assertEquals( 0, cstmt.executeUpdate() );
         assertEquals( value, cstmt.getString( 2 ) );
         cstmt.close();
      }
      finally
      {
         con.close();
      }
   }

   /**
    * Test for bug [1296482] setAutoCommit() behaviour.
    * <p/>
    * The behaviour of setAutoCommit() on JtdsConnection is inconsistent with
    * the Sun JDBC 3.0 Specification. JDBC 3.0 Specification, section 10.1.1:
    * <blockquote>"If the value of auto-commit is changed in the middle of a
    * transaction, the current transaction is committed."</blockquote>
    */
   public void testAutoCommit() throws Exception
   {
      Connection con = getConnectionOverrideProperties( new Properties() );

      try
      {
         Statement stmt = con.createStatement();
         // Create temp table
         assertEquals( 0, stmt.executeUpdate( "create table #testAutoCommit (i int)" ) );
         // Manual commit mode
         con.setAutoCommit( false );
         // Insert one row
         assertEquals( 1, stmt.executeUpdate( "insert into #testAutoCommit (i) values (0)" ) );
         // Set commit mode to manual again; should have no effect
         con.setAutoCommit( false );
         // Rollback the transaction; should roll back the insert
         con.rollback();
         // Insert one more row
         assertEquals( 1, stmt.executeUpdate( "insert into #testAutoCommit (i) values (1)" ) );
         // Set commit mode to automatic; should commit everything
         con.setAutoCommit( true );
         // Go back to manual commit mode
         con.setAutoCommit( false );
         // Rollback transaction; should do nothing
         con.rollback();
         // And back to auto commit mode again
         con.setAutoCommit( true );
         // Now see if the second row is there
         ResultSet rs = stmt.executeQuery( "select i from #testAutoCommit" );
         assertTrue( rs.next() );
         assertEquals( 1, rs.getInt( 1 ) );
         assertFalse( rs.next() );
         // We're done, close everything
         rs.close();
         stmt.close();
      }
      finally
      {
         con.close();
      }
   }

   /**
    * Regression test for bug #673, function expansion causes buffer overflow.
    */
   public void testBug673() throws Exception
   {
      Connection con = getConnection();
      Statement stmt = con.createStatement();

      stmt.execute( "SELECT {fn curdate()}, {fn curdate()}, {fn curdate()}, {fn curdate()}, {fn curdate()}, {fn curdate()}, {fn curdate()}, {fn curdate()}, {fn curdate()}, {fn curdate()}, {fn curdate()}, {fn curdate()}, {fn curdate()}, {fn curdate()}, {fn curdate()}, {fn curdate()}" );
   }

   /**
    * Test that temporary procedures created within transactions with savepoints
    * which are released are still kept in the procedure cache.
    *
    * @test.manual
    *    when testing, prepareSQL will have to be set to 1 to make sure temp procedures are used
    */
   public void testSavepointRelease() throws SQLException
   {
      // Manual commit mode
      con.setAutoCommit( false );
      // Create two savepoints
      Savepoint sp1 = con.setSavepoint();
      Savepoint sp2 = con.setSavepoint();
      // Create and execute a prepared statement
      PreparedStatement stmt = con.prepareStatement( "SELECT 1" );
      assertTrue( stmt.execute() );
      // Release the inner savepoint and rollback the outer
      con.releaseSavepoint( sp2 );
      con.rollback( sp1 );
      // Now make sure the temp stored procedure still exists
      assertTrue( stmt.execute() );
      // Release resources
      stmt.close();
      con.close();
   }

   /**
    * Test for bug [1755448], login failure leaves unclosed sockets.
    */
   public void testUnclosedSocket()
   {
      final int count = 100;

      String url = props.getProperty( "url" ) + ";loginTimeout=600";
      Properties p = new Properties( props );
      p.put( "PASSWORD", "invalid_password" );
      p.put( "loginTimeout", "60" );

      for( int i = 0; i < count; i++ )
      {
         try
         {
            DriverManager.getConnection( url, p );
            assertTrue( false );
         }
         catch( SQLException e )
         {
            assertEquals( 18456, e.getErrorCode() );
         }
      }
   }

   /**
    * Test for bug [2871274], TimerThread prevents classloader from being GCed.
    */
   public void testTimerStop() throws Throwable
   {
      // number of load/unload cycles (use large numbers > 1000 for real stress
      // test)
      int RELOADS = 10;

      // counter for GCed class loaders
      final int[] counter = new int[] { 0 };

      try
      {
         // run the test RELOADS times to ensure everything is GCed correctly
         for( int i = 0; i < RELOADS; i++ )
         {

            // create new classloader for loading the actual test
            ClassLoader cloader = new URLClassLoader( new URL[] { new File( "bin" ).toURI().toURL() }, null )
            {
               @Override
               protected void finalize() throws Throwable
               {
                  counter[0]++;
                  super.finalize();
               }
            };

            // load the actual test class
            Class clazz = cloader.loadClass( testTimerStopHelper.class.getName() );
            Constructor constructor = clazz.getDeclaredConstructor( (Class[]) null );

            // start the test by
            try
            {
               constructor.newInstance( (Object[]) null );
            }
            catch( InvocationTargetException e )
            {
               // extract target exception
               throw e.getTargetException();
            }
         }

         // squeeze out any remaining class loaders
         for( int i = 0; i < 10; i++ )
         {
            System.gc();
            System.runFinalization();
         }

         // ensure some of the created classloaders have been GCed at all
         assertTrue( "jTDS prevented its classloader from being GCed", counter[0] > 0 );

         // ensure that any of the created classloaders has been GCed
         assertEquals( "not all of jTDS' classloaders have been GCed", RELOADS, counter[0] );
      }
      catch( OutOfMemoryError oome )
      {
         fail( "jTDS leaked memory, maybe its classloaders could not be GCed" );
      }
   }

   /**
    * Helper class for test for bug [2871274].
    */
   public static class testTimerStopHelper
   {
      /**
       * Constructor for helper class, simply starts method {@link #test()}.
       */
      public testTimerStopHelper() throws Throwable
      {
         test();
      }

      /**
       * The actual test, creates and closes a number of connections.
       */
      public void test() throws Exception
      {
         // load driver
         Class.forName( "net.sourceforge.jtds.jdbc.Driver" );

         // load connection properties
         Properties p = loadProperties();

         Connection[] conns = new Connection[5];

         // create a number of connections
         for( int c = 0; c < conns.length; c++ )
         {
            conns[c] = DriverManager.getConnection( p.getProperty( "url" ), p );
         }

         // close the previously created connections
         for( int c = 0; c < conns.length; c++ )
         {
            conns[c].close();
         }

         // remove driver from DriverManager
         Enumeration e = DriverManager.getDrivers();
         while( e.hasMoreElements() )
         {
            java.sql.Driver d = (java.sql.Driver) e.nextElement();
            if( d.getClass().getName().equals( "net.sourceforge.jtds.jdbc.Driver" ) )
            {
               DriverManager.deregisterDriver( d );
               break;
            }
         }

         // the class loader should be ready for GC now
      }

      /**
       * Loads the connection properties from config file.
       */
      private static Properties loadProperties() throws Exception
      {
         File propFile = new File( "conf/connection.properties" );

         if( !propFile.exists() ) fail( "Connection properties not found (" + propFile + ")." );

         Properties props = new Properties();
         props.load( new FileInputStream( propFile ) );
         props.put( "loginTimeout", "60" );
         return props;
      }
   }

   /**
    * Assert that an SQLException is thrown when
    * {@link JtdsConnection#unpackProperties(Properties)} is called with an
    * invalid integer (or long) string set on a property.
    * <p/>
    * Note that because Java 1.3 is still supported, the
    * {@link RuntimeException} that is caught may not contain the original
    * {@link Throwable} cause, only the original message.
    *
    * @param key
    * The message key used to retrieve the property name.
    */
   private void assertSQLExceptionForBadWholeNumberProperty( final String key )
   {

      final JtdsConnection instance = (JtdsConnection) invokeConstructor( JtdsConnection.class, new Class[] {}, new Object[] {} );

      Properties properties = (Properties) invokeStaticMethod( Driver.class, "parseURL", new Class[] { String.class, Properties.class }, new Object[] { "jdbc:jtds:sqlserver://servername", new Properties() } );
      properties = (Properties) invokeStaticMethod( DefaultProperties.class, "addDefaultProperties", new Class[] { Properties.class }, new Object[] { properties } );

      properties.setProperty( Messages.get( key ), "1.21 Gigawatts" );

      try
      {
         invokeInstanceMethod( instance, "unpackProperties", new Class[] { Properties.class }, new Object[] { properties } );
         fail( "RuntimeException expected" );
      }
      catch( RuntimeException e )
      {
         assertEquals( "Unexpected exception message", Messages.get( "error.connection.badprop", Messages.get( key ) ), e.getMessage() );
      }
   }

   /**
    * Creates a <code>Connection</code>, overriding the default properties with
    * the ones provided.
    *
    * @param override
    *    the overriding properties
    *
    * @return
    *    a <code>Connection</code> object
    */
   private Connection getConnectionOverrideProperties( Properties override ) throws Exception
   {
      // Get properties, override with provided values
      Properties props = (Properties) TestBase.props.clone();
      for( Enumeration e = override.keys(); e.hasMoreElements(); )
      {
         String key = (String) e.nextElement();
         props.setProperty( key, override.getProperty( key ) );
      }

      // Obtain connection
      Class.forName( props.getProperty( "driver" ) );
      String url = props.getProperty( "url" );
      return DriverManager.getConnection( url, props );
   }

}
