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

package net.sourceforge.jtds.jdbcx;

import java.util.HashMap;
import javax.naming.Reference;

import junit.framework.Test;
import junit.framework.TestSuite;
import net.sourceforge.jtds.jdbc.DefaultProperties;
import net.sourceforge.jtds.jdbc.Driver;
import net.sourceforge.jtds.jdbc.UnitTestBase;
import net.sourceforge.jtds.jdbcx.JtdsDataSource;
import net.sourceforge.jtds.jdbcx.JtdsObjectFactory;

/**
 * Unit tests for the {@link JtdsObjectFactory} class.
 *
 * @author
 *    Alin Sinpalean, David D. Kilzer, Holger Rehn
 */
public class JtdsObjectFactoryTest extends UnitTestBase
{

   /**
    * @return The test suite to run.
    */
   public static Test suite()
   {
      return new TestSuite( JtdsObjectFactoryTest.class );
   }

   /**
    * Constructor.
    *
    * @param name
    * The name of the test.
    */
   public JtdsObjectFactoryTest( String name )
   {
      super( name );
   }

   /**
    * Tests that the factory can correctly rebuild a DataSource with no
    * properties set (i.e. all values should be null and no NPE should be
    * thrown).
    */
   public void testNoProperties() throws Exception
   {
      JtdsDataSource ds = new JtdsDataSource();

      Reference dsRef = ds.getReference();
      assertEquals( "net.sourceforge.jtds.jdbcx.JtdsObjectFactory", dsRef.getFactoryClassName() );
      assertEquals( "net.sourceforge.jtds.jdbcx.JtdsDataSource", dsRef.getClassName() );

      ds = (JtdsDataSource) new JtdsObjectFactory().getObjectInstance( dsRef, null, null, null );

      assertNull  ( ds.getDescription() );
      assertNull  ( ds.getAppName() );
      assertFalse ( ds.getAutoCommit() );
      assertEquals( 0, ds.getBatchSize() );
      assertNull  ( ds.getBindAddress() );
      assertNull  ( ds.getBufferDir() );
      assertEquals( 0, ds.getBufferMaxMemory() );
      assertEquals( 0, ds.getBufferMinPackets() );
      assertFalse ( ds.getCacheMetaData() );
      assertNull  ( ds.getCharset() );
      assertNull  ( ds.getDatabaseName() );
      assertNull  ( ds.getDomain() );
      assertNull  ( ds.getInstance() );
      assertNull  ( ds.getLanguage() );
      assertEquals( false, ds.getLastUpdateCount() );
      assertEquals( 0, ds.getLobBuffer() );
      assertNull  ( ds.getLogFile() );
      assertEquals( 0, ds.getLoginTimeout() );
      assertNull  ( ds.getMacAddress() );
      assertEquals( 0, ds.getMaxStatements() );
      assertEquals( false, ds.getNamedPipe() );
      assertEquals( 0, ds.getPacketSize() );
      assertNull  ( ds.getPassword() );
      assertEquals( 0, ds.getPortNumber() );
      assertEquals( 0, ds.getPrepareSql() );
      assertNull  ( ds.getProcessId() );
      assertNull  ( ds.getProgName() );
      assertEquals( false, ds.getSendStringParametersAsUnicode() );
      assertNull  ( ds.getServerName() );
      assertEquals( 0, ds.getServerType() );
      assertFalse ( ds.getSocketKeepAlive() );
      assertEquals( 0, ds.getSocketTimeout() );
      assertNull  ( ds.getSsl() );
      assertFalse ( ds.getTcpNoDelay() );
      assertNull  ( ds.getTds() );
      assertFalse ( ds.getUseCursors() );
      assertFalse ( ds.getUseJCIFS() );
      assertFalse ( ds.getUseLOBs() );
      assertFalse ( ds.getUseNTLMV2() );
      assertNull  ( ds.getUser() );
      assertNull  ( ds.getWsid() );
      assertFalse ( ds.getXaEmulation() );
   }

   public void testGetterSetter()
      throws Exception
   {
      HashMap defaults = new HashMap();

      defaults.put( "description"                       , "DESCRIPTION"                           );
      defaults.put( Driver.APPNAME                      , DefaultProperties.APP_NAME              );
      defaults.put( Driver.AUTOCOMMIT                   , DefaultProperties.AUTO_COMMIT           );
      defaults.put( Driver.BATCHSIZE                    , DefaultProperties.BATCH_SIZE_SQLSERVER  );
      defaults.put( Driver.BINDADDRESS                  , DefaultProperties.BIND_ADDRESS          );
      defaults.put( Driver.BUFFERDIR                    , DefaultProperties.BUFFER_DIR            );
      defaults.put( Driver.BUFFERMAXMEMORY              , DefaultProperties.BUFFER_MAX_MEMORY     );
      defaults.put( Driver.BUFFERMINPACKETS             , DefaultProperties.BUFFER_MIN_PACKETS    );
      defaults.put( Driver.CACHEMETA                    , DefaultProperties.CACHEMETA             );
      defaults.put( Driver.CHARSET                      , DefaultProperties.CHARSET               );
      defaults.put( Driver.DATABASENAME                 , DefaultProperties.DATABASE_NAME         );
      defaults.put( Driver.DOMAIN                       , DefaultProperties.DOMAIN                );
      defaults.put( Driver.INSTANCE                     , DefaultProperties.INSTANCE              );
      defaults.put( Driver.LANGUAGE                     , DefaultProperties.LANGUAGE              );
      defaults.put( Driver.LASTUPDATECOUNT              , DefaultProperties.LAST_UPDATE_COUNT     );
      defaults.put( Driver.LOBBUFFER                    , DefaultProperties.LOB_BUFFER_SIZE       );
      defaults.put( Driver.LOGFILE                      , DefaultProperties.LOGFILE               );
      defaults.put( Driver.LOGINTIMEOUT                 , DefaultProperties.LOGIN_TIMEOUT         );
      defaults.put( Driver.MACADDRESS                   , DefaultProperties.MAC_ADDRESS           );
      defaults.put( Driver.MAXSTATEMENTS                , DefaultProperties.MAX_STATEMENTS        );
      defaults.put( Driver.NAMEDPIPE                    , DefaultProperties.NAMED_PIPE            );
      defaults.put( Driver.PACKETSIZE                   , DefaultProperties.PACKET_SIZE_42        );
      defaults.put( Driver.PASSWORD                     , DefaultProperties.PASSWORD              );
      defaults.put( Driver.PORTNUMBER                   , DefaultProperties.PORT_NUMBER_SQLSERVER );
      defaults.put( Driver.PREPARESQL                   , DefaultProperties.PREPARE_SQLSERVER     );
      defaults.put( Driver.PROCESSID                    , DefaultProperties.PROCESS_ID            );
      defaults.put( Driver.PROGNAME                     , DefaultProperties.PROG_NAME             );
      defaults.put( Driver.SENDSTRINGPARAMETERSASUNICODE, DefaultProperties.USE_UNICODE           );
      defaults.put( Driver.SERVERNAME                   , "SERVERNAME"                            );
      defaults.put( Driver.SERVERTYPE                   , String.valueOf( Driver.SQLSERVER )      );
      defaults.put( Driver.SOKEEPALIVE                  , DefaultProperties.SOCKET_KEEPALIVE      );
      defaults.put( Driver.SOTIMEOUT                    , DefaultProperties.SOCKET_TIMEOUT        );
      defaults.put( Driver.SSL                          , DefaultProperties.SSL                   );
      defaults.put( Driver.TCPNODELAY                   , DefaultProperties.TCP_NODELAY           );
      defaults.put( Driver.TDS                          , DefaultProperties.TDS_VERSION_42        );
      defaults.put( Driver.USECURSORS                   , DefaultProperties.USECURSORS            );
      defaults.put( Driver.USEJCIFS                     , DefaultProperties.USEJCIFS              );
      defaults.put( Driver.USELOBS                      , DefaultProperties.USELOBS               );
      defaults.put( Driver.USENTLMV2                    , DefaultProperties.USENTLMV2             );
      defaults.put( Driver.USEKERBEROS                  , DefaultProperties.USEKERBEROS           );
      defaults.put( Driver.USER                         , DefaultProperties.USER                  );
      defaults.put( Driver.WSID                         , DefaultProperties.WSID                  );
      defaults.put( Driver.XAEMULATION                  , DefaultProperties.XAEMULATION           );

      JtdsObjectFactory jtdsObjectFactory = new JtdsObjectFactory();

      // create datasource using default values and check values
      JtdsDataSource ds = new JtdsDataSource( defaults );
      checkDefaults( ds );

      // create datasource from reference and check values
      checkDefaults( (JtdsDataSource) jtdsObjectFactory.getObjectInstance( ds.getReference(), null, null, null ) );

      // now us setters for modifying all properties and check via getter whether changes are applied
      ds.setDescription                  ( "TEST" ); assertEquals( "TEST", ds.getDescription()                   );
      ds.setAppName                      ( "TEST" ); assertEquals( "TEST", ds.getAppName()                       );
      ds.setAutoCommit                   ( false  ); assertEquals( false , ds.getAutoCommit()                    );
      ds.setBatchSize                    ( 123456 ); assertEquals( 123456, ds.getBatchSize()                     );
      ds.setBindAddress                  ( "1234" ); assertEquals( "1234", ds.getBindAddress()                   );
      ds.setBufferMaxMemory              ( 123456 ); assertEquals( 123456, ds.getBufferMaxMemory()               );
      ds.setBufferMinPackets             ( 123456 ); assertEquals( 123456, ds.getBufferMinPackets()              );
      ds.setCacheMetaData                ( true   ); assertEquals( true  , ds.getCacheMetaData()                 );
      ds.setCharset                      ( "1234" ); assertEquals( "1234", ds.getCharset()                       );
      ds.setDatabaseName                 ( "1234" ); assertEquals( "1234", ds.getDatabaseName()                  );
      ds.setDomain                       ( "1234" ); assertEquals( "1234", ds.getDomain()                        );
      ds.setInstance                     ( "1234" ); assertEquals( "1234", ds.getInstance()                      );
      ds.setLanguage                     ( "1234" ); assertEquals( "1234", ds.getLanguage()                      );
      ds.setLastUpdateCount              ( false  ); assertEquals( false , ds.getLastUpdateCount()               );
      ds.setLobBuffer                    ( 123456 ); assertEquals( 123456, ds.getLobBuffer()                     );
      ds.setLogFile                      ( "1234" ); assertEquals( "1234", ds.getLogFile()                       );
      ds.setLoginTimeout                 ( 123456 ); assertEquals( 123456, ds.getLoginTimeout()                  );
      ds.setMacAddress                   ( "1234" ); assertEquals( "1234", ds.getMacAddress()                    );
      ds.setMaxStatements                ( 123456 ); assertEquals( 123456, ds.getMaxStatements()                 );
      ds.setNamedPipe                    ( true   ); assertEquals( true  , ds.getNamedPipe()                     );
      ds.setPacketSize                   ( 123456 ); assertEquals( 123456, ds.getPacketSize()                    );
      ds.setPassword                     ( "1234" ); assertEquals( "1234", ds.getPassword()                      );
      ds.setPortNumber                   ( 123456 ); assertEquals( 123456, ds.getPortNumber()                    );
      ds.setPrepareSql                   ( 123456 ); assertEquals( 123456, ds.getPrepareSql()                    );
      ds.setProcessId                    ( "1234" ); assertEquals( "1234", ds.getProcessId()                     );
      ds.setProgName                     ( "1234" ); assertEquals( "1234", ds.getProgName()                      );
      ds.setSendStringParametersAsUnicode( false  ); assertEquals( false , ds.getSendStringParametersAsUnicode() );
      ds.setServerName                   ( "1234" ); assertEquals( "1234", ds.getServerName()                    );
      ds.setServerType                   ( 123456 ); assertEquals( 123456, ds.getServerType()                    );
      ds.setSocketKeepAlive              ( true   ); assertEquals( true  , ds.getSocketKeepAlive()               );
      ds.setSocketTimeout                ( 123456 ); assertEquals( 123456, ds.getSocketTimeout()                 );
      ds.setSsl                          ( "1234" ); assertEquals( "1234", ds.getSsl()                           );
      ds.setTcpNoDelay                   ( false  ); assertEquals( false , ds.getTcpNoDelay()                    );
      ds.setTds                          ( "1234" ); assertEquals( "1234", ds.getTds()                           );
      ds.setUseCursors                   ( true   ); assertEquals( true  , ds.getUseCursors()                    );
      ds.setUseJCIFS                     ( true   ); assertEquals( true  , ds.getUseJCIFS()                      );
      ds.setUseLOBs                      ( false  ); assertEquals( false , ds.getUseLOBs()                       );
      ds.setUseNTLMV2                    ( true   ); assertEquals( true  , ds.getUseNTLMV2()                     );
      ds.setUser                         ( "1234" ); assertEquals( "1234", ds.getUser()                          );
      ds.setWsid                         ( "1234" ); assertEquals( "1234", ds.getWsid()                          );
      ds.setXaEmulation                  ( false  ); assertEquals( false , ds.getXaEmulation()                   );
   }

   private void checkDefaults( JtdsDataSource ds )
   {
      assertEquals( "DESCRIPTION"                          , String.valueOf( ds.getDescription()                   ) );
      assertEquals( DefaultProperties.APP_NAME             , String.valueOf( ds.getAppName()                       ) );
      assertEquals( DefaultProperties.AUTO_COMMIT          , String.valueOf( ds.getAutoCommit()                    ) );
      assertEquals( DefaultProperties.BATCH_SIZE_SQLSERVER , String.valueOf( ds.getBatchSize()                     ) );
      assertEquals( DefaultProperties.BIND_ADDRESS         , String.valueOf( ds.getBindAddress()                   ) );
      assertEquals( DefaultProperties.BUFFER_MAX_MEMORY    , String.valueOf( ds.getBufferMaxMemory()               ) );
      assertEquals( DefaultProperties.BUFFER_MIN_PACKETS   , String.valueOf( ds.getBufferMinPackets()              ) );
      assertEquals( DefaultProperties.CACHEMETA            , String.valueOf( ds.getCacheMetaData()                 ) );
      assertEquals( DefaultProperties.CHARSET              , String.valueOf( ds.getCharset()                       ) );
      assertEquals( DefaultProperties.DATABASE_NAME        , String.valueOf( ds.getDatabaseName()                  ) );
      assertEquals( DefaultProperties.DOMAIN               , String.valueOf( ds.getDomain()                        ) );
      assertEquals( DefaultProperties.INSTANCE             , String.valueOf( ds.getInstance()                      ) );
      assertEquals( DefaultProperties.LANGUAGE             , String.valueOf( ds.getLanguage()                      ) );
      assertEquals( DefaultProperties.LAST_UPDATE_COUNT    , String.valueOf( ds.getLastUpdateCount()               ) );
      assertEquals( DefaultProperties.LOB_BUFFER_SIZE      , String.valueOf( ds.getLobBuffer()                     ) );
      assertEquals( DefaultProperties.LOGFILE              , String.valueOf( ds.getLogFile()                       ) );
      assertEquals( DefaultProperties.LOGIN_TIMEOUT        , String.valueOf( ds.getLoginTimeout()                  ) );
      assertEquals( DefaultProperties.MAC_ADDRESS          , String.valueOf( ds.getMacAddress()                    ) );
      assertEquals( DefaultProperties.MAX_STATEMENTS       , String.valueOf( ds.getMaxStatements()                 ) );
      assertEquals( DefaultProperties.NAMED_PIPE           , String.valueOf( ds.getNamedPipe()                     ) );
      assertEquals( DefaultProperties.PACKET_SIZE_42       , String.valueOf( ds.getPacketSize()                    ) );
      assertEquals( DefaultProperties.PASSWORD             , String.valueOf( ds.getPassword()                      ) );
      assertEquals( DefaultProperties.PORT_NUMBER_SQLSERVER, String.valueOf( ds.getPortNumber()                    ) );
      assertEquals( DefaultProperties.PREPARE_SQLSERVER    , String.valueOf( ds.getPrepareSql()                    ) );
      assertEquals( DefaultProperties.PROCESS_ID           , String.valueOf( ds.getProcessId()                     ) );
      assertEquals( DefaultProperties.PROG_NAME            , String.valueOf( ds.getProgName()                      ) );
      assertEquals( DefaultProperties.USE_UNICODE          , String.valueOf( ds.getSendStringParametersAsUnicode() ) );
      assertEquals( "SERVERNAME"                           , String.valueOf( ds.getServerName()                    ) );
      assertEquals( String.valueOf( Driver.SQLSERVER )     , String.valueOf( ds.getServerType()                    ) );
      assertEquals( DefaultProperties.SOCKET_KEEPALIVE     , String.valueOf( ds.getSocketKeepAlive()               ) );
      assertEquals( DefaultProperties.SOCKET_TIMEOUT       , String.valueOf( ds.getSocketTimeout()                 ) );
      assertEquals( DefaultProperties.SSL                  , String.valueOf( ds.getSsl()                           ) );
      assertEquals( DefaultProperties.TCP_NODELAY          , String.valueOf( ds.getTcpNoDelay()                    ) );
      assertEquals( DefaultProperties.TDS_VERSION_42       , String.valueOf( ds.getTds()                           ) );
      assertEquals( DefaultProperties.USECURSORS           , String.valueOf( ds.getUseCursors()                    ) );
      assertEquals( DefaultProperties.USEJCIFS             , String.valueOf( ds.getUseJCIFS()                      ) );
      assertEquals( DefaultProperties.USELOBS              , String.valueOf( ds.getUseLOBs()                       ) );
      assertEquals( DefaultProperties.USENTLMV2            , String.valueOf( ds.getUseNTLMV2()                     ) );
      assertEquals( DefaultProperties.USER                 , String.valueOf( ds.getUser()                          ) );
      assertEquals( DefaultProperties.WSID                 , String.valueOf( ds.getWsid()                          ) );
      assertEquals( DefaultProperties.XAEMULATION          , String.valueOf( ds.getXaEmulation()                   ) );
   }

}