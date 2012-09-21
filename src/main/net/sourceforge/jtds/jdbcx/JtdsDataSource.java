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

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;
import javax.sql.XAConnection;
import javax.sql.XADataSource;

import net.sourceforge.jtds.jdbc.DefaultProperties;
import net.sourceforge.jtds.jdbc.Driver;
import net.sourceforge.jtds.jdbc.Messages;
import net.sourceforge.jtds.jdbc.Support;
import net.sourceforge.jtds.util.Logger;

/**
 * The jTDS <code>DataSource</code>, <code>ConnectionPoolDataSource</code> and
 * <code>XADataSource</code> implementation.
 *
 * @author
 *    Alin Sinplean, Holger Rehn
 *
 * @since
 *    jTDS 0.3
 */
public class JtdsDataSource implements DataSource, ConnectionPoolDataSource, XADataSource, Referenceable, Serializable
{

   /**
    * serial version UID
    */
   static final long           serialVersionUID = 01010001L;

   static final String         DESCRIPTION      = "description";

   private final HashMap       _Config;

   /**
    * Driver instance used for obtaining connections.
    */
   private static final Driver _Driver          = new Driver();

   /**
    * Constructs a configured DataSource.
    */
   JtdsDataSource( HashMap config )
   {
      // no need to clone, only called by JtdsObjectFactory
      _Config = config;
   }

   /**
    * Constructs a new DataSource.
    */
   public JtdsDataSource()
   {
      // Do not set default property values here. Properties whose default
      // values depend on server type will likely be incorrect unless the
      // user specified them explicitly.

      _Config = new HashMap();
   }

   /**
    * Returns a new XA database connection.
    *
    * @return a new database connection
    * @throws SQLException
    * if an error occurs
    */
   public XAConnection getXAConnection()
      throws SQLException
   {
      return new JtdsXAConnection( this, getConnection( (String) _Config.get( Driver.USER ), (String) _Config.get( Driver.PASSWORD ) ) );
   }

   /**
    * Returns a new XA database connection for the user and password specified.
    *
    * @param user
    * the user name to connect with
    * @param password
    * the password to connect with
    * @return a new database connection
    * @throws SQLException
    * if an error occurs
    */
   public XAConnection getXAConnection( String user, String password )
      throws SQLException
   {
      return new JtdsXAConnection( this, getConnection( user, password ) );
   }

   /**
    * Returns a new database connection.
    *
    * @return a new database connection
    * @throws SQLException
    * if an error occurs
    */
   public Connection getConnection()
      throws SQLException
   {
      return getConnection( (String) _Config.get( Driver.USER ), (String) _Config.get( Driver.PASSWORD ) );
   }

   /**
    * Returns a new database connection for the user and password specified.
    *
    * @param user
    * the user name to connect with
    * @param password
    * the password to connect with
    * @return a new database connection
    * @throws SQLException
    * if an error occurs
    */
   public Connection getConnection( String user, String password )
      throws SQLException
   {
      String servername = (String) _Config.get( Driver.SERVERNAME );
      String servertype = (String) _Config.get( Driver.SERVERTYPE );
      String logfile    = (String) _Config.get( Driver.LOGFILE    );

      if( servername == null || servername.length() == 0 ) throw new SQLException( Messages.get( "error.connection.nohost" ), "08001" );

      // this may be the only way to initialize the logging subsystem
      // with some containers such as JBOSS.
      if( getLogWriter() == null && logfile != null && logfile.length() > 0 )
      {
         // try to initialize a PrintWriter
         try
         {
            setLogWriter( new PrintWriter( new FileOutputStream( logfile ), true ) );
         }
         catch( IOException e )
         {
            System.err.println( "jTDS: Failed to set log file " + e );
         }
      }

      Properties props = new Properties();
      addNonNullProperties( props, user, password );

      String url;
      try
      {
         // Determine the server type (for the URL stub) or use the default
         int serverTypeDef = (servertype == null) ? 0 : Integer.parseInt( servertype );
         url = "jdbc:jtds:" + DefaultProperties.getServerTypeWithDefault( serverTypeDef ) + ':';
      }
      catch( RuntimeException ex )
      {
         SQLException sqlException = new SQLException( Messages.get( "error.connection.servertype", ex.toString() ), "08001" );
         Support.linkException( sqlException, ex );
         throw sqlException;
      }

      // Connect with the URL stub and set properties. The defaults will be
      // filled in by connect().
      return _Driver.connect( url, props );
   }

   public Reference getReference()
   {
      Reference ref = new Reference( getClass().getName(), JtdsObjectFactory.class.getName(), null );

      Iterator it = _Config.entrySet().iterator();

      while( it.hasNext() )
      {
         Entry e = (Entry) it.next();

         String key = (String) e.getKey();
         String val = (String) e.getValue();

         ref.add( new StringRefAddr( key, val ) );
      }

      return ref;
   }

   //
   // ConnectionPoolDataSource methods
   //

   /**
    * Returns a new pooled database connection.
    *
    * @return a new pooled database connection
    * @throws SQLException
    * if an error occurs
    */
   public javax.sql.PooledConnection getPooledConnection()
      throws SQLException
   {
      return getPooledConnection( (String) _Config.get( Driver.USER ), (String) _Config.get( Driver.PASSWORD ) );
   }

   /**
    * Returns a new pooled database connection for the user and password
    * specified.
    *
    * @param user
    * the user name to connect with
    * @param password
    * the password to connect with
    * @return a new pooled database connection
    * @throws SQLException
    * if an error occurs
    */
   public synchronized javax.sql.PooledConnection getPooledConnection( String user, String password )
      throws SQLException
   {
      return new net.sourceforge.jtds.jdbcx.PooledConnection( getConnection( user, password ) );
   }

   //
   // Getters and setters
   //

   public void setLogWriter( PrintWriter out )
   {
      Logger.setLogWriter( out );
   }

   public PrintWriter getLogWriter()
   {
      return Logger.getLogWriter();
   }

   public void setLoginTimeout( int loginTimeout )
   {
      _Config.put( Driver.LOGINTIMEOUT, String.valueOf( loginTimeout ) );
   }

   public int getLoginTimeout()
   {
      return getIntProperty( Driver.LOGINTIMEOUT );
   }

   public void setSocketTimeout( int socketTimeout )
   {
      _Config.put( Driver.SOTIMEOUT, String.valueOf( socketTimeout ) );
   }

   public int getSocketTimeout()
   {
      return getIntProperty( Driver.SOTIMEOUT );
   }

   public void setSocketKeepAlive( boolean socketKeepAlive )
   {
      _Config.put( Driver.SOKEEPALIVE, String.valueOf( socketKeepAlive ) );
   }

   public boolean getSocketKeepAlive()
   {
      return Boolean.valueOf( (String) _Config.get( Driver.SOKEEPALIVE ) ).booleanValue();
   }

   public void setProcessId( String processId )
   {
      _Config.put( Driver.PROCESSID, processId );
   }

   public String getProcessId()
   {
      return (String) _Config.get( Driver.PROCESSID );
   }

   public void setDatabaseName( String databaseName )
   {
      _Config.put( Driver.DATABASENAME, databaseName );
   }

   public String getDatabaseName()
   {
      return (String) _Config.get( Driver.DATABASENAME );
   }

   public void setDescription( String description )
   {
      _Config.put( DESCRIPTION, description );
   }

   public String getDescription()
   {
      return (String) _Config.get( DESCRIPTION );
   }

   public void setPassword( String password )
   {
      _Config.put( Driver.PASSWORD, password );
   }

   public String getPassword()
   {
      return (String) _Config.get( Driver.PASSWORD );
   }

   public void setPortNumber( int portNumber )
   {
      _Config.put( Driver.PORTNUMBER, String.valueOf( portNumber ) );
   }

   public int getPortNumber()
   {
      return getIntProperty( Driver.PORTNUMBER );
   }

   public void setServerName( String serverName )
   {
      _Config.put( Driver.SERVERNAME, serverName );
   }

   public String getServerName()
   {
      return (String) _Config.get( Driver.SERVERNAME );
   }

   public void setAutoCommit( boolean autoCommit )
   {
      _Config.put( Driver.AUTOCOMMIT, String.valueOf( autoCommit ) );
   }

   public boolean getAutoCommit()
   {
      return Boolean.valueOf( (String) _Config.get( Driver.AUTOCOMMIT ) ).booleanValue();
   }

   public void setUser( String user )
   {
      _Config.put( Driver.USER, user );
   }

   public String getUser()
   {
      return (String) _Config.get( Driver.USER );
   }

   public void setTds( String tds )
   {
      _Config.put( Driver.TDS, tds );
   }

   public String getTds()
   {
      return (String) _Config.get( Driver.TDS );
   }

   public void setServerType( int serverType )
   {
      _Config.put( Driver.SERVERTYPE, String.valueOf( serverType ) );
   }

   public int getServerType()
   {
      return getIntProperty( Driver.SERVERTYPE );
   }

   public void setDomain( String domain )
   {
      _Config.put( Driver.DOMAIN, domain );
   }

   public String getDomain()
   {
      return (String) _Config.get( Driver.DOMAIN );
   }

   public void setUseNTLMV2( boolean usentlmv2 )
   {
      _Config.put( Driver.USENTLMV2, String.valueOf( usentlmv2 ) );
   }

   public boolean getUseNTLMV2()
   {
      return Boolean.valueOf( (String) _Config.get( Driver.USENTLMV2 ) ).booleanValue();
   }

   public void setInstance( String instance )
   {
      _Config.put( Driver.INSTANCE, instance );
   }

   public String getInstance()
   {
      return (String) _Config.get( Driver.INSTANCE );
   }

   public void setSendStringParametersAsUnicode( boolean sendStringParametersAsUnicode )
   {
      _Config.put( Driver.SENDSTRINGPARAMETERSASUNICODE, String.valueOf( sendStringParametersAsUnicode ) );
   }

   public boolean getSendStringParametersAsUnicode()
   {
      return Boolean.valueOf( (String) _Config.get( Driver.SENDSTRINGPARAMETERSASUNICODE ) ).booleanValue();
   }

   public void setNamedPipe( boolean namedPipe )
   {
      _Config.put( Driver.NAMEDPIPE, String.valueOf( namedPipe ) );
   }

   public boolean getNamedPipe()
   {
      return Boolean.valueOf( (String) _Config.get( Driver.NAMEDPIPE ) ).booleanValue();
   }

   public void setLastUpdateCount( boolean lastUpdateCount )
   {
      _Config.put( Driver.LASTUPDATECOUNT, String.valueOf( lastUpdateCount ) );
   }

   public boolean getLastUpdateCount()
   {
      return Boolean.valueOf( (String) _Config.get( Driver.LASTUPDATECOUNT ) ).booleanValue();
   }

   public void setXaEmulation( boolean xaEmulation )
   {
      _Config.put( Driver.XAEMULATION, String.valueOf( xaEmulation ) );
   }

   public boolean getXaEmulation()
   {
      return Boolean.valueOf( (String) _Config.get( Driver.XAEMULATION ) ).booleanValue();
   }

   public void setCharset( String charset )
   {
      _Config.put( Driver.CHARSET, charset );
   }

   public String getCharset()
   {
      return (String) _Config.get( Driver.CHARSET );
   }

   public void setLanguage( String language )
   {
      _Config.put( Driver.LANGUAGE, language );
   }

   public String getLanguage()
   {
      return (String) _Config.get( Driver.LANGUAGE );
   }

   public void setMacAddress( String macAddress )
   {
      _Config.put( Driver.MACADDRESS, macAddress );
   }

   public String getMacAddress()
   {
      return (String) _Config.get( Driver.MACADDRESS );
   }

   public void setPacketSize( int packetSize )
   {
      _Config.put( Driver.PACKETSIZE, String.valueOf( packetSize ) );
   }

   public int getPacketSize()
   {
      return getIntProperty( Driver.PACKETSIZE );
   }

   public void setTcpNoDelay( boolean tcpNoDelay )
   {
      _Config.put( Driver.TCPNODELAY, String.valueOf( tcpNoDelay ) );
   }

   public boolean getTcpNoDelay()
   {
      return Boolean.valueOf( (String) _Config.get( Driver.TCPNODELAY ) ).booleanValue();
   }

   public void setPrepareSql( int prepareSql )
   {
      _Config.put( Driver.PREPARESQL, String.valueOf( prepareSql ) );
   }

   public int getPrepareSql()
   {
      return getIntProperty( Driver.PREPARESQL );
   }

   public void setLobBuffer( long lobBuffer )
   {
      _Config.put( Driver.LOBBUFFER, String.valueOf( lobBuffer ) );
   }

   public long getLobBuffer()
   {
      return getLongProperty( Driver.LOBBUFFER );
   }

   public void setMaxStatements( int maxStatements )
   {
      _Config.put( Driver.MAXSTATEMENTS, String.valueOf( maxStatements ) );
   }

   public int getMaxStatements()
   {
      return getIntProperty( Driver.MAXSTATEMENTS );
   }

   public void setAppName( String appName )
   {
      _Config.put( Driver.APPNAME, appName );
   }

   public String getAppName()
   {
      return (String) _Config.get( Driver.APPNAME );
   }

   public void setProgName( String progName )
   {
      _Config.put( Driver.PROGNAME, progName );
   }

   public String getProgName()
   {
      return (String) _Config.get( Driver.PROGNAME );
   }

   public void setWsid( String wsid )
   {
      _Config.put( Driver.WSID, wsid );
   }

   public String getWsid()
   {
      return (String) _Config.get( Driver.WSID );
   }

   public void setLogFile( String logFile )
   {
      _Config.put( Driver.LOGFILE, logFile );
   }

   public String getLogFile()
   {
      return (String) _Config.get( Driver.LOGFILE );
   }

   public void setSsl( String ssl )
   {
      _Config.put( Driver.SSL, ssl );
   }

   public String getSsl()
   {
      return (String) _Config.get( Driver.SSL );
   }

   public void setBatchSize( int batchSize )
   {
      _Config.put( Driver.BATCHSIZE, String.valueOf( batchSize ) );
   }

   public int getBatchSize()
   {
      return getIntProperty( Driver.BATCHSIZE );
   }

   public void setBufferDir( String bufferDir )
   {
      _Config.put( Driver.BUFFERDIR, bufferDir );
   }

   public String getBufferDir()
   {
      return (String) _Config.get( Driver.BUFFERDIR );
   }

   public int getBufferMaxMemory()
   {
      return getIntProperty( Driver.BUFFERMAXMEMORY );
   }

   public void setBufferMaxMemory( int bufferMaxMemory )
   {
      _Config.put( Driver.BUFFERMAXMEMORY, String.valueOf( bufferMaxMemory ) );
   }

   public void setBufferMinPackets( int bufferMinPackets )
   {
      _Config.put( Driver.BUFFERMINPACKETS, String.valueOf( bufferMinPackets ) );
   }

   public int getBufferMinPackets()
   {
      return getIntProperty( Driver.BUFFERMINPACKETS );
   }

   public void setCacheMetaData( boolean cacheMetaData )
   {
      _Config.put( Driver.CACHEMETA, String.valueOf( cacheMetaData ) );
   }

   public boolean getCacheMetaData()
   {
      return Boolean.valueOf( (String) _Config.get( Driver.CACHEMETA ) ).booleanValue();
   }

   public void setUseCursors( boolean useCursors )
   {
      _Config.put( Driver.USECURSORS, String.valueOf( useCursors ) );
   }

   public boolean getUseCursors()
   {
      return Boolean.valueOf( (String) _Config.get( Driver.USECURSORS ) ).booleanValue();
   }

   public void setUseLOBs( boolean useLOBs )
   {
      _Config.put( Driver.USELOBS, String.valueOf( useLOBs ) );
   }

   public boolean getUseLOBs()
   {
      return Boolean.valueOf( (String) _Config.get( Driver.USELOBS ) ).booleanValue();
   }

   public void setBindAddress( String bindAddress )
   {
      _Config.put( Driver.BINDADDRESS, bindAddress );
   }

   public String getBindAddress()
   {
      return (String) _Config.get( Driver.BINDADDRESS );
   }

   public void setUseJCIFS( boolean useJCIFS )
   {
      _Config.put( Driver.USEJCIFS, String.valueOf( useJCIFS ) );
   }

   public boolean getUseJCIFS()
   {
      return Boolean.valueOf( (String) _Config.get( Driver.USEJCIFS ) ).booleanValue();
   }

   private void addNonNullProperties( Properties props, String user, String password )
   {
      Iterator it = _Config.entrySet().iterator();

      while( it.hasNext() )
      {
         Entry e = (Entry) it.next();

         String key = (String) e.getKey();
         String val = (String) e.getValue();

         if( ! key.equals( DESCRIPTION ) && val != null )
         {
            props.setProperty( Messages.get( key ), val );
         }
      }

      if( user != null )
      {
         props.setProperty( Messages.get( Driver.USER ), user );
      }

      if( password != null )
      {
         props.setProperty( Messages.get( Driver.PASSWORD ), password );
      }
   }

   private int getIntProperty( String key )
   {
      return Long.valueOf( getLongProperty( key ) ).intValue();
   }

   private long getLongProperty( String key )
   {
      String val = (String) _Config.get( key );

      if( val == null )
      {
         return 0;
      }

      return Long.parseLong( val );
   }

   /////// JDBC4 demarcation, do NOT put any JDBC3 code below this line ///////

   /*
    * (non-Javadoc)
    * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
    */
   public boolean isWrapperFor( Class arg0 )
   {
      // TODO Auto-generated method stub
      throw new AbstractMethodError();
   }

   /*
    * (non-Javadoc)
    * @see java.sql.Wrapper#unwrap(java.lang.Class)
    */
   public Object unwrap( Class arg0 )
   {
      // TODO Auto-generated method stub
      throw new AbstractMethodError();
   }

   // // JDBC4.1 demarcation, do NOT put any JDBC3/4.0 code below this line ////

   @Override
   public java.util.logging.Logger getParentLogger() throws java.sql.SQLFeatureNotSupportedException
   {
      // TODO Auto-generated method stub
      throw new AbstractMethodError();
   }

}