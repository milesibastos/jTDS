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
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

package net.sourceforge.jtds.jdbcx;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import javax.naming.Context;
import javax.naming.Name;
import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;

import net.sourceforge.jtds.jdbc.Driver;
import net.sourceforge.jtds.jdbc.Messages;

/**
 * Description
 *
 * @author
 *    Alin Sinplean, Holger Rehn
 *
 * @since
 *    0.3
 */
public class JtdsObjectFactory implements ObjectFactory
{

   /**
    *
    */
   public Object getObjectInstance( Object refObj, Name name, Context nameCtx, Hashtable env )
      throws Exception
   {
      Reference ref = (Reference) refObj;

      if( ref.getClassName().equals( JtdsDataSource.class.getName() ) )
      {
         HashMap props = loadProps( ref, new String[]
         {
            JtdsDataSource.DESCRIPTION,

            Driver.APPNAME,
            Driver.AUTOCOMMIT,
            Driver.BATCHSIZE,
            Driver.BINDADDRESS,
            Driver.BUFFERDIR,
            Driver.BUFFERMAXMEMORY,
            Driver.BUFFERMINPACKETS,
            Driver.CACHEMETA,
            Driver.CHARSET,
            Driver.DATABASENAME,
            Driver.DOMAIN,
            Driver.INSTANCE,
            Driver.LANGUAGE,
            Driver.LASTUPDATECOUNT,
            Driver.LOBBUFFER,
            Driver.LOGFILE,
            Driver.LOGINTIMEOUT,
            Driver.MACADDRESS,
            Driver.MAXSTATEMENTS,
            Driver.NAMEDPIPE,
            Driver.PACKETSIZE,
            Driver.PASSWORD,
            Driver.PORTNUMBER,
            Driver.PREPARESQL,
            Driver.PROGNAME,
            Driver.SERVERNAME,
            Driver.SERVERTYPE,
            Driver.SOTIMEOUT,
            Driver.SOKEEPALIVE,
            Driver.PROCESSID,
            Driver.SSL,
            Driver.TCPNODELAY,
            Driver.TDS,
            Driver.USECURSORS,
            Driver.USEJCIFS,
            Driver.USENTLMV2,
            Driver.USEKERBEROS,
            Driver.USELOBS,
            Driver.USER,
            Driver.SENDSTRINGPARAMETERSASUNICODE,
            Driver.WSID,
            Driver.XAEMULATION
         } );

         return new JtdsDataSource( props );
      }

      return null;
   }

   private HashMap loadProps( Reference ref, String[] props )
   {
      HashMap config = new HashMap();
      HashMap values = new HashMap();

      Enumeration c = ref.getAll();

      while( c.hasMoreElements() )
      {
         RefAddr ra = (RefAddr) c.nextElement();
         values.put( ra.getType().toLowerCase(), ra.getContent() );
      }

      for( int i = 0; i < props.length; i ++ )
      {
         String value = (String) values.get( props[i].toLowerCase() );
         value = value == null ? (String) values.get( Messages.get( props[i].toLowerCase() ) ) : value;

         if( value != null )
         {
            config.put( props[i], value );
         }
      }

      return config;
   }

}