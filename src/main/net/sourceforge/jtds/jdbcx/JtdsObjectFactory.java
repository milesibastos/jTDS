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
//
package net.sourceforge.jtds.jdbcx;

import java.util.Hashtable;
import javax.naming.*;
import javax.naming.spi.*;
import net.sourceforge.jtds.jdbc.Messages;
import net.sourceforge.jtds.jdbc.Driver;

/**
 * Description
 *
 * @author Alin Sinplean
 * @since 0.3
 * @version $Id: JtdsObjectFactory.java,v 1.13 2005-01-14 06:02:24 alin_sinpalean Exp $
 */
public class JtdsObjectFactory implements ObjectFactory {
    public Object getObjectInstance(Object refObj,
                                    Name name,
                                    Context nameCtx,
                                    Hashtable env)
    throws Exception {
        Reference ref = (Reference) refObj;

        if (ref.getClassName().equals(JtdsDataSource.class.getName())) {
            JtdsDataSource ds = new JtdsDataSource();

            ds.setServerName((String) ref.get(Messages.get(Driver.SERVERNAME)).getContent());
            ds.setPortNumber(Integer.parseInt((String) ref.get(Messages.get(Driver.PORTNUMBER)).getContent()));
            ds.setDatabaseName((String) ref.get(Messages.get(Driver.DATABASENAME)).getContent());
            ds.setUser((String) ref.get(Messages.get(Driver.USER)).getContent());
            ds.setPassword((String) ref.get(Messages.get(Driver.PASSWORD)).getContent());
            ds.setCharset((String) ref.get(Messages.get(Driver.CHARSET)).getContent());
            ds.setLanguage((String) ref.get(Messages.get(Driver.LANGUAGE)).getContent());
            ds.setTds((String) ref.get(Messages.get(Driver.TDS)).getContent());
            ds.setServerType(Integer.parseInt((String) ref.get(Messages.get(Driver.SERVERTYPE)).getContent()));
            ds.setDomain((String) ref.get(Messages.get(Driver.DOMAIN)).getContent());
            ds.setInstance((String) ref.get(Messages.get(Driver.INSTANCE)).getContent());
            ds.setLastUpdateCount("true".equals(ref.get(Messages.get(Driver.LASTUPDATECOUNT)).getContent()));
            ds.setSendStringParametersAsUnicode("true".equals(
                    ref.get(Messages.get(Driver.SENDSTRINGPARAMETERSASUNICODE)).getContent()));
            ds.setNamedPipe("true".equals(ref.get(Messages.get(Driver.NAMEDPIPE)).getContent()));
            ds.setMacAddress((String) ref.get(Messages.get(Driver.MACADDRESS)).getContent());
            ds.setMaxStatements(Integer.parseInt((String) ref.get(Messages.get(Driver.MAXSTATEMENTS)).getContent()));
            ds.setPacketSize(Integer.parseInt((String) ref.get(Messages.get(Driver.PACKETSIZE)).getContent()));
            ds.setPrepareSql(Integer.parseInt((String) ref.get(Messages.get(Driver.PREPARESQL)).getContent()));
            ds.setLobBuffer(Long.parseLong((String) ref.get(Messages.get(Driver.LOBBUFFER)).getContent()));
            ds.setLoginTimeout(Integer.parseInt((String) ref.get(Messages.get(Driver.LOGINTIMEOUT)).getContent()));
            ds.setAppName((String) ref.get(Messages.get(Driver.APPNAME)).getContent());
            ds.setProgName((String) ref.get(Messages.get(Driver.PROGNAME)).getContent());
            ds.setTcpNoDelay("true".equals(ref.get(Messages.get(Driver.TCPNODELAY)).getContent()));
            ds.setXaEmulation("true".equals(ref.get(Messages.get(Driver.XAEMULATION)).getContent()));
            ds.setLogFile((String) ref.get(Messages.get(Driver.LOGFILE)).getContent());
            ds.setSsl((String) ref.get(Messages.get(Driver.SSL)).getContent());

            return ds;
        }

        return null;
    }
}
