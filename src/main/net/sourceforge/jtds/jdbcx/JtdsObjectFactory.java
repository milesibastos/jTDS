// jTDS JDBC Driver for Microsoft SQL Server
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
import net.sourceforge.jtds.jdbc.Support;

/**
 * Description
 *
 * @author Alin Sinplean
 * @since 0.3
 * @version $Id: JtdsObjectFactory.java,v 1.3 2004-07-25 15:30:26 bheineman Exp $
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

            ds.setServerName((String) ref.get(Support.getMessage("prop.servername")).getContent());
            ds.setPortNumber(Integer.parseInt((String) ref.get(Support.getMessage("prop.portnumber")).getContent()));
            ds.setDatabaseName((String) ref.get(Support.getMessage("prop.databasename")).getContent());
            ds.setUser((String) ref.get(Support.getMessage("prop.user")).getContent());
            ds.setPassword((String) ref.get(Support.getMessage("prop.password")).getContent());
            ds.setCharset((String) ref.get(Support.getMessage("prop.charset")).getContent());
            ds.setLanguage((String) ref.get(Support.getMessage("prop.language")).getContent());
            ds.setTds((String) ref.get(Support.getMessage("prop.tds")).getContent());
            ds.setServerType(Integer.parseInt((String) ref.get(Support.getMessage("prop.servertype")).getContent()));
            ds.setDomain((String) ref.get(Support.getMessage("prop.domain")).getContent());
            ds.setInstance((String) ref.get(Support.getMessage("prop.instance")).getContent());
            ds.setLastUpdateCount("true".equals(ref.get(Support.getMessage("prop.lastupdatecount")).getContent()));
            ds.setSendStringParametersAsUnicode("true".equals(ref.get(Support.getMessage("prop.useunicode")).getContent()));
            ds.setNamedPipe("true".equals(ref.get(Support.getMessage("prop.namedpipe")).getContent()));
            ds.setMacAddress((String) ref.get(Support.getMessage("prop.macaddress")).getContent());
            ds.setPacketSize(Integer.parseInt((String) ref.get(Support.getMessage("prop.packetsize")).getContent()));
            ds.setPrepareSql("true".equals(ref.get(Support.getMessage("prop.preparesql")).getContent()));
            ds.setLobBuffer(Long.parseLong((String) ref.get(Support.getMessage("prop.lobbuffer")).getContent()));

            return ds;
        }

        return null;
    }
}
