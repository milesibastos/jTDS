package net.sourceforge.jtds.jdbcx;

import java.util.Hashtable;
import javax.naming.*;
import javax.naming.spi.*;

/**
 * Description
 *
 * @author Alin Sinplean
 * @since 0.3
 */
public class TdsObjectFactory implements ObjectFactory {
    public Object getObjectInstance(Object refObj,
                                    Name name,
                                    Context nameCtx,
                                    Hashtable env)
    throws Exception {
        Reference ref = (Reference) refObj;

        if (ref.getClassName().equals(TdsDataSource.class.getName())) {
            TdsDataSource ds = new TdsDataSource();

            ds.setServerName((String) ref.get("serverName").getContent());
            ds.setPortNumber(Integer.parseInt((String) ref.get("portNumber").getContent()));
            ds.setDatabaseName((String) ref.get("databaseName").getContent());
            ds.setUser((String) ref.get("user").getContent());
            ds.setPassword((String) ref.get("password").getContent());
            ds.setCharset((String) ref.get("charset").getContent());
            ds.setTds((String) ref.get("tds").getContent());
            ds.setServerType(Integer.parseInt((String) ref.get("serverType").getContent()));
            ds.setDomain((String) ref.get("domain").getContent());
            ds.setInstance((String) ref.get("instance").getContent());
            ds.setLastUpdateCount(
                    "true".equals(ref.get("lastUpdateCount").getContent()));
            ds.setSendStringParametersAsUnicode(
                    "true".equals(ref.get("sendStringParametersAsUnicode").getContent()));
            ds.setMacAddress((String) ref.get("macAddress").getContent());

            return ds;
        }

        return null;
    }
}
