package net.sourceforge.jtds.test;

import javax.naming.Reference;

import net.sourceforge.jtds.jdbcx.JtdsDataSource;
import net.sourceforge.jtds.jdbcx.JtdsObjectFactory;
import net.sourceforge.jtds.jdbc.Driver;

/**
 * Unit tests for the {@link JtdsObjectFactory} class.
 *
 * @author David D. Kilzer
 * @author Alin Sinpalean
 * @version $Id: JtdsObjectFactoryUnitTest.java,v 1.6 2005-03-04 00:11:11 alin_sinpalean Exp $
 */
public class JtdsObjectFactoryUnitTest extends UnitTestBase {
    /**
     * Constructor.
     *
     * @param name The name of the test.
     */
    public JtdsObjectFactoryUnitTest(String name) {
        super(name);
    }

    /**
     * Tests that the factory can correctly rebuild a DataSource with all
     * properties set.
     */
    public void testAllProperties() throws Exception {
        String serverName = "serverName";
        int serverType = Driver.SYBASE;
        int portNumber = 2197;
        String databaseName = "databaseName";
        String tds = "7.0";
        String charset = "charset";
        String language = "language";
        String domain = "domain";
        String instance = "instance";
        boolean lastUpdateCount = false;
        boolean sendStringParametersAsUnicode = false;
        boolean namedPipe = true;
        String macAddress = "macAddress";
        int prepareSql = 4;
        int packetSize = 2048;
        boolean tcpNoDelay = true;
        String user = "user";
        String password = "password";
        int loginTimeout = 1;
        long lobBuffer = 4096;
        int maxStatements = 12;
        String appName = "appName";
        String progName = "progName";
        boolean xaEmulation = false;
        String logFile = "logFile";
        String ssl = "ssl";
        int batchSize = 123;
        String description = "description";

        JtdsDataSource ds = new JtdsDataSource();
        ds.setServerName(serverName);
        ds.setServerType(serverType);
        ds.setDatabaseName(databaseName);
        ds.setPortNumber(portNumber);
        ds.setTds(tds);
        ds.setCharset(charset);
        ds.setLanguage(language);
        ds.setDomain(domain);
        ds.setInstance(instance);
        ds.setLastUpdateCount(lastUpdateCount);
        ds.setSendStringParametersAsUnicode(sendStringParametersAsUnicode);
        ds.setNamedPipe(namedPipe);
        ds.setMacAddress(macAddress);
        ds.setPrepareSql(prepareSql);
        ds.setPacketSize(packetSize);
        ds.setTcpNoDelay(tcpNoDelay);
        ds.setUser(user);
        ds.setPassword(password);
        ds.setLoginTimeout(loginTimeout);
        ds.setLobBuffer(lobBuffer);
        ds.setMaxStatements(maxStatements);
        ds.setAppName(appName);
        ds.setProgName(progName);
        ds.setXaEmulation(xaEmulation);
        ds.setLogFile(logFile);
        ds.setSsl(ssl);
        ds.setBatchSize(batchSize);
        ds.setDescription(description);

        Reference dsRef = ds.getReference();
        assertEquals("net.sourceforge.jtds.jdbcx.JtdsObjectFactory",
                dsRef.getFactoryClassName());
        assertEquals("net.sourceforge.jtds.jdbcx.JtdsDataSource",
                dsRef.getClassName());

        ds = (JtdsDataSource) new JtdsObjectFactory()
                .getObjectInstance(dsRef, null, null, null);

        assertEquals(serverName, ds.getServerName());
        assertEquals(serverType, ds.getServerType());
        assertEquals(portNumber, ds.getPortNumber());
        assertEquals(databaseName, ds.getDatabaseName());
        assertEquals(portNumber, ds.getPortNumber());
        assertEquals(tds, ds.getTds());
        assertEquals(charset, ds.getCharset());
        assertEquals(language, ds.getLanguage());
        assertEquals(domain, ds.getDomain());
        assertEquals(instance, ds.getInstance());
        assertEquals(lastUpdateCount, ds.getLastUpdateCount());
        assertEquals(sendStringParametersAsUnicode,
                ds.getSendStringParametersAsUnicode());
        assertEquals(namedPipe, ds.getNamedPipe());
        assertEquals(macAddress, ds.getMacAddress());
        assertEquals(prepareSql, ds.getPrepareSql());
        assertEquals(packetSize, ds.getPacketSize());
        assertEquals(tcpNoDelay, ds.getTcpNoDelay());
        assertEquals(user, ds.getUser());
        assertEquals(password, ds.getPassword());
        assertEquals(loginTimeout, ds.getLoginTimeout());
        assertEquals(lobBuffer, ds.getLobBuffer());
        assertEquals(maxStatements, ds.getMaxStatements());
        assertEquals(appName, ds.getAppName());
        assertEquals(progName, ds.getProgName());
        assertEquals(xaEmulation, ds.getXaEmulation());
        assertEquals(logFile, ds.getLogFile());
        assertEquals(ssl, ds.getSsl());
        assertEquals(batchSize, ds.getBatchSize());
        assertEquals(description, ds.getDescription());
    }

    /**
     * Tests that the factory can correctly rebuild a DataSource with no
     * properties set (i.e. all values should be null and no NPE should be
     * thrown).
     */
    public void testNoProperties() throws Exception {
        JtdsDataSource ds = new JtdsDataSource();

        Reference dsRef = ds.getReference();
        assertEquals("net.sourceforge.jtds.jdbcx.JtdsObjectFactory",
                dsRef.getFactoryClassName());
        assertEquals("net.sourceforge.jtds.jdbcx.JtdsDataSource",
                dsRef.getClassName());

        ds = (JtdsDataSource) new JtdsObjectFactory()
                .getObjectInstance(dsRef, null, null, null);

        assertNull(ds.getServerName());
        assertEquals(0, ds.getServerType());
        assertEquals(0, ds.getPortNumber());
        assertNull(ds.getDatabaseName());
        assertNull(ds.getDatabaseName());
        assertEquals(0, ds.getPortNumber());
        assertNull(ds.getTds());
        assertNull(ds.getCharset());
        assertNull(ds.getLanguage());
        assertNull(ds.getDomain());
        assertNull(ds.getInstance());
        assertEquals(false, ds.getLastUpdateCount());
        assertEquals(false, ds.getSendStringParametersAsUnicode());
        assertEquals(false, ds.getNamedPipe());
        assertNull(ds.getMacAddress());
        assertEquals(0, ds.getPrepareSql());
        assertEquals(0, ds.getPacketSize());
        assertEquals(false, ds.getTcpNoDelay());
        assertNull(ds.getUser());
        assertNull(ds.getPassword());
        assertEquals(0, ds.getLoginTimeout());
        assertEquals(0, ds.getLobBuffer());
        assertEquals(0, ds.getMaxStatements());
        assertNull(ds.getAppName());
        assertNull(ds.getProgName());
        assertEquals(false, ds.getXaEmulation());
        assertNull(ds.getLogFile());
        assertNull(ds.getSsl());
        assertEquals(0, ds.getBatchSize());
        assertNull(ds.getDescription());
    }
}
