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
package net.sourceforge.jtds.test;

import junit.framework.Test;
import junit.framework.TestCase;
import net.sourceforge.jtds.jdbc.DefaultProperties;
import net.sourceforge.jtds.jdbc.Driver;
import net.sourceforge.jtds.jdbc.Messages;
import java.util.Properties;



/**
 * Library for testing default properties.
 * <p/>
 * Uses a {@link DefaultPropertiesTester} object to test different methods
 * in different classes.
 * <p/>
 * To extend this class, the programmer must implement the following items:
 * <ol>
 * <li>A <code>private static DefaultPropertiesTester tester</code> field.</li>
 * <li>The {@link #getTester()} method that returns <code>tester</code>.</li>
 * <li>A <code>public static Test suite()</code> method that takes one or more
 *     arguments and sets the <code>tester</code> field.  (The
 *     <code>suite()</code> method in this class should <em>not</em> be
 *     overridden.)</li>
 * </ol>
 * 
 * @author David D. Kilzer
 * @version $Id: DefaultPropertiesTestLibrary.java,v 1.5 2004-08-05 16:25:27 bheineman Exp $
 */
public abstract class DefaultPropertiesTestLibrary extends TestCase {

    /** Test JDBC URL for SQL Server. */
    private static final String URL_SQLSERVER = "jdbc:jtds:sqlserver://servername";
    /** Test JDBC URL for Sybase. */
    private static final String URL_SYBASE = "jdbc:jtds:sybase://servername";


    /**
     * Provides a null test suite so that JUnit will not try to instantiate
     * this class directly.
     * 
     * @return The test suite (always <code>null</code>).
     */ 
    public static final Test suite() {
        return null;
    }


    /**
     * Default constructor.
     * <p/>
     * Used by <code>suite()</code> method in a subclass to construct a
     * test suite.
     */
    public DefaultPropertiesTestLibrary() {
    }


    /**
     * Test the <code>serverType</code> property.
     * <p/>
     * Different values are set depending on whether SQL Server or
     * Sybase is used.
     */ 
    public void test_serverType() {
        String fieldName = "serverType";
        String messageKey = "prop.servertype";
        assertDefaultPropertyByServerType(URL_SQLSERVER, messageKey, fieldName, String.valueOf(Driver.SQLSERVER));
        assertDefaultPropertyByServerType(URL_SYBASE, messageKey, fieldName, String.valueOf(Driver.SYBASE));
    }


    /**
     * Test the <code>tds</code> (version) property.
     */
    public void test_tds() {
        String fieldName = "tds";
        String messageKey = "prop.tds";
        assertDefaultPropertyByServerType(URL_SQLSERVER, messageKey, fieldName, DefaultProperties.TDS_VERSION_70);
        assertDefaultPropertyByServerType(URL_SYBASE, messageKey, fieldName, DefaultProperties.TDS_VERSION_50);
    }


    /**
     * Test the <code>portNumber</code> property.
     * <p/>
     * Different values are set depending on whether SQL Server or
     * Sybase is used.
     */ 
    public void test_portNumber() {
        String fieldName = "portNumber";
        String messageKey = "prop.portnumber";
        assertDefaultPropertyByServerType(
                URL_SQLSERVER, messageKey, fieldName, String.valueOf(DefaultProperties.PORT_NUMBER_SQLSERVER));
        assertDefaultPropertyByServerType(
                URL_SYBASE, messageKey, fieldName, String.valueOf(DefaultProperties.PORT_NUMBER_SYBASE));
    }


    /**
     * Test the <code>databaseName</code> property.
     */ 
    public void test_databaseName() {
        String fieldName = "databaseName";
        String messageKey = "prop.databasename";
        String expectedValue = DefaultProperties.DATABASE_NAME;
        assertDefaultPropertyByServerType(URL_SQLSERVER, messageKey, fieldName, expectedValue);
        assertDefaultPropertyByServerType(URL_SYBASE, messageKey, fieldName, expectedValue);
    }


    /**
     * Test the <code>appName</code> property.
     */ 
    public void test_appName() {
        String fieldName = "appName";
        String messageKey = "prop.appname";
        String expectedValue = DefaultProperties.APP_NAME;
        assertDefaultPropertyByServerType(URL_SQLSERVER, messageKey, fieldName, expectedValue);
        assertDefaultPropertyByServerType(URL_SYBASE, messageKey, fieldName, expectedValue);
    }


    /**
     * Test the <code>lastUpdateCount</code> property.
     */ 
    public void test_lastUpdateCount() {
        String fieldName = "lastUpdateCount";
        String messageKey = "prop.lastupdatecount";
        String expectedValue = String.valueOf(DefaultProperties.LAST_UPDATE_COUNT);
        assertDefaultPropertyByServerType(URL_SQLSERVER, messageKey, fieldName, expectedValue);
        assertDefaultPropertyByServerType(URL_SYBASE, messageKey, fieldName, expectedValue);
    }


    /**
     * Test the <code>lobBuffer</code> property.
     */ 
    public void test_lobBuffer() {
        String fieldName = "lobBuffer";
        String messageKey = "prop.lobbuffer";
        String expectedValue = String.valueOf(DefaultProperties.LOB_BUFFER_SIZE);
        assertDefaultPropertyByServerType(URL_SQLSERVER, messageKey, fieldName, expectedValue);
        assertDefaultPropertyByServerType(URL_SYBASE, messageKey, fieldName, expectedValue);
    }


    /**
     * Test the <code>loginTimeout</code> property.
     */ 
    public void test_loginTimeout() {
        String fieldName = "loginTimeout";
        String messageKey = "prop.logintimeout";
        String expectedValue = String.valueOf(DefaultProperties.LOGIN_TIMEOUT);
        assertDefaultPropertyByServerType(URL_SQLSERVER, messageKey, fieldName, expectedValue);
        assertDefaultPropertyByServerType(URL_SYBASE, messageKey, fieldName, expectedValue);
    }

    /**
     * Test the <code>macAddress</code> property.
     */ 

    public void test_macAddress() {
        String fieldName = "macAddress";
        String messageKey = "prop.macaddress";
        String expectedValue = DefaultProperties.MAC_ADDRESS;
        assertDefaultPropertyByServerType(URL_SQLSERVER, messageKey, fieldName, expectedValue);
        assertDefaultPropertyByServerType(URL_SYBASE, messageKey, fieldName, expectedValue);
    }


    /**
     * Test the <code>namedPipe</code> property.
     */ 
    public void test_namedPipe() {
        String fieldName = "namedPipe";
        String messageKey = "prop.namedpipe";
        String expectedValue = String.valueOf(DefaultProperties.NAMED_PIPE);
        assertDefaultPropertyByServerType(URL_SQLSERVER, messageKey, fieldName, expectedValue);
        assertDefaultPropertyByServerType(URL_SYBASE, messageKey, fieldName, expectedValue);
    }


    /**
     * Test the <code>packetSize</code> property.
     */ 
    public void test_packetSize() {

        String fieldName = "packetSize";
        String messageKey = "prop.packetsize";

        String expectedValue = String.valueOf(DefaultProperties.PACKET_SIZE_42_50);
        assertDefaultPropertyByTdsVersion(
                URL_SQLSERVER, DefaultProperties.TDS_VERSION_42, messageKey, fieldName, expectedValue);
        assertDefaultPropertyByTdsVersion(URL_SYBASE, DefaultProperties.TDS_VERSION_50, messageKey, fieldName, expectedValue);

        expectedValue = String.valueOf(DefaultProperties.PACKET_SIZE_70_80);
        assertDefaultPropertyByTdsVersion(
                URL_SQLSERVER, DefaultProperties.TDS_VERSION_70, messageKey, fieldName, expectedValue);
        assertDefaultPropertyByTdsVersion(
                URL_SQLSERVER, DefaultProperties.TDS_VERSION_80, messageKey, fieldName, expectedValue);
    }


    /**
     * Test the <code>prepareSql</code> property.
     */ 
    public void test_prepareSql() {
        String fieldName = "prepareSql";
        String messageKey = "prop.preparesql";
        String expectedValue = String.valueOf(DefaultProperties.PREPARE_SQL);
        assertDefaultPropertyByServerType(URL_SQLSERVER, messageKey, fieldName, expectedValue);
        assertDefaultPropertyByServerType(URL_SYBASE, messageKey, fieldName, expectedValue);
    }


    /**
     * Test the <code>progName</code> property.
     */ 
    public void test_progName() {
        String fieldName = "progName";
        String messageKey = "prop.progname";
        String expectedValue = DefaultProperties.PROG_NAME;
        assertDefaultPropertyByServerType(URL_SQLSERVER, messageKey, fieldName, expectedValue);
        assertDefaultPropertyByServerType(URL_SYBASE, messageKey, fieldName, expectedValue);
    }


    /**
     * Test the <code>sendStringParametersAsUnicode</code> property.
     */ 
    public void test_sendStringParametersAsUnicode() {
        String fieldName = "sendStringParametersAsUnicode";
        String messageKey = "prop.useunicode";
        String expectedValue = String.valueOf(DefaultProperties.USE_UNICODE);
        assertDefaultPropertyByServerType(URL_SQLSERVER, messageKey, fieldName, expectedValue);
        assertDefaultPropertyByServerType(URL_SYBASE, messageKey, fieldName, expectedValue);
    }


    /**
     * Get the <code>tester</code> object.
     * 
     * @return The <code>tester</code> object to be used in the tests.
     */ 
    protected abstract DefaultPropertiesTester getTester();


    /**
     * Assert that the <code>expected</code> property value is set using
     * a given <code>url</code> and <code>tdsVersion</code> property.
     * 
     * @param url The JDBC URL.
     * @param tdsVersion The TDS version.
     * @param key The message key.
     * @param fieldName The field name used in the class.
     * @param expected The expected value of the property.
     */ 
    private void assertDefaultPropertyByTdsVersion(
            String url, String tdsVersion, String key, String fieldName, String expected) {

        Properties properties = new Properties();
        properties.setProperty(Messages.get("prop.tds"), tdsVersion);
        getTester().assertDefaultProperty("Default property incorrect", url, properties, fieldName, key, expected);
    }


    /**
     * Assert that the <code>expected</code> property value is set using
     * a given <code>url</code>.
     * 
     * @param url The JDBC URL.
     * @param key The message key.
     * @param fieldName The field name used in the class.
     * @param expected The expected value of the property.
     */ 
    private void assertDefaultPropertyByServerType(String url, String key, String fieldName, String expected) {
        getTester().assertDefaultProperty("Default property incorrect", url, new Properties(), fieldName, key, expected);
    }

}
