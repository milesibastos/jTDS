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
 * <li>Set the {@link #tester} field in a <code>public</code> default
 *     constructor that takes no arguments.</li>
 * <li>A <code>public static Test suite()</code> method that takes one or more
 *     arguments.  (The {@link #suite()} method in this class should
 *     <em>not</em> be overridden.)</li>
 * </ol>
 *
 * @author David D. Kilzer
 * @version $Id: DefaultPropertiesTestLibrary.java,v 1.14 2005-03-18 11:46:53 alin_sinpalean Exp $
 */
public abstract class DefaultPropertiesTestLibrary extends TestCase {

    /** Test JDBC URL for SQL Server. */
    private static final String URL_SQLSERVER =
            "jdbc:jtds:" + DefaultProperties.SERVER_TYPE_SQLSERVER + "://servername";
    /** Test JDBC URL for Sybase. */
    private static final String URL_SYBASE =
            "jdbc:jtds:" + DefaultProperties.SERVER_TYPE_SYBASE + "://servername";

    /** Object used to run all of the tests. */
    private DefaultPropertiesTester tester;
    /** If true, only run tests for SQL Server, not Sybase. */
    private boolean onlySqlServerTests = false;
    /** If true, only run tests for TDS 7.0. */
    private boolean onlyTds70Tests = false;


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
     * The extender of this class is required to set the {@link #tester}
     * field in a <code>public</code> default constructor.
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
        String messageKey = Driver.SERVERTYPE;
        assertDefaultPropertyByServerType(URL_SQLSERVER, messageKey, fieldName, String.valueOf(Driver.SQLSERVER));
        if (!isOnlySqlServerTests()) {
            assertDefaultPropertyByServerType(URL_SYBASE, messageKey, fieldName, String.valueOf(Driver.SYBASE));
        }
    }


    /**
     * Test the <code>tds</code> (version) property.
     */
    public void test_tds() {
        String fieldName = "tdsVersion";
        String messageKey = Driver.TDS;
        assertDefaultPropertyByServerType(URL_SQLSERVER, messageKey, fieldName, DefaultProperties.TDS_VERSION_80);
        if (!isOnlySqlServerTests()) {
            assertDefaultPropertyByServerType(URL_SYBASE, messageKey, fieldName, DefaultProperties.TDS_VERSION_50);
        }
    }


    /**
     * Test the <code>portNumber</code> property.
     * <p/>
     * Different values are set depending on whether SQL Server or
     * Sybase is used.
     */
    public void test_portNumber() {
        String fieldName = "portNumber";
        String messageKey = Driver.PORTNUMBER;
        assertDefaultPropertyByServerType(
                URL_SQLSERVER, messageKey, fieldName, String.valueOf(DefaultProperties.PORT_NUMBER_SQLSERVER));
        if (!isOnlySqlServerTests()) {
            assertDefaultPropertyByServerType(
                    URL_SYBASE, messageKey, fieldName, String.valueOf(DefaultProperties.PORT_NUMBER_SYBASE));
        }
    }


    /**
     * Test the <code>databaseName</code> property.
     */
    public void test_databaseName() {
        String fieldName = "databaseName";
        String messageKey = Driver.DATABASENAME;
        String expectedValue = DefaultProperties.DATABASE_NAME;
        assertDefaultPropertyByServerType(URL_SQLSERVER, messageKey, fieldName, expectedValue);
        if (!isOnlySqlServerTests()) {
            assertDefaultPropertyByServerType(URL_SYBASE, messageKey, fieldName, expectedValue);
        }
    }


    /**
     * Test the <code>appName</code> property.
     */
    public void test_appName() {
        String fieldName = "appName";
        String messageKey = Driver.APPNAME;
        String expectedValue = DefaultProperties.APP_NAME;
        assertDefaultPropertyByServerType(URL_SQLSERVER, messageKey, fieldName, expectedValue);
        if (!isOnlySqlServerTests()) {
            assertDefaultPropertyByServerType(URL_SYBASE, messageKey, fieldName, expectedValue);
        }
    }


    /**
     * Test the <code>lastUpdateCount</code> property.
     */
    public void test_lastUpdateCount() {
        String fieldName = "lastUpdateCount";
        String messageKey = Driver.LASTUPDATECOUNT;
        String expectedValue = String.valueOf(DefaultProperties.LAST_UPDATE_COUNT);
        assertDefaultPropertyByServerType(URL_SQLSERVER, messageKey, fieldName, expectedValue);
        if (!isOnlySqlServerTests()) {
            assertDefaultPropertyByServerType(URL_SYBASE, messageKey, fieldName, expectedValue);
        }
    }


    /**
     * Test the <code>lobBuffer</code> property.
     */
    public void test_lobBuffer() {
        String fieldName = "lobBuffer";
        String messageKey = Driver.LOBBUFFER;
        String expectedValue = String.valueOf(DefaultProperties.LOB_BUFFER_SIZE);
        assertDefaultPropertyByServerType(URL_SQLSERVER, messageKey, fieldName, expectedValue);
        if (!isOnlySqlServerTests()) {
            assertDefaultPropertyByServerType(URL_SYBASE, messageKey, fieldName, expectedValue);
        }
    }


    /**
     * Test the <code>loginTimeout</code> property.
     */
    public void test_loginTimeout() {
        String fieldName = "loginTimeout";
        String messageKey = Driver.LOGINTIMEOUT;
        String expectedValue = String.valueOf(DefaultProperties.LOGIN_TIMEOUT);
        assertDefaultPropertyByServerType(URL_SQLSERVER, messageKey, fieldName, expectedValue);
        if (!isOnlySqlServerTests()) {
            assertDefaultPropertyByServerType(URL_SYBASE, messageKey, fieldName, expectedValue);
        }
    }


    /**
     * Test the <code>macAddress</code> property.
     */

    public void test_macAddress() {
        String fieldName = "macAddress";
        String messageKey = Driver.MACADDRESS;
        String expectedValue = DefaultProperties.MAC_ADDRESS;
        assertDefaultPropertyByServerType(URL_SQLSERVER, messageKey, fieldName, expectedValue);
        if (!isOnlySqlServerTests()) {
            assertDefaultPropertyByServerType(URL_SYBASE, messageKey, fieldName, expectedValue);
        }
    }


    /**
     * Test the <code>namedPipe</code> property.
     */
    public void test_namedPipe() {
        String fieldName = "namedPipe";
        String messageKey = Driver.NAMEDPIPE;
        String expectedValue = String.valueOf(DefaultProperties.NAMED_PIPE);
        assertDefaultPropertyByServerType(URL_SQLSERVER, messageKey, fieldName, expectedValue);
        if (!isOnlySqlServerTests()) {
            assertDefaultPropertyByServerType(URL_SYBASE, messageKey, fieldName, expectedValue);
        }
    }


    /**
     * Test the <code>packetSize</code> property.
     */
    public void test_packetSize() {

        String fieldName = "packetSize";
        String messageKey = Driver.PACKETSIZE;

        if (!isOnlyTds70Tests()) {
            String expectedValue = String.valueOf(DefaultProperties.PACKET_SIZE_42_50);
            assertDefaultPropertyByTdsVersion(
                    URL_SQLSERVER, DefaultProperties.TDS_VERSION_42, messageKey, fieldName, expectedValue);
            assertDefaultPropertyByTdsVersion(
                    URL_SYBASE, DefaultProperties.TDS_VERSION_50, messageKey, fieldName, expectedValue);
        }

        String expectedValue = String.valueOf(DefaultProperties.PACKET_SIZE_70_80);
        assertDefaultPropertyByTdsVersion(
                URL_SQLSERVER, DefaultProperties.TDS_VERSION_70, messageKey, fieldName, expectedValue);
        if (!isOnlyTds70Tests()) {
            assertDefaultPropertyByTdsVersion(
                    URL_SQLSERVER, DefaultProperties.TDS_VERSION_80, messageKey, fieldName, expectedValue);
        }
    }


    /**
     * Test the <code>prepareSql</code> property.
     */
    public void test_prepareSql() {
        String fieldName = "prepareSql";
        String messageKey = Driver.PREPARESQL;
        String expectedValue = String.valueOf(DefaultProperties.PREPARE_SQL);
        assertDefaultPropertyByServerType(URL_SQLSERVER, messageKey, fieldName, expectedValue);
        if (!isOnlySqlServerTests()) {
            assertDefaultPropertyByServerType(URL_SYBASE, messageKey, fieldName, expectedValue);
        }
    }


    /**
     * Test the <code>progName</code> property.
     */
    public void test_progName() {
        String fieldName = "progName";
        String messageKey = Driver.PROGNAME;
        String expectedValue = DefaultProperties.PROG_NAME;
        assertDefaultPropertyByServerType(URL_SQLSERVER, messageKey, fieldName, expectedValue);
        if (!isOnlySqlServerTests()) {
            assertDefaultPropertyByServerType(URL_SYBASE, messageKey, fieldName, expectedValue);
        }
    }


    /**
     * Test the <code>sendStringParametersAsUnicode</code> property.
     */
    public void test_sendStringParametersAsUnicode() {
        String fieldName = "sendStringParametersAsUnicode";
        String messageKey = Driver.SENDSTRINGPARAMETERSASUNICODE;
        String expectedValue = String.valueOf(DefaultProperties.USE_UNICODE);
        assertDefaultPropertyByServerType(URL_SQLSERVER, messageKey, fieldName, expectedValue);
        if (!isOnlySqlServerTests()) {
            assertDefaultPropertyByServerType(URL_SYBASE, messageKey, fieldName, expectedValue);
        }
    }


    /**
     * Test the <code>tcpNoDelay</code> property.
     */
    public void test_tcpNoDelay() {
        String fieldName = "tcpNoDelay";
        String messageKey = Driver.TCPNODELAY;
        String expectedValue = String.valueOf(DefaultProperties.TCP_NODELAY);
        assertDefaultPropertyByServerType(URL_SQLSERVER, messageKey, fieldName, expectedValue);
        if (!isOnlySqlServerTests()) {
            assertDefaultPropertyByServerType(URL_SYBASE, messageKey, fieldName, expectedValue);
        }
    }


    /**
     * Test the <code>wsid</code> property.
     */
    public void test_wsid() {
        String fieldName = "wsid";
        String messageKey = Driver.WSID;
        String expectedValue = DefaultProperties.WSID;
        assertDefaultPropertyByServerType(URL_SQLSERVER, messageKey, fieldName, expectedValue);
        if (!isOnlySqlServerTests()) {
            assertDefaultPropertyByServerType(URL_SYBASE, messageKey, fieldName, expectedValue);
        }
    }


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
        properties.setProperty(Messages.get(Driver.TDS), tdsVersion);
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


    /**
     * Getter for {@link #tester}.
     *
     * @return Value of {@link #tester}.
     */
    protected DefaultPropertiesTester getTester() {
        return tester;
    }


    /**
     * Setter for {@link #tester}.
     *
     * @param tester The value to set {@link #tester} to.
     */
    public void setTester(DefaultPropertiesTester tester) {
        this.tester = tester;
    }


    /**
     * Getter for {@link #onlySqlServerTests}.
     *
     * @return Value of {@link #onlySqlServerTests}.
     */
    public boolean isOnlySqlServerTests() {
        return onlySqlServerTests;
    }


    /**
     * Setter for {@link #onlySqlServerTests}.
     *
     * @param onlySqlServerTests The value to set {@link #onlySqlServerTests} to.
     */
    protected void setOnlySqlServerTests(boolean onlySqlServerTests) {
        this.onlySqlServerTests = onlySqlServerTests;
    }


    /**
     * Getter for {@link #onlyTds70Tests}.
     *
     * @return Value of {@link #onlyTds70Tests}.
     */
    public boolean isOnlyTds70Tests() {
        return onlyTds70Tests;
    }


    /**
     * Setter for {@link #onlyTds70Tests}.
     *
     * @param onlyTds70Tests The value to set {@link #onlyTds70Tests} to.
     */
    protected void setOnlyTds70Tests(boolean onlyTds70Tests) {
        this.onlyTds70Tests = onlyTds70Tests;
    }


    /**
     * Changes the first character of a string to uppercase.
     *
     * @param s The string to be processed.
     * @return The value of <code>s</code> if it is <code>null</code> or zero length,
     *         else the string with the first character changed to uppercase.
     */
    protected String ucFirst(String s) {
        if (s == null || s.length() == 0) return s;
        if (s.length() == 1) {
            return s.toUpperCase();
        }
        return s.substring(0, 1).toUpperCase() + s.substring(1);
    }

}
