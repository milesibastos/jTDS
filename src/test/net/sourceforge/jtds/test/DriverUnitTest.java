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
import junit.framework.TestSuite;
import net.sourceforge.jtds.jdbc.Driver;
import net.sourceforge.jtds.jdbc.Support;
import net.sourceforge.jtds.jdbc.Messages;
import java.sql.SQLException;
import java.sql.DriverPropertyInfo;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.Enumeration;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;



/**
 * Unit tests for the {@link net.sourceforge.jtds.jdbc.Driver} class.
 * 
 * @author David D. Kilzer
 * @version $Id: DriverUnitTest.java,v 1.4 2004-08-05 01:45:23 ddkilzer Exp $
 */
public class DriverUnitTest extends UnitTestBase {


    /**
     * Construct a test suite for this class.
     * <p/>
     * The test suite includes the tests in this class, and adds tests
     * from {@link DefaultPropertiesTestLibrary} after creating
     * anonymous {@link DefaultPropertiesTester} objects.
     * 
     * @return The test suite to run.
     */
    public static Test suite() {

        TestSuite testSuite = new TestSuite(DriverUnitTest.class);

        testSuite.addTest(
                DefaultPropertiesTestLibrary_Driver_parseURL.suite("test_parseURL_DefaultProperties"));

        testSuite.addTest(
                DefaultPropertiesTestLibrary_Driver_getPropertyInfo.suite("test_getPropertyInfo_DefaultProperties"));

        return testSuite;
    }


    /**
     * Constructor.
     * 
     * @param name The name of the test.
     */
    public DriverUnitTest(final String name) {
        super(name);
    }


    /**
     * Tests that passing in a null properties argument to
     * {@link net.sourceforge.jtds.jdbc.Driver#getPropertyInfo(java.lang.String, java.util.Properties)}
     * causes the url to be parsed, which then throws a {@link java.sql.SQLException}.
     */
    public void test_getPropertyInfo_ThrowsSQLExceptionWithNullProperties() {
        try {
            new Driver().getPropertyInfo("wxyz:", null);
            fail("Expected SQLException to be throw");
        }
        catch (SQLException e) {
            // Expected
        }
    }


    /**
     * Tests that passing in a non-null properties argument to
     * {@link net.sourceforge.jtds.jdbc.Driver#getPropertyInfo(java.lang.String, java.util.Properties)}
     * causes the url to be parsed, which then throws a {@link java.sql.SQLException}.
     */
    public void test_getPropertyInfo_ThrowsSQLExceptionWithNonNullProperties() {
        try {
            new Driver().getPropertyInfo("wxyz:", new Properties());
            fail("Expected SQLException to be throw");
        }
        catch (SQLException e) {
            // Expected
        }
    }


    public void test_getPropertyInfo_MatchesMessagesProperties() {

        final Map driverPropertyInfoMap = new HashMap();
        loadDriverPropertyInfoMap(driverPropertyInfoMap);

        final Map propertyMap = new HashMap();
        final Map descriptionMap = new HashMap();
        loadMessageProperties(propertyMap, descriptionMap);

        assertEquals(
                "Properties list size (expected) does not equal DriverPropertyInfo array length (actual)",
                propertyMap.size(), driverPropertyInfoMap.keySet().size());
        assertEquals(
                "Description list size (expected) does not equal DriverPropertyInfo array length (actual)",
                descriptionMap.size(), driverPropertyInfoMap.keySet().size());

        for (Iterator iterator = propertyMap.keySet().iterator(); iterator.hasNext();) {

            String key = (String) iterator.next();

            final DriverPropertyInfo driverPropertyInfo = 
                    (DriverPropertyInfo) driverPropertyInfoMap.get(propertyMap.get(key));
            assertNotNull("No DriverPropertyInfo object exists for property '" + key + "'", driverPropertyInfo);

            final String descriptionKey = "prop.desc." + key.substring("prop.".length());
            assertEquals(
                    "Property description (expected) does not match DriverPropertyInfo description (actual)",
                    descriptionMap.get(descriptionKey), driverPropertyInfo.description);
        }
    }


    private void loadDriverPropertyInfoMap(final Map driverPropertyInfoMap) {
        final DriverPropertyInfo[] driverPropertyInfoArray = (DriverPropertyInfo[]) invokeInstanceMethod(
                new Driver(), "getPropertyInfo",
                new Class[]{String.class, Properties.class},
                new Object[]{"jdbc:jtds:sqlserver://servername/databasename", new Properties()});
        for (int i = 0; i < driverPropertyInfoArray.length; i++) {
            DriverPropertyInfo driverPropertyInfo = driverPropertyInfoArray[i];
            driverPropertyInfoMap.put(driverPropertyInfo.name, driverPropertyInfo);
        }
    }


    private void loadMessageProperties(Map propertyMap, Map descriptionMap) {

        final ResourceBundle bundle = (ResourceBundle) invokeStaticMethod(
                Messages.class, "loadResourceBundle", new Class[]{}, new Object[]{});

        final Enumeration keys = bundle.getKeys();
        while (keys.hasMoreElements()) {
            String key = (String) keys.nextElement();
            if (key.startsWith("prop.desc.")) {
                descriptionMap.put(key, bundle.getString(key));
            } else if (key.startsWith("prop.")) {
                propertyMap.put(key, bundle.getString(key));
            }
        }
    }



    /**
     * Class use to test {@link Driver#parseURL(java.lang.String, java.util.Properties)}.
     */ 
    public static class DefaultPropertiesTestLibrary_Driver_parseURL extends DefaultPropertiesTestLibrary {

        private static DefaultPropertiesTester tester;


        /**
         * Construct a test suite for this library.
         * 
         * @param name The name of the tests.
         * @return The test suite.
         */
        public static Test suite(String name) {

            DefaultPropertiesTestLibrary_Driver_parseURL.tester = new DefaultPropertiesTester() {

                public void assertDefaultProperty(String message, String url, Properties properties, String fieldName,
                        String key, String expected) {

                    Properties results =
                            (Properties) invokeStaticMethod(Driver.class, "parseURL",
                                                            new Class[]{String.class, Properties.class},
                                                            new Object[]{url, properties});
                    assertEquals(message, expected, results.getProperty(Messages.get(key)));
                }
            };

            return new TestSuite(DefaultPropertiesTestLibrary_Driver_parseURL.class, name);
        }


        /**
         * Get the <code>tester</code> object.
         * 
         * @return The <code>tester</code> object to be used in the tests.
         */ 
        protected DefaultPropertiesTester getTester() {
            return tester;
        }
    }



    /**
     * Class use to test {@link Driver#getPropertyInfo(java.lang.String, java.util.Properties)}.
     */ 
    public static class DefaultPropertiesTestLibrary_Driver_getPropertyInfo extends DefaultPropertiesTestLibrary {

        private static DefaultPropertiesTester tester;


        /**
         * Construct a test suite for this library.
         * 
         * @param name The name of the tests.
         * @return The test suite.
         */
        public static Test suite(String name) {

            DefaultPropertiesTestLibrary_Driver_getPropertyInfo.tester = new DefaultPropertiesTester() {

                public void assertDefaultProperty(String message, String url, Properties properties, String fieldName,
                        String key, String expected) {

                    try {
                        boolean found = false;
                        String messageKey = Messages.get(key);

                        DriverPropertyInfo[] infoArray = new Driver().getPropertyInfo(url, properties);
                        for (int i = 0; i < infoArray.length; i++) {
                            DriverPropertyInfo info = infoArray[i];
                            if (info.name.equals(messageKey)) {
                                assertEquals(message, expected, info.value);
                                found = true;
                            }
                        }

                        if (!found) {
                            fail("DriverPropertyInfo for '" + messageKey + "' not found!");
                        }
                    }
                    catch (SQLException e) {
                        throw new RuntimeException(e.getMessage());
                    }
                }
            };

            return new TestSuite(DefaultPropertiesTestLibrary_Driver_getPropertyInfo.class, name);
        }


        /**
         * Get the <code>tester</code> object.
         * 
         * @return The <code>tester</code> object to be used in the tests.
         */ 
        protected DefaultPropertiesTester getTester() {
            return tester;
        }
    }
}
