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
import java.sql.SQLException;
import java.sql.DriverPropertyInfo;
import java.util.Properties;



/**
 * Unit tests for the {@link net.sourceforge.jtds.jdbc.Driver} class.
 * 
 * @author David D. Kilzer.
 * @version $Id: DriverUnitTest.java,v 1.3 2004-08-04 03:30:36 ddkilzer Exp $
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
                    assertEquals(message, expected, results.getProperty(Support.getMessage(key)));
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
                        String messageKey = Support.getMessage(key);

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
