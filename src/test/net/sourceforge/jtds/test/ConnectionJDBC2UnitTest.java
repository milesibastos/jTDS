package net.sourceforge.jtds.test;

import junit.framework.Test;
import junit.framework.TestSuite;
import java.util.Properties;
import net.sourceforge.jtds.jdbc.ConnectionJDBC2;
import net.sourceforge.jtds.jdbc.Driver;
import net.sourceforge.jtds.jdbc.DefaultProperties;
import net.sourceforge.jtds.jdbc.Messages;



public class ConnectionJDBC2UnitTest extends UnitTestBase {

    /**
     * Construct a test suite for this class.
     * <p/>
     * The test suite includes the tests in this class, and adds tests
     * from {@link DefaultPropertiesTestLibrary} after creating an
     * anonymous {@link DefaultPropertiesTester} object.
     *
     * @return The test suite to run.
     */
    public static Test suite() {

        final TestSuite testSuite = new TestSuite(ConnectionJDBC2UnitTest.class);

        testSuite.addTest(
                ConnectionJDBC2UnitTest.Test_ConnectionJDBC2_unpackProperties.suite(
                        "test_unpackProperties_DefaultProperties"));

        return testSuite;
    }


    /**
     * Constructor.
     *
     * @param name The name of the test.
     */
    public ConnectionJDBC2UnitTest(String name) {
        super(name);
    }


    /**
     * Test that an {@link java.sql.SQLException} is thrown when
     * parsing invalid integer (and long) properties.
     */
    public void test_unpackProperties_invalidIntegerProperty() {
        assertSQLExceptionForBadWholeNumberProperty(Driver.PORTNUMBER);
        assertSQLExceptionForBadWholeNumberProperty(Driver.SERVERTYPE);
        assertSQLExceptionForBadWholeNumberProperty(Driver.PREPARESQL);
        assertSQLExceptionForBadWholeNumberProperty(Driver.PACKETSIZE);
        assertSQLExceptionForBadWholeNumberProperty(Driver.LOGINTIMEOUT);
        assertSQLExceptionForBadWholeNumberProperty(Driver.LOBBUFFER);
    }


    /**
     * Assert that an SQLException is thrown when
     * {@link ConnectionJDBC2#unpackProperties(Properties)} is called
     * with an invalid integer (or long) string set on a property.
     * <p/>
     * Note that because Java 1.3 is still supported, the
     * {@link RuntimeException} that is caught may not contain the
     * original {@link Throwable} cause, only the original message.
     *
     * @param key The message key used to retrieve the property name.
     */
    private void assertSQLExceptionForBadWholeNumberProperty(final String key) {

        final ConnectionJDBC2 instance =
                (ConnectionJDBC2) invokeConstructor(
                        ConnectionJDBC2.class, new Class[]{}, new Object[]{});

        final Properties properties =
                (Properties) invokeStaticMethod(
                        Driver.class, "parseURL",
                        new Class[]{String.class, Properties.class},
                        new Object[]{"jdbc:jtds:sqlserver://servername", new Properties()});

        properties.setProperty(Messages.get(key), "1.21 Gigawatts");

        try {
            invokeInstanceMethod(
                    instance, "unpackProperties",
                    new Class[]{Properties.class},
                    new Object[]{properties});
            fail("RuntimeException expected");
        }
        catch (RuntimeException e) {
            assertEquals("Unexpected exception message",
                         Messages.get("error.connection.badprop", Messages.get(key)),
                         e.getMessage());
        }
    }



    /**
     * Class used to test {@link net.sourceforge.jtds.jdbc.ConnectionJDBC2#unpackProperties(Properties)}.
     */
    public static class Test_ConnectionJDBC2_unpackProperties
            extends DefaultPropertiesTestLibrary {

        /**
         * Construct a test suite for this library.
         *
         * @param name The name of the tests.
         * @return The test suite.
         */
        public static Test suite(String name) {
            return new TestSuite(
                    ConnectionJDBC2UnitTest.Test_ConnectionJDBC2_unpackProperties.class, name);
        }


        /**
         * Default constructor.
         */
        public Test_ConnectionJDBC2_unpackProperties() {
            setTester(
                    new DefaultPropertiesTester() {

                        public void assertDefaultProperty(
                                String message, String url, Properties properties, String fieldName,
                                String key, String expected) {

                            // FIXME: Hack for ConnectionJDBC2
                            {
                                if ("sendStringParametersAsUnicode".equals(fieldName)) {
                                    fieldName = "useUnicode";
                                }
                            }

                            Properties parsedProperties =
                                    (Properties) invokeStaticMethod(
                                            Driver.class, "parseURL",
                                            new Class[]{ String.class, Properties.class},
                                            new Object[]{ url, properties});
                            ConnectionJDBC2 instance =
                                    (ConnectionJDBC2) invokeConstructor(
                                            ConnectionJDBC2.class, new Class[]{}, new Object[]{});
                            invokeInstanceMethod(
                                    instance, "unpackProperties",
                                    new Class[]{Properties.class},
                                    new Object[]{parsedProperties});
                            String actual =
                                    String.valueOf(invokeGetInstanceField(instance, fieldName));

                            // FIXME: Another hack for ConnectionJDBC2
                            {
                                if ("tdsVersion".equals(fieldName)) {
                                    expected = String.valueOf(DefaultProperties.getTdsVersion(expected));
                                }
                            }

                            assertEquals(message, expected, actual);
                        }
                    }
            );
        }
    }

}
