package net.sourceforge.jtds.test;

import junit.framework.Test;
import junit.framework.TestSuite;
import java.util.Properties;
import java.util.HashMap;
import java.util.Map;
import net.sourceforge.jtds.jdbc.ConnectionJDBC2;
import net.sourceforge.jtds.jdbc.Driver;
import net.sourceforge.jtds.jdbc.DefaultProperties;



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
     * Dummy test.
     * <p/>
     * Needed so that this class has at least one test.
     */ 
    public void testDummy() {
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
