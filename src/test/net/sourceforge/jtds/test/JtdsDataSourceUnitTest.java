package net.sourceforge.jtds.test;

import junit.framework.Test;
import junit.framework.TestSuite;
import java.util.Properties;
import net.sourceforge.jtds.jdbcx.JtdsDataSource;
import net.sourceforge.jtds.jdbc.Messages;
import javax.naming.Reference;
import javax.naming.NamingException;



public class JtdsDataSourceUnitTest extends UnitTestBase {

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

        final TestSuite testSuite = new TestSuite(JtdsDataSourceUnitTest.class);

        testSuite.addTest(
                JtdsDataSourceUnitTest.DefaultPropertiesTestLibrary_JtdsDataSource.suite(
                        "test_JtdsDataSource_field_DefaultProperties"));

        testSuite.addTest(
                JtdsDataSourceUnitTest.DefaultPropertiesTestLibrary_JtdsDataSource_getReference.suite(
                        "test_getReference_DefaultProperties"));

        return testSuite;
    }


    /**
     * Constructor.
     * 
     * @param name The name of the test.
     */
    public JtdsDataSourceUnitTest(String name) {
        super(name);
    }


    /**
     * Tests that the public constructor works.
     * <p/>
     * Needed so that this class has at least one test.
     */ 
    public void testPublicConstructor() {
        assertNotNull(new JtdsDataSource());
    }



    /**
     * Class used to test {@link JtdsDataSource}.
     */
    public static class DefaultPropertiesTestLibrary_JtdsDataSource
            extends DefaultPropertiesTestLibrary {

        /**
         * Construct a test suite for this library.
         * 
         * @param name The name of the tests.
         * @return The test suite.
         */
        public static Test suite(String name) {
            return new TestSuite(JtdsDataSourceUnitTest.DefaultPropertiesTestLibrary_JtdsDataSource.class, name);
        }

        /**
         * Default constructor.
         * <p/>
         * This class only has one default setup (SQL Server with TDS 7.0),
         * so flags are set to make sure only that configuration is tested.
         */
        public DefaultPropertiesTestLibrary_JtdsDataSource() {

            setOnlySqlServerTests(true);
            setOnlyTds70Tests(true);

            setTester(
                    new DefaultPropertiesTester() {

                        public void assertDefaultProperty(
                                String message, String url, Properties properties, String fieldName,
                                String key, String expected) {

                            JtdsDataSource dataSource = new JtdsDataSource();
                            String actual = 
                                    String.valueOf(
                                            invokeInstanceMethod(
                                                    dataSource,
                                                    "get" + ucFirst(fieldName),
                                                    new Class[]{}, new Object[]{}));
                            assertEquals(message, expected, actual);
                        }

                        private String ucFirst(String s) {
                            if (s == null || s.length() == 0) return s;
                            if (s.length() == 1) {
                                return s.toUpperCase();
                            }
                            return s.substring(0, 1).toUpperCase() + s.substring(1);
                        }
                    }
            );
        }
    }



    /**
     * Class used to test {@link JtdsDataSource#getReference()}.
     */
    public static class DefaultPropertiesTestLibrary_JtdsDataSource_getReference
            extends DefaultPropertiesTestLibrary {

        /**
         * Construct a test suite for this library.
         * 
         * @param name The name of the tests.
         * @return The test suite.
         */
        public static Test suite(String name) {
            return new TestSuite(
                    JtdsDataSourceUnitTest.DefaultPropertiesTestLibrary_JtdsDataSource_getReference.class, name);
        }

        /**
         * Default constructor.
         * <p/>
         * This class only has one default setup (SQL Server with TDS 7.0),
         * so flags are set to make sure only that configuration is tested.
         */
        public DefaultPropertiesTestLibrary_JtdsDataSource_getReference() {
            setOnlySqlServerTests(true);
            setOnlyTds70Tests(true);
            setTester(
                    new DefaultPropertiesTester() {

                        public void assertDefaultProperty(
                                String message, String url, Properties properties, String fieldName,
                                String key, String expected) {

                            try {
                                Reference reference = new JtdsDataSource().getReference();
                                assertEquals(message, expected, reference.get(Messages.get(key)).getContent());
                            }
                            catch (NamingException e) {
                                throw new RuntimeException(e.getMessage());
                            }
                        }
                    }
            );
        }
    }

}
