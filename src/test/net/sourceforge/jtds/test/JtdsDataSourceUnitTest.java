package net.sourceforge.jtds.test;

import junit.framework.Test;
import junit.framework.TestSuite;
import java.util.Properties;
import net.sourceforge.jtds.jdbcx.JtdsDataSource;
import net.sourceforge.jtds.jdbc.Messages;
import javax.naming.Reference;
import javax.naming.NamingException;



/**
 * Unit tests for the {@link JtdsDataSource} class.
 * 
 * @author David D. Kilzer
 * @version $Id: JtdsDataSourceUnitTest.java,v 1.5 2004-08-16 18:43:05 ddkilzer Exp $
 */
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
                JtdsDataSourceUnitTest.Test_JtdsDataSource_fields.suite(
                        "test_fields_DefaultProperties"));

        testSuite.addTest(
                JtdsDataSourceUnitTest.Test_JtdsDataSource_getReference.suite(
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
    public static class Test_JtdsDataSource_fields
            extends DefaultPropertiesTestLibrary {

        /**
         * Construct a test suite for this library.
         * 
         * @param name The name of the tests.
         * @return The test suite.
         */
        public static Test suite(String name) {
            return new TestSuite(JtdsDataSourceUnitTest.Test_JtdsDataSource_fields.class, name);
        }

        /**
         * Default constructor.
         * <p/>
         * This class only has one default setup (SQL Server with TDS 7.0),
         * so flags are set to make sure only that configuration is tested.
         */
        public Test_JtdsDataSource_fields() {

            setOnlySqlServerTests(true);
            setOnlyTds70Tests(true);

            setTester(
                    new DefaultPropertiesTester() {

                        public void assertDefaultProperty(
                                String message, String url, Properties properties, String fieldName,
                                String key, String expected) {

                            // Hack for JtdsDataSource.getTds()
                            {
                                if ("tdsVersion".equals(fieldName)) {
                                    fieldName = "tds";
                                }
                            }

                            JtdsDataSource dataSource = new JtdsDataSource();
                            String actual = 
                                    String.valueOf(
                                            invokeInstanceMethod(
                                                    dataSource,
                                                    "get" + ucFirst(fieldName),
                                                    new Class[]{}, new Object[]{}));
                            assertEquals(message, expected, actual);
                        }
                    }
            );
        }
    }



    /**
     * Class used to test {@link JtdsDataSource#getReference()}.
     */
    public static class Test_JtdsDataSource_getReference
            extends DefaultPropertiesTestLibrary {

        /**
         * Construct a test suite for this library.
         * 
         * @param name The name of the tests.
         * @return The test suite.
         */
        public static Test suite(String name) {
            return new TestSuite(
                    JtdsDataSourceUnitTest.Test_JtdsDataSource_getReference.class, name);
        }

        /**
         * Default constructor.
         * <p/>
         * This class only has one default setup (SQL Server with TDS 7.0),
         * so flags are set to make sure only that configuration is tested.
         */
        public Test_JtdsDataSource_getReference() {
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
