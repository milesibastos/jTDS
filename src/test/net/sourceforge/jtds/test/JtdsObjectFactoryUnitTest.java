package net.sourceforge.jtds.test;

import junit.framework.Test;
import junit.framework.TestSuite;
import net.sourceforge.jtds.jdbcx.JtdsDataSource;
import net.sourceforge.jtds.jdbcx.JtdsObjectFactory;
import javax.naming.NamingException;
import javax.naming.Reference;
import java.util.Properties;



/**
 * Unit tests for the {@link JtdsObjectFactory} class.
 * 
 * @author David D. Kilzer
 * @version $Id: JtdsObjectFactoryUnitTest.java,v 1.1 2004-08-16 18:43:05 ddkilzer Exp $
 */
public class JtdsObjectFactoryUnitTest extends UnitTestBase {

    /**
     * Construct a test suite for this class.
     * <p/>
     * The test suite includes the tests in this class, and adds tests from {@link DefaultPropertiesTestLibrary} after
     * creating an anonymous {@link DefaultPropertiesTester} object.
     * 
     * @return The test suite to run.
     */
    public static Test suite() {

        final TestSuite testSuite = new TestSuite(JtdsObjectFactoryUnitTest.class);

        testSuite.addTest(
                JtdsObjectFactoryUnitTest.Test_JtdsObjectFactory_getObjectInstance.suite(
                        "test_getObjectInstance_DefaultProperties"));

        // todo: Run through tests will all NON-DEFAULT properties (JtdsObjectFactory is missing some properties)

        return testSuite;
    }


    /**
     * Constructor.
     * 
     * @param name The name of the test.
     */
    public JtdsObjectFactoryUnitTest(String name) {
        super(name);
    }


    /**
     * Tests that the public constructor works.
     * <p/>
     * Needed so that this class has at least one test.
     */
    public void testPublicConstructor() {
        assertNotNull(new JtdsObjectFactory());
    }



    /**
     * Class used to test {@link JtdsObjectFactory#getObjectInstance(Object, javax.naming.Name, javax.naming.Context,
            * java.util.Hashtable)}.
     */
    public static class Test_JtdsObjectFactory_getObjectInstance
            extends DefaultPropertiesTestLibrary {

        /**
         * Construct a test suite for this library.
         * 
         * @param name The name of the tests.
         * @return The test suite.
         */
        public static Test suite(String name) {
            return new TestSuite(JtdsObjectFactoryUnitTest.Test_JtdsObjectFactory_getObjectInstance.class, name);
        }


        /**
         * Default constructor.
         * <p/>
         * This class only has one default setup (SQL Server with TDS 7.0), so flags are set to make sure only that
         * configuration is tested.
         */
        public Test_JtdsObjectFactory_getObjectInstance() {
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

                            try {
                                Reference reference = new JtdsDataSource().getReference();
                                JtdsDataSource dataSource =
                                        (JtdsDataSource) new JtdsObjectFactory().getObjectInstance(
                                                reference, null, null, null);
                                String actual =
                                        String.valueOf(invokeInstanceMethod(dataSource,
                                                                            "get" + ucFirst(fieldName),
                                                                            new Class[]{}, new Object[]{}));
                                assertEquals(message, expected, actual);
                            }
                            catch (NamingException e) {
                                throw new RuntimeException(e.getMessage());
                            }
                            catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }
            );
        }
    }
}
