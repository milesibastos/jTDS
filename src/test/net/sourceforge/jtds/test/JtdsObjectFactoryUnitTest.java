package net.sourceforge.jtds.test;

import junit.framework.Test;
import junit.framework.TestSuite;

import net.sourceforge.jtds.jdbcx.JtdsObjectFactory;



/**
 * Unit tests for the {@link JtdsObjectFactory} class.
 *
 * @author David D. Kilzer
 * @version $Id: JtdsObjectFactoryUnitTest.java,v 1.4 2005-02-07 13:47:45 alin_sinpalean Exp $
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
     * Tests that the public constructor works.
     * <p/>
     * Needed so that this class has at least one test.
     */
    public void testPublicConstructor() {
        assertNotNull(new JtdsObjectFactory());
    }
}
