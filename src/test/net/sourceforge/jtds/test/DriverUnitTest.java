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

import net.sourceforge.jtds.jdbc.Driver;
import java.sql.SQLException;
import java.util.Properties;



/**
 * Unit tests for the {@link net.sourceforge.jtds.jdbc.Driver} class.
 * 
 * @author David D. Kilzer.
 * @version $Id: DriverUnitTest.java,v 1.1 2004-08-03 19:14:13 ddkilzer Exp $
 */
public class DriverUnitTest extends UnitTestBase {

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
    public void testGetPropertyInfoThrowsSQLException_NullProperties() {
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
    public void testGetPropertyInfoThrowsSQLException_NonNullProperties() {
        try {
            new Driver().getPropertyInfo("wxyz:", new Properties());
            fail("Expected SQLException to be throw");
        }
        catch (SQLException e) {
            // Expected
        }
    }

}
