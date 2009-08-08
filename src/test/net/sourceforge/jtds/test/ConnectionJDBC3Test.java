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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Savepoint;

/**
 * JDBC 3.0-only tests for Connection.
 *
 * @author Alin Sinpalean
 * @version $Id: ConnectionJDBC3Test.java,v 1.1.2.1 2009-08-08 14:37:55 ickzon Exp $
 */
public class ConnectionJDBC3Test extends DatabaseTestCase {

    public ConnectionJDBC3Test(String name) {
        super(name);
    }

    /**
     * Test that temporary procedures created within transactions with
     * savepoints which are released are still kept in the procedure cache.
     *
     * @test.manual when testing, prepareSQL will have to be set to 1 to make
     *              sure temp procedures are used
     */
    public void testSavepointRelease() throws SQLException {
        // Manual commit mode
        con.setAutoCommit(false);
        // Create two savepoints
        Savepoint sp1 = con.setSavepoint();
        Savepoint sp2 = con.setSavepoint();
        // Create and execute a prepared statement
        PreparedStatement stmt = con.prepareStatement("SELECT 1");
        assertTrue(stmt.execute());
        // Release the inner savepoint and rollback the outer
        con.releaseSavepoint(sp2);
        con.rollback(sp1);
        // Now make sure the temp stored procedure still exists
        assertTrue(stmt.execute());
        // Release resources
        stmt.close();
        con.close();
    }
    
    public void testUnclosedSocket() throws SQLException {
        final int count = 100000;

        Connection conn = null;
        String url = "jdbc:jtds:sqlserver://localhost;loginTimeout=0";

        for (int i = 0; i < count; i ++) {
            try {
                conn = DriverManager.getConnection(url, "sa", "invalid_password");
                assertTrue(false);
            } catch (SQLException e) {
                assertEquals(e.getErrorCode(),18456);
            }
        }
        
    }

}