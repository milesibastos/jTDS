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

import java.sql.*;

/**
 * Simple test case that runs multiple threads against the same connection
 * trying to break the driver.
 * <p>
 * NOTE: This test needs to be updated to run properly within the test harness.
 * It is being included now for complete application of the patch included in
 * bug [1017616] 0.9-RC1 Threading problem.
 *
 * @version $Id: TestMultiThread.java,v 1.2 2004-10-27 14:57:58 alin_sinpalean Exp $
 */
public class TestMultiThread extends Thread {
    public static final String driverClass = "net.sourceforge.jtds.jdbc.Driver";
    public static final String driverUrl = "jdbc:jtds:sqlserver://localhost/jtds;tds=8.0";
    public static final int THREAD_MAX = 20;
    public static final int LOOP_MAX = 100;
    static Connection con;
    static int live;
    int threadId;

    public TestMultiThread(int n) {
        threadId = n;
    }

    public void run() {
        try {
            Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

            for (int i = 1; i <= LOOP_MAX; i++) {
                ResultSet rs = stmt.executeQuery("SELECT * FROM #TEST");

                while (rs.next()) {
                    rs.getInt(1);
                    rs.getString(2);
                }

                System.err.println("ID=" + threadId + " loop=" + i);
            }

            stmt.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        live--;
    }

    public static void main(String[] args) {
        try {
            Class.forName(driverClass).newInstance();

            DriverManager.setLoginTimeout(5);
            con = DriverManager.getConnection(driverUrl, "xxxx", "xxxx");
            Statement stmt = con.createStatement();
            stmt.execute("CREATE TABLE #TEST (id int identity primary key, data varchar(255))");

            for (int i = 1; i < 101; i++) {
                stmt.executeUpdate("INSERT INTO #TEST (data) VALUES('This is line " + i + "')");
            }

            stmt.close();
            live = THREAD_MAX;

            for (int i = 0; i < THREAD_MAX; i++) {
                TestMultiThread t = new TestMultiThread(i);

                t.start();
            }

            while (live > 0) {
                sleep(1);
            }

            con.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
