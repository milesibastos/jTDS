/*
 * AsTest.java
 *
 * Created on 10. September 2001, 09:44
 */

package freetds;
import java.sql.*;
import java.math.BigDecimal;
import com.internetcds.util.Logger;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.framework.AssertionFailedError;
import java.io.*;
import java.lang.reflect.*;

/**
 *
 * @author  builder
 * @version 
 */
public class AsTest extends DatabaseTestCase {

  public AsTest(String name) {
    super(name);
  }
  
  public static void main(String args[]) {
    try {
      Logger.setActive(true);
    }
    catch (java.io.IOException ex) {
      throw new RuntimeException("Unexpected Exception " + ex + " occured in main");
    }
    if (args.length > 0) {
      junit.framework.TestSuite s = new TestSuite();
      for (int i = 0; i < args.length; i++) {
        s.addTest(new AsTest(args[i]));
      }
      junit.textui.TestRunner.run(s);
    }
    else
      junit.textui.TestRunner.run(AsTest.class);
  }
  
  public void testProc1() throws Exception
  {

    boolean passed = false;
    Statement stmt = con.createStatement();
    stmt.executeUpdate("  if (exists (select * from sysobjects where name = 'spTestExec')) drop procedure spTestExec");
    stmt.executeUpdate("  if (exists (select * from sysobjects where name = 'spTestExec2')) drop procedure spTestExec2");

    stmt.executeUpdate(" create procedure spTestExec2 as " +
                    "select 'Did it work?' as Result");
    stmt.executeUpdate("create procedure spTestExec as " +
		"create table #tmp ( Result varchar(50) ) " + 
		"insert #tmp execute spTestExec2 " +
		"select * from #tmp");
    CallableStatement cstmt = con.prepareCall("exec spTestExec");
    ResultSet rs = cstmt.executeQuery();
    while (rs.next()) {
      passed = true;
    }
    assertTrue(passed);
    // stmt.executeQuery("execute spTestExec");

  }

}
