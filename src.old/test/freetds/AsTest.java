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
                "set nocount off " + 
		"create table #tmp ( Result varchar(50) ) " + 
		"insert #tmp execute spTestExec2 " +
		"select * from #tmp");
    CallableStatement cstmt = con.prepareCall("spTestExec");
    assertTrue(!cstmt.execute());
    assertTrue(cstmt.getUpdateCount() == 0);  // set 
    assertTrue(!cstmt.getMoreResults());
    assertTrue(cstmt.getUpdateCount() == 0);  // create
    assertTrue(!cstmt.getMoreResults());
    assertTrue(cstmt.getUpdateCount() == 0);  // execute
    assertTrue(!cstmt.getMoreResults());
    assertTrue(cstmt.getUpdateCount() == 1);  // insert
    assertTrue(cstmt.getMoreResults());        
    ResultSet rs = cstmt.getResultSet();
    while (rs.next()) {
      passed = true;
    }
    assertTrue(!cstmt.getMoreResults() && cstmt.getUpdateCount() == -1);
    assertTrue(passed);
    // stmt.executeQuery("execute spTestExec");

  }
  
  public void testProc2() throws Exception
  {
    boolean passed = false;
    Statement stmt = con.createStatement();
    String sqlwithcount1 = 
      "if (exists(select * from sysobjects where name = 'multi1withcount' and xtype = 'P'))" +
      "  drop procedure multi1withcount ";
    String sqlwithcount2 =
      "create procedure multi1withcount as " +
      "  set nocount off " +
      "  select 'a' " +
      "  select 'b' " +
      "  create table #multi1withcountt (A VARCHAR(20)) " +
      "  insert into #multi1withcountt VALUES ('a') " +
      "  insert into #multi1withcountt VALUES ('a') " +
      "  insert into #multi1withcountt VALUES ('a') " +
      "  select 'a' " +
      "  select 'b' ";
    String sqlnocount1 = 
      "if (exists(select * from sysobjects where name = 'multi1nocount' and xtype = 'P'))" +
      "  drop procedure multi1nocount ";
    String sqlnocount2 = 
      "create procedure multi1nocount as " +
      "  set nocount on " +
      "  select 'a' " +
      "  select 'b' " +
      "  create table #multi1nocountt (A VARCHAR(20)) " +
      "  insert into #multi1nocountt VALUES ('a') " +
      "  insert into #multi1nocountt VALUES ('a') " +
      "  insert into #multi1nocountt VALUES ('a') " +
      "  select 'a' " +
      "  select 'b' ";
    stmt.executeUpdate(sqlwithcount1);
    stmt.executeUpdate(sqlnocount1);
    stmt.executeUpdate(sqlwithcount2);
    stmt.executeUpdate(sqlnocount2);
    CallableStatement cstmt = con.prepareCall("multi1nocount");
    assertTrue(cstmt.execute());
    ResultSet rs = cstmt.getResultSet();
    Statement s2 = cstmt;    
    assertTrue(rs.next());
    assertTrue(rs.getString(1).equals("a"));
    assertTrue(!rs.next());
    assertTrue(s2.getMoreResults());
    rs = s2.getResultSet();
    assertTrue(rs.next());
    assertTrue(rs.getString(1).equals("b"));
    assertTrue(!rs.next());
    assertTrue(s2.getMoreResults());
    rs = s2.getResultSet();
    assertTrue(rs.next());
    assertTrue(!rs.next());
    assertTrue(s2.getMoreResults());
    rs = s2.getResultSet();
    assertTrue(rs.next());
    assertTrue(!rs.next());
    assertTrue(!s2.getMoreResults() && s2.getUpdateCount() == -1);
    cstmt = con.prepareCall("multi1withcount");
    assertTrue(!cstmt.execute());   // because of set nocount off
    s2 = cstmt;    
    assertTrue(s2.getMoreResults());
    rs = cstmt.getResultSet();
    assertTrue(rs.next());
    assertTrue(rs.getString(1).equals("a"));
    assertTrue(!rs.next());
    rs = s2.getResultSet();
    assertTrue(rs.next());
    assertTrue(rs.getString(1).equals("b"));
    assertTrue(!rs.next());
    assertTrue(!s2.getMoreResults() && s2.getUpdateCount() == 0);  // create
    assertTrue(!s2.getMoreResults() && s2.getUpdateCount() == 1);  // insert
    assertTrue(!s2.getMoreResults() && s2.getUpdateCount() == 1);  // insert
    assertTrue(!s2.getMoreResults() && s2.getUpdateCount() == 1);  // insert
    assertTrue(s2.getMoreResults());    // select
    rs = s2.getResultSet();
    assertTrue(rs.next());
    assertTrue(!rs.next());
    assertTrue(s2.getMoreResults());
    rs = s2.getResultSet();
    assertTrue(rs.next());
    assertTrue(!rs.next());
    assertTrue(!s2.getMoreResults() && s2.getUpdateCount() == -1);
    
  }

  public void testBatch1() throws Exception
  {
    boolean passed = false;
    Statement stmt = con.createStatement();
    String sqlwithcount1 =
      "  set nocount off " +
      "  select 'a' " +
      "  select 'b' " +
      "  create table #multi2withcountt (A VARCHAR(20)) " +
      "  insert into #multi2withcountt VALUES ('a') " +
      "  insert into #multi2withcountt VALUES ('a') " +
      "  insert into #multi2withcountt VALUES ('a') " +
      "  select 'a' " +
      "  select 'b' " +
      "  drop table #multi2withcountt";
    String sqlnocount1 = 
      "  set nocount on " +
      "  select 'a' " +
      "  select 'b' " +
      "  create table #multi2nocountt (A VARCHAR(20)) " +
      "  insert into #multi2nocountt VALUES ('a') " +
      "  insert into #multi2nocountt VALUES ('a') " +
      "  insert into #multi2nocountt VALUES ('a') " +
      "  select 'a' " +
      "  select 'b' " + 
      "  drop table #multi2nocountt";
    assertTrue(!stmt.execute(sqlwithcount1));    // set
    assertTrue(stmt.getMoreResults());    
    ResultSet rs = stmt.getResultSet();
    assertTrue(rs.next());
    assertTrue(rs.getString(1).equals("a"));
    assertTrue(!rs.next());
    assertTrue(stmt.getMoreResults());
    rs = stmt.getResultSet();
    assertTrue(rs.next());
    assertTrue(rs.getString(1).equals("b"));
    assertTrue(!rs.next());
    assertTrue(!stmt.getMoreResults() && stmt.getUpdateCount() == 0);
    assertTrue(!stmt.getMoreResults() && stmt.getUpdateCount() == 1);
    assertTrue(!stmt.getMoreResults() && stmt.getUpdateCount() == 1);
    assertTrue(!stmt.getMoreResults() && stmt.getUpdateCount() == 1);
    assertTrue(stmt.getMoreResults());
    rs = stmt.getResultSet();
    assertTrue(rs.next());
    assertTrue(!rs.next());
    assertTrue(stmt.getMoreResults());
    rs = stmt.getResultSet();
    assertTrue(rs.next());
    assertTrue(!rs.next());
    assertTrue(!stmt.getMoreResults() && stmt.getUpdateCount() == 0);   // drop
    assertTrue(!stmt.getMoreResults() && stmt.getUpdateCount() == -1);
    
    // next Statement 
    /* nocount seems not to work in batches
    assertTrue(stmt.execute(sqlnocount1));    // set
    rs = stmt.getResultSet();
    assertTrue(rs.next());
    assertTrue(rs.getString(1).equals("a"));
    assertTrue(!rs.next());
    assertTrue(stmt.getMoreResults());
    rs = stmt.getResultSet();
    assertTrue(rs.next());
    assertTrue(rs.getString(1).equals("b"));
    assertTrue(!rs.next());
    assertTrue(stmt.getMoreResults());    // select
    rs = stmt.getResultSet();
    assertTrue(rs.next());
    assertTrue(!rs.next());
    assertTrue(stmt.getMoreResults());
    rs = stmt.getResultSet();
    assertTrue(rs.next());
    assertTrue(!rs.next());
    assertTrue(!stmt.getMoreResults() && stmt.getUpdateCount() == -1);
     */
  }    

}
