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
 * @version 1.0
 */
public class AsTest extends DatabaseTestCase {

  public AsTest(String name) {
    super(name);
  }

  public static void main(String args[]) {
    Logger.setActive(true);
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
    stmt.executeUpdate("  if (exists (select * from sysobjects where name = '#spTestExec')) drop procedure spTestExec");
    stmt.executeUpdate("  if (exists (select * from sysobjects where name = '#spTestExec2')) drop procedure spTestExec2");

    stmt.executeUpdate(" create procedure #spTestExec2 as " +
                    "select 'Did it work?' as Result");
    stmt.executeUpdate("create procedure #spTestExec as " +
                "set nocount off " +
        "create table #tmp ( Result varchar(50) ) " +
        "insert #tmp execute #spTestExec2 " +
        "select * from #tmp");
    CallableStatement cstmt = con.prepareCall("#spTestExec");
    assertTrue(cstmt.execute());

//    The JDBC-ODBC driver does not return update counts from stored procedures
//    so we won't, either.
//    assertTrue(cstmt.getUpdateCount() == 0);  // set
//    assertTrue(!cstmt.getMoreResults());
//    assertTrue(cstmt.getUpdateCount() == 0);  // create
//    assertTrue(!cstmt.getMoreResults());
//    assertTrue(cstmt.getUpdateCount() == 0);  // execute
//    assertTrue(!cstmt.getMoreResults());
//    assertTrue(cstmt.getUpdateCount() == 1);  // insert
//    assertTrue(cstmt.getMoreResults());

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
      "if (exists(select * from sysobjects where name = '#multi1withcount' and xtype = 'P'))" +
      "  drop procedure #multi1withcount ";
    String sqlwithcount2 =
      "create procedure #multi1withcount as " +
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
      "if (exists(select * from sysobjects where name = '#multi1nocount' and xtype = 'P'))" +
      "  drop procedure #multi1nocount ";
    String sqlnocount2 =
      "create procedure #multi1nocount as " +
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

    CallableStatement cstmt = con.prepareCall("#multi1nocount");
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
    cstmt = con.prepareCall("#multi1withcount");

//    The JDBC-ODBC driver does not return update counts from stored procedures
//    so we won't, either.
//    assertTrue(!cstmt.execute());   // because of set nocount off
    assertTrue(cstmt.execute());   // because of set nocount off
    s2 = cstmt;
//    assertTrue(s2.getMoreResults());
    rs = s2.getResultSet();
    assertTrue(rs.next());
    assertTrue(rs.getString(1).equals("a"));
    assertTrue(!rs.next());
    assertTrue(s2.getMoreResults());
    rs = s2.getResultSet();
    assertTrue(rs.next());
    assertTrue(rs.getString(1).equals("b"));
    assertTrue(!rs.next());
//    The JDBC-ODBC driver does not return update counts from stored procedures
//    so we won't, either.
//    assertTrue(!s2.getMoreResults() && s2.getUpdateCount() == 0);  // create
//    assertTrue(!s2.getMoreResults() && s2.getUpdateCount() == 1);  // insert
//    assertTrue(!s2.getMoreResults() && s2.getUpdateCount() == 1);  // insert
//    assertTrue(!s2.getMoreResults() && s2.getUpdateCount() == 1);  // insert
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

  public void testBug457955() throws Exception
  {
    boolean passed = false;
    Statement stmt = con.createStatement();
    dropProcedure("#Bug457955");
    stmt.executeUpdate("  create procedure #Bug457955 (@par1 VARCHAR(10)) as select @par1");
    String param = "123456789";
    CallableStatement s = con.prepareCall( "exec #Bug457955 ?" );
    s.setString(1, param);
    ResultSet r = s.executeQuery();

  }



  public void testBugAttTest2() throws Exception
  {
    String tabdef =
      "CREATE TABLE #ICEributeTest_AttributeTest2( " +
      "  ICEobjectId NUMERIC(19) " +
      "     /*CONSTRAINT ICEributeTest_AttributeTest2_PKICEobjectId PRIMARY KEY */ " +
      "    ,  " +
      "  ICEtestShort INTEGER  " +
      "     NULL,  " +
      "  ICEtestFloat NUMERIC(28,10) " +
      "     NULL, " +
      "  ICEtestDecimal NUMERIC(28,10) " +
      "     NULL, " +
      "  ICEtestCharacter INTEGER " +
      "     NULL, " +
      "  ICEtestInteger INTEGER " +
      "     NULL, " +
      "  ICEtestString VARCHAR(20) " +
      "     NULL, " +
      "  ICEtestBoolean BIT " +
      "     NULL, " +
      "  ICEtestByte INTEGER " +
      "     NULL, " +
      "  ICEtestDouble NUMERIC(28,10) " +
      "     NULL, " +
      "  ICEtestLong NUMERIC(19) " +
      "     NULL, " +
      "  ICEtestCombined1 VARBINARY(8000) " +
      "     NULL, " +
      "  ICEtestDate DATETIME " +
      "     NULL, " +
      "  testCombined_testFloat NUMERIC(28,10) " +
      "     NULL, " +
      "  testCombined_testShort INTEGER " +
      "     NULL, " +
      "  testCombined_testDecimal NUMERIC(28,10) " +
      "     NULL, " +
      "  testCombined_testCharacter INTEGER " +
      "     NULL, " +
      "  testCombined_testInteger INTEGER " +
      "     NULL, " +
      "  testCombined_testString VARCHAR(50) " +
      "     NULL, " +
      "  testCombined_testBoolean BIT " +
      "     NULL, " +
      "  testCombined_testByte INTEGER " +
      "     NULL, " +
      "  testCombined_testDouble NUMERIC(28,10) " +
      "     NULL, " +
      "  testCombined_testLong NUMERIC(19) " +
      "     NULL, " +
      "  testCombined_testDate DATETIME " +
      "     NULL, " +
      "  ICEtestContainedArrays VARBINARY(8000) " +
      "     NULL, " +
      "  BSF_FILTER_ATTRIBUTE_NAME INTEGER " +
      "     NOT NULL, " +
      "  updateCount INTEGER " +
      "    NOT NULL " +
      "  ) ";
    Statement stmt = con.createStatement();
    dropTable("#ICEributeTest_AttributeTest2");
    stmt.executeUpdate(tabdef);
    PreparedStatement istmt = con.prepareStatement("INSERT INTO #ICEributeTest_AttributeTest2 (" +
      "ICEobjectId,BSF_FILTER_ATTRIBUTE_NAME,ICEtestShort,ICEtestFloat,ICEtestDecimal,"+
      "ICEtestCharacter,ICEtestInteger,ICEtestString,ICEtestBoolean,ICEtestByte,"+
      "ICEtestDouble,ICEtestLong,ICEtestCombined1,ICEtestDate,testCombined_testFloat,"+
      "testCombined_testShort,testCombined_testDecimal,testCombined_testCharacter,testCombined_testInteger,testCombined_testString,"+
      "testCombined_testBoolean,testCombined_testByte,testCombined_testDouble,testCombined_testLong"+
      ",testCombined_testDate,ICEtestContainedArrays,updateCount ) "+
      "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
    istmt.setLong(1,(long)650002);
    istmt.setInt(2,(int)-1461101755);
    istmt.setNull(3,java.sql.Types.INTEGER);
    istmt.setNull(4,java.sql.Types.REAL);
    try {
      istmt.setNull(5,java.sql.Types.NUMERIC);
    }
    catch (java.sql.SQLException e) {
      istmt.setNull(5,java.sql.Types.DECIMAL);
    }
    istmt.setNull(6,java.sql.Types.INTEGER);
    istmt.setNull(7,java.sql.Types.INTEGER);
    istmt.setNull(8,java.sql.Types.VARCHAR);
    istmt.setNull(9,java.sql.Types.BIT);
    istmt.setNull(10,java.sql.Types.INTEGER);
    istmt.setNull(11,java.sql.Types.DOUBLE);
    istmt.setNull(12,java.sql.Types.BIGINT);
    istmt.setNull(13,java.sql.Types.LONGVARBINARY);
    istmt.setNull(14,java.sql.Types.TIMESTAMP);
    istmt.setNull(15,java.sql.Types.REAL);
    istmt.setNull(16,java.sql.Types.INTEGER);
    try {
      istmt.setNull(17,java.sql.Types.NUMERIC);
    }
    catch (java.sql.SQLException e) {
      istmt.setNull(17,java.sql.Types.DECIMAL);
    }
    istmt.setNull(18,java.sql.Types.INTEGER);
    istmt.setNull(19,java.sql.Types.INTEGER);
    istmt.setNull(20,java.sql.Types.VARCHAR);
    istmt.setNull(21,java.sql.Types.BIT);
    istmt.setNull(22,java.sql.Types.INTEGER);
    istmt.setNull(23,java.sql.Types.DOUBLE);
    istmt.setNull(24,java.sql.Types.BIGINT);
    istmt.setNull(25,java.sql.Types.TIMESTAMP);
    istmt.setNull(26,java.sql.Types.LONGVARBINARY);
    istmt.setInt(27,1);
    istmt.execute();

  }

  public void testBigInt() throws Throwable
  {
    // String crtab = "create table #testBigInt (a bigint)";
    String crtab = "create table #testBigInt (a NUMERIC(19) NULL)";
    dropTable("#testBigInt");
    Statement stmt = con.createStatement();
    stmt.executeUpdate(crtab);
    PreparedStatement pstmt = con.prepareStatement("insert into #testBigInt values (?)");
    pstmt.setNull(1,java.sql.Types.BIGINT);
    assertTrue(!pstmt.execute());
    assertTrue(pstmt.getUpdateCount() == 1);
    pstmt.setLong(1,99999999999L);
    assertTrue(!pstmt.execute());
    assertTrue(pstmt.getUpdateCount() == 1);
    pstmt.setLong(1,-99999999999L);
    assertTrue(!pstmt.execute());
    assertTrue(pstmt.getUpdateCount() == 1);
    pstmt.setLong(1,9999999999999L);
    assertTrue(!pstmt.execute());
    assertTrue(pstmt.getUpdateCount() == 1);
    pstmt.setLong(1,-9999999999999L);
    assertTrue(!pstmt.execute());
    assertTrue(pstmt.getUpdateCount() == 1);
    pstmt.setLong(1,(long)99999999999L);
    assertTrue(!pstmt.execute());
    assertTrue(pstmt.getUpdateCount() == 1);
  }
  public void testBoolean() throws Throwable
  {
    // String crtab = "create table #testBigInt (a bigint)";
    String crtab = "create table #testBit (a BIT NULL)";
    dropTable("#testBit");
    Statement stmt = con.createStatement();
    stmt.executeUpdate(crtab);
    stmt.executeUpdate("insert into #testBit values (NULL)");
    stmt.executeUpdate("insert into #testBit values (0)");
    stmt.executeUpdate("insert into #testBit values (1)");
    ResultSet rs = stmt.executeQuery("select * from #testBit where a is NULL");
    rs.next();
    rs.getBoolean(1);
    rs = stmt.executeQuery("select * from #testBit where a  = 0");
    rs.next();
    rs.getBoolean(1);
    rs = stmt.executeQuery("select * from #testBit where a = 1");
    rs.next();
    rs.getBoolean(1);
    PreparedStatement pstmt = con.prepareStatement("insert into #testBit values (?)");
    pstmt.setBoolean(1,true);
    assertTrue(!pstmt.execute());
    assertTrue(pstmt.getUpdateCount() == 1);
    pstmt.setBoolean(1,false);
    assertTrue(!pstmt.execute());
    assertTrue(pstmt.getUpdateCount() == 1);
    pstmt.setNull(1,java.sql.Types.BIT);
    assertTrue(!pstmt.execute());
    assertTrue(pstmt.getUpdateCount() == 1);
  }


  public void testBinary() throws Throwable
  {
    String crtab = "create table #testBinary (a varbinary(8000))";
    dropTable("#testBinary");
    byte[] ba = new byte[8000];
    for ( int i = 0; i < ba.length; i++) {
      ba[i] = (byte)(i % 256);
    }
    Statement stmt = con.createStatement();
    stmt.executeUpdate(crtab);
    PreparedStatement pstmt = con.prepareStatement("insert into #testBinary values (?)");
    pstmt.setObject(1,ba);
    pstmt.execute();
  }

  private void checkTime(long time) throws Throwable
  {
    PreparedStatement pstmt = con.prepareStatement("insert into #testTimestamp values (?)");
    java.sql.Timestamp ts = new java.sql.Timestamp(time);
    pstmt.setTimestamp(1,ts);
    pstmt.executeUpdate();
    pstmt.close();
    Statement stmt = con.createStatement();
    ResultSet rs = stmt.executeQuery("select * from #testTimestamp");
    rs.next();
    java.sql.Timestamp tsres = rs.getTimestamp(1);
    assertTrue(ts.equals(tsres));
    stmt.executeUpdate("truncate table #testTimestamp");
  }

  public void testSpecTime() throws Throwable
  {
    String crtab = "create table #testTimestamp (a datetime)";
    dropTable("#testTimestamp");
    Statement stmt = con.createStatement();
    stmt.executeUpdate(crtab);
    checkTime(92001000);
    checkTime(4200000);  // sent in 4 Bytes
    checkTime(4201000);
    checkTime(1234567000);
    checkTime(420000000000L);   // sent in 4 Bytes
    checkTime(840000000000L);
  }

  public void testBigDecimal() throws Throwable
  {    String crtab = "create table #testBigDecimal (a decimal(28,10) NULL)";
    dropTable("#testBigDecimal");
    Statement stmt = con.createStatement();
    stmt.executeUpdate(crtab);
    PreparedStatement pstmt = con.prepareStatement("insert into #testBigDecimal values (?)");
    pstmt.setObject(1,new BigDecimal("10.200"));
    pstmt.execute();
    pstmt.setObject(1,new BigDecimal(10.200));
    pstmt.execute();
    pstmt.setObject(1,null);
    pstmt.execute();
    pstmt.setObject(1,new Integer(20));
    pstmt.execute();
    pstmt.setObject(1,new Double(2.10));
    pstmt.execute();
    pstmt.setObject(1,new BigDecimal(-10.200));
    pstmt.execute();
    pstmt.setObject(1,new Long(200));
    pstmt.execute();
    pstmt.setByte(1,(byte)1);
    pstmt.execute();
    pstmt.setInt(1,200);
    pstmt.execute();
    pstmt.setLong(1,200L);
    pstmt.execute();
    pstmt.setFloat(1,(float)1.1);
    pstmt.execute();
    pstmt.setDouble(1,1.1);
    pstmt.execute();
  }
    }

