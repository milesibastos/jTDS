/*
 * CSUnitTest.java
 *
 * Created on 8. September 2001, 07:54
 */

package freetds;
import java.sql.*;
import java.math.BigDecimal;
import com.internetcds.util.Logger;
import junit.framework.TestCase;
import junit.framework.AssertionFailedError;
import java.io.*;
import java.lang.reflect.*;
/**
 *
 * @author  builder
 * @version
 */
public class CSUnitTest extends DatabaseTestCase {
  
  public CSUnitTest(String name) {
    super(name);
  }
  
  public static void main(String args[]) {
    try {
      Logger.setActive(true);
    }
    catch (java.io.IOException ex) {
      throw new RuntimeException("Unexpected Exception " + ex + " occured in main");
    }
    junit.textui.TestRunner.run(CSUnitTest.class);
  }
  
  
  public void testMaxRows0003() throws Exception {
    dropTable("t0003");
    Statement stmt = con.createStatement();
    
    int count = stmt.executeUpdate("create table t0003              "
    + "  (i  integer not null)       ");
    PreparedStatement  pStmt = con.prepareStatement(
    "insert into t0003 values (?)");
    
    final int rowsToAdd = 100;
    count = 0;
    for(int i=1; i<=rowsToAdd; i++) {
      pStmt.setInt(1, i);
      count += pStmt.executeUpdate();
    }
    assertTrue("count: " + count + " rowsToAdd: " + rowsToAdd,count == rowsToAdd);
    pStmt = con.prepareStatement(
    "select i from t0003 order by i");
    int   rowLimit = 32;
    pStmt.setMaxRows(rowLimit);
    
    assertTrue(pStmt.getMaxRows() == rowLimit);
    ResultSet  rs = pStmt.executeQuery();
    count = 0;
    while(rs.next()) {
      int   n = rs.getInt("i");
      count++;
    }
    assertTrue(count == rowLimit);
  }
  
  
  
  public void testGetAsciiStream0018() throws Exception {
    int         count    = 0;
    Statement   stmt     = con.createStatement();
    ResultSet   rs;
    
    String bigtext1 =
    "abcdefghijklmnop" +
    "abcdefghijklmnop" +
    "abcdefghijklmnop" +
    "abcdefghijklmnop" +
    "abcdefghijklmnop" +
    "abcdefghijklmnop" +
    "abcdefghijklmnop" +
    "abcdefghijklmnop" +
    "abcdefghijklmnop" +
    "abcdefghijklmnop" +
    "abcdefghijklmnop" +
    "abcdefghijklmnop" +
    "abcdefghijklmnop" +
    "abcdefghijklmnop" +
    "abcdefghijklmnop" +
    "abcdefghijklmnop" +
    "abcdefghijklmnop" +
    "abcdefghijklmnop" +
    "abcdefghijklmnop" +
    "abcdefghijklmnop" +
    "abcdefghijklmnop" +
    "abcdefghijklmnop" +
    "abcdefghijklmnop" +
    "abcdefghijklmnop" +
    "abcdefghijklmnop" +
    "abcdefghijklmnop" +
    "abcdefghijklmnop" +
    "abcdefghijklmnop" +
    "abcdefghijklmnop" +
    "abcdefghijklmnop" +
    "";
    String bigimage1 = "0x" +
    "0123456789abcdef" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "fedcba9876543210" +
    "";
    dropTable("t0018");
    String sql =
    "create table t0018 (                                  " +
    " mybinary                   binary(5) not null,       " +
    " myvarbinary                varbinary(4) not null,    " +
    " mychar                     char(10) not null,        " +
    " myvarchar                  varchar(8) not null,      " +
    " mytext                     text not null,            " +
    " myimage                    image not null,           " +
    " mynullbinary               binary(3) null,           " +
    " mynullvarbinary            varbinary(6) null,        " +
    " mynullchar                 char(10) null,            " +
    " mynullvarchar              varchar(40) null,         " +
    " mynulltext                 text null,                " +
    " mynullimage                image null)               ";
    
    count = stmt.executeUpdate(sql);
    // Insert a row without nulls via a Statement
    sql =
    "insert into t0018(       " +
    " mybinary,               " +
    " myvarbinary,            " +
    " mychar,                 " +
    " myvarchar,              " +
    " mytext,                 " +
    " myimage,                " +
    " mynullbinary,           " +
    " mynullvarbinary,        " +
    " mynullchar,             " +
    " mynullvarchar,          " +
    " mynulltext,             " +
    " mynullimage             " +
    ")                        " +
    "values(                  " +
    " 0xffeeddccbb,           " +  // mybinary
    " 0x78,                   " +  // myvarbinary
    " 'Z',                    " +  // mychar
    " '',                     " +  // myvarchar
    " '" + bigtext1 + "',     " +  // mytext
    " " + bigimage1 + ",      " +  // myimage
    " null,                   " +  // mynullbinary
    " null,                   " +  // mynullvarbinary
    " null,                   " +  // mynullchar
    " null,                   " +  // mynullvarchar
    " null,                   " +  // mynulltext
    " null                    " +  // mynullimage
    ")";
    
    
    count = stmt.executeUpdate(sql);
    
    sql = "select * from t0018";
    rs = stmt.executeQuery(sql);
    if (!rs.next()) {
      assertTrue("should get Result",false);
    }
    else {
      System.out.println("Getting the results");
      System.out.println("mybinary is " + rs.getObject("mybinary"));
      System.out.println("myvarbinary is " + rs.getObject("myvarbinary"));
      System.out.println("mychar is " + rs.getObject("mychar"));
      System.out.println("myvarchar is " + rs.getObject("myvarchar"));
      System.out.println("mytext is " + rs.getObject("mytext"));
      System.out.println("myimage is " + rs.getObject("myimage"));
      System.out.println("mynullbinary is " + rs.getObject("mynullbinary"));
      System.out.println("mynullvarbinary is " + rs.getObject("mynullvarbinary"));
      System.out.println("mynullchar is " + rs.getObject("mynullchar"));
      System.out.println("mynullvarchar is " + rs.getObject("mynullvarchar"));
      System.out.println("mynulltext is " + rs.getObject("mynulltext"));
      System.out.println("mynullimage is " + rs.getObject("mynullimage"));
    }
  }
  
  
  public void testMoneyHandling0019() throws Exception {
    java.sql.Statement  stmt   = null;
    BigDecimal          tmp1   = null;
    BigDecimal          tmp2   = null;
    int                 i;
    BigDecimal          money[] = {
      new BigDecimal("922337203685477.5807"),
      new BigDecimal("-922337203685477.5807"),
      new BigDecimal("1.00"),
      new BigDecimal("0.00"),
      new BigDecimal("-1.00")
    };
    BigDecimal          smallmoney[] = {
      new BigDecimal("214748.3647"),
      new BigDecimal("-214748.3648"),
      new BigDecimal("1.00"),
      new BigDecimal("0.00"),
      new BigDecimal("-1.00")
    };
    
    if (smallmoney.length != money.length) {
      throw new SQLException("Must have same number of elements in " +
      "money and smallmoney");
    }
    
    stmt = con.createStatement();
    
    dropTable("t0019");
    
    
    stmt.executeUpdate("create table t0019 (                     " +
    "  i               integer primary key,   " +
    "  mymoney         money not null,        " +
    "  mysmallmoney    smallmoney not null)   " +
    "");
    
    for(i=0; i<money.length; i++) {
      stmt.executeUpdate("insert into t0019 values (" +
      i + ", " + money[i] + ",   " +
      smallmoney[i] + ")         ");
    }
    
    
    // long l = System.currentTimeMillis();
    // while (l + 500 > System.currentTimeMillis()) ;
    ResultSet rs = stmt.executeQuery("select * from t0019 order by i");
    
    for(i=0; rs.next(); i++) {
      BigDecimal  m;
      BigDecimal  sm;
      
      m = (BigDecimal)rs.getObject("mymoney");
      sm = (BigDecimal)rs.getObject("mysmallmoney");
      
      money[i].setScale(2, BigDecimal.ROUND_DOWN);
      smallmoney[i].setScale(2, BigDecimal.ROUND_DOWN);
      
      assertTrue(m.equals(money[i].setScale(2, BigDecimal.ROUND_DOWN)));
      assertTrue(sm.equals(smallmoney[i].setScale(2, BigDecimal.ROUND_DOWN)));
      
      System.out.println(m + ", " + sm);
    }
  }
  
  public void testBooleanAndCompute0026() throws Exception {
    Statement   stmt = con.createStatement();
    dropTable("t0026");
    int count = stmt.executeUpdate("create table t0026             " +
    "  (i      integer,             " +
    "   b      bit,                 " +
    "   s      char(5),             " +
    "   f      float)               ");
    System.out.println("Creating table affected " + count + " rows");
    
    stmt.executeUpdate("insert into t0026 values(0, 0, 'false', 0.0)");
    stmt.executeUpdate("insert into t0026 values(0, 0, 'N', 10)");
    stmt.executeUpdate("insert into t0026 values(1, 1, 'true', 7.0)");
    stmt.executeUpdate("insert into t0026 values(2, 1, 'Y', -5.0)");
    
    ResultSet  rs = stmt.executeQuery(
    "select * from t0026 order by i compute sum(f) by i");
    
    assertTrue(rs.next());
    
    assertTrue(!(rs.getBoolean("i")
    || rs.getBoolean("b")
    || rs.getBoolean("s")
    || rs.getBoolean("f")));
    
    assertTrue(rs.next());
    
    assertTrue(!(rs.getBoolean("i")
    || rs.getBoolean("b")
    || rs.getBoolean("s")
    || rs.getBoolean("f")));
    assertTrue(rs.next());
    
    assertTrue(rs.getBoolean("i")
    && rs.getBoolean("b")
    && rs.getBoolean("s")
    && rs.getBoolean("f"));
    assertTrue(rs.next());
    
    assertTrue(rs.getBoolean("i")
    && rs.getBoolean("b")
    && rs.getBoolean("s")
    && rs.getBoolean("f"));
    
      /* XXX compute does not work in the momeent
      ResultSet  rs = stmt.executeQuery(
         "select * from t0026 order by i compute sum(f) by i");
       
       
       
      if (!rs.next())
      {
         throw new SQLException("Failed");
      }
      passed = passed && (! (rs.getBoolean("i")
                             || rs.getBoolean("b")
                             || rs.getBoolean("s")
                             || rs.getBoolean("f")));
       
       
      if (!rs.next())
      {
         throw new SQLException("Failed");
      }
      passed = passed && (! (rs.getBoolean("i")
                             || rs.getBoolean("b")
                             || rs.getBoolean("s")
                             || rs.getBoolean("f")));
       
       
      if (!rs.next())
      {
         throw new SQLException("Failed");
      }
      passed = passed && (rs.getBoolean("i")
                          && rs.getBoolean("b")
                          && rs.getBoolean("s")
                          && rs.getBoolean("f"));
       
      if (!rs.next())
      {
         throw new SQLException("Failed");
      }
      passed = passed && (rs.getBoolean("i")
                          && rs.getBoolean("b")
                          && rs.getBoolean("s")
                          && rs.getBoolean("f"));
       
     System.out.println("\n" + (passed ? "Passed" : "Failed")
                         + " t0026.\n");
       */
    
  }
  public void testDataTypes0027() throws Exception {
    System.out.println("Test all the SQLServer datatypes in Statement\n"
    + "and PreparedStatement using the preferred getXXX()\n"
    + "instead of getObject like t0017.java does.");
    System.out.println("!!!Note- This test is not fully implemented yet!!!");
    Statement   stmt = con.createStatement();
    ResultSet   rs;
    dropTable("t0027");
    String sql =
    "create table t0027 (                                  " +
    " mybinary                   binary(5) not null,       " +
    " myvarbinary                varbinary(4) not null,    " +
    " mychar                     char(10) not null,        " +
    " myvarchar                  varchar(8) not null,      " +
    " mydatetime                 datetime not null,        " +
    " mysmalldatetime            smalldatetime not null,   " +
    " mydecimal10_3              decimal(10,3) not null,   " +
    " mynumeric5_4               numeric (5,4) not null,   " +
    " myfloat6                   float(6) not null,        " +
    " myfloat14                  float(6) not null,        " +
    " myreal                     real not null,            " +
    " myint                      int not null,             " +
    " mysmallint                 smallint not null,        " +
    " mytinyint                  tinyint not null,         " +
    " mymoney                    money not null,           " +
    " mysmallmoney               smallmoney not null,      " +
    " mybit                      bit not null,             " +
    " mytimestamp                timestamp not null,       " +
    " mytext                     text not null,            " +
    " myimage                    image not null,           " +
    " mynullbinary               binary(3) null,          " +
    " mynullvarbinary            varbinary(6) null,       " +
    " mynullchar                 char(10) null,            " +
    " mynullvarchar              varchar(40) null,         " +
    " mynulldatetime             datetime null,            " +
    " mynullsmalldatetime        smalldatetime null,       " +
    " mynulldecimal10_3          decimal(10,3) null,       " +
    " mynullnumeric15_10         numeric(15,10) null,      " +
    " mynullfloat6               float(6) null,            " +
    " mynullfloat14              float(14) null,           " +
    " mynullreal                 real null,                " +
    " mynullint                  int null,                 " +
    " mynullsmallint             smallint null,            " +
    " mynulltinyint              tinyint null,             " +
    " mynullmoney                money null,               " +
    " mynullsmallmoney           smallmoney null,          " +
    " mynulltext                 text null,                " +
    " mynullimage                image null)               ";
    
    int count = stmt.executeUpdate(sql);
    
    
    // Insert a row without nulls via a Statement
    sql =
    "insert into t0027               " +
    "  (mybinary,                    " +
    "   myvarbinary,                 " +
    "   mychar,                      " +
    "   myvarchar,                   " +
    "   mydatetime,                  " +
    "   mysmalldatetime,             " +
    "   mydecimal10_3,               " +
    "   mynumeric5_4,              " +
    "   myfloat6,                    " +
    "   myfloat14,                   " +
    "   myreal,                      " +
    "   myint,                       " +
    "   mysmallint,                  " +
    "   mytinyint,                   " +
    "   mymoney,                     " +
    "   mysmallmoney,                " +
    "   mybit,                       " +
    "   mytimestamp,                 " +
    "   mytext,                      " +
    "   myimage,                     " +
    "   mynullbinary,                " +
    "   mynullvarbinary,             " +
    "   mynullchar,                  " +
    "   mynullvarchar,               " +
    "   mynulldatetime,              " +
    "   mynullsmalldatetime,         " +
    "   mynulldecimal10_3,           " +
    "   mynullnumeric15_10,          " +
    "   mynullfloat6,                " +
    "   mynullfloat14,               " +
    "   mynullreal,                  " +
    "   mynullint,                   " +
    "   mynullsmallint,              " +
    "   mynulltinyint,               " +
    "   mynullmoney,                 " +
    "   mynullsmallmoney,            " +
    "   mynulltext,                  " +
    "   mynullimage)                 " +
    " values                         " +
    "   (0x1213141516,               " + //   mybinary,
    "    0x1718191A,                 " + //   myvarbinary
    "    '1234567890',               " + //   mychar
    "    '12345678',                 " + //   myvarchar
    "    '19991015 21:29:59.01',     " + //   mydatetime
    "    '19991015 20:45',           " + //   mysmalldatetime
    "    1234567.089,                " + //   mydecimal10_3
    "    1.2345,                     " + //   mynumeric5_4
    "    65.4321,                    " + //   myfloat6
    "    1.123456789,                " + //   myfloat14
    "    987654321.0,                " + //   myreal
    "    4097,                       " + //   myint
    "    4094,                       " + //   mysmallint
    "    200,                        " + //   mytinyint
    "    19.95,                      " + //   mymoney
    "    9.97,                       " + //   mysmallmoney
    "    1,                          " + //   mybit
    "    null,                       " + //   mytimestamp
    "    'abcdefg',                  " + //   mytext
    "    0x0AAABB,                   " + //   myimage
    "    0x123456,                   " + //   mynullbinary
    "    0xAB,                       " + //   mynullvarbinary
    "    'z',                        " + //   mynullchar
    "    'zyx',                      " + //   mynullvarchar
    "    '1976-07-04 12:00:00.04',   " + //   mynulldatetime
    "    '2000-02-29 13:46',         " + //   mynullsmalldatetime
    "     1.23,                      " + //   mynulldecimal10_3
    "     7.1234567891,              " + //   mynullnumeric15_10
    "     987654,                    " + //   mynullfloat6
    "     0,                         " + //   mynullfloat14
    "     -1.1,                      " + //   mynullreal
    "     -10,                       " + //   mynullint
    "     126,                       " + //   mynullsmallint
    "     7,                         " + //   mynulltinyint
    "     -19999.00,                 " + //   mynullmoney
    "     -9.97,                     " + //   mynullsmallmoney
    "     '1234',                    " + //   mynulltext
    "     0x1200340056)              " + //   mynullimage)
    "";
    
    count = stmt.executeUpdate(sql);
    
    sql = "select * from t0027";
    rs = stmt.executeQuery(sql);
    assertTrue(rs.next());
    System.out.println("mybinary is " + rs.getObject("mybinary"));
    System.out.println("myvarbinary is " + rs.getObject("myvarbinary"));
    System.out.println("mychar is " + rs.getString("mychar"));
    System.out.println("myvarchar is " + rs.getString("myvarchar"));
    System.out.println("mydatetime is " + rs.getTimestamp("mydatetime"));
    System.out.println("mysmalldatetime is " + rs.getTimestamp("mysmalldatetime"));
    System.out.println("mydecimal10_3 is " + rs.getObject("mydecimal10_3"));
    System.out.println("mynumeric5_4 is " + rs.getObject("mynumeric5_4"));
    System.out.println("myfloat6 is " + rs.getDouble("myfloat6"));
    System.out.println("myfloat14 is " + rs.getDouble("myfloat14"));
    System.out.println("myreal is " + rs.getDouble("myreal"));
    System.out.println("myint is " + rs.getInt("myint"));
    System.out.println("mysmallint is " + rs.getShort("mysmallint"));
    System.out.println("mytinyint is " + rs.getByte("mytinyint"));
    System.out.println("mymoney is " + rs.getObject("mymoney"));
    System.out.println("mysmallmoney is " + rs.getObject("mysmallmoney"));
    System.out.println("mybit is " + rs.getObject("mybit"));
    System.out.println("mytimestamp is " + rs.getObject("mytimestamp"));
    System.out.println("mytext is " + rs.getObject("mytext"));
    System.out.println("myimage is " + rs.getObject("myimage"));
    System.out.println("mynullbinary is " + rs.getObject("mynullbinary"));
    System.out.println("mynullvarbinary is " + rs.getObject("mynullvarbinary"));
    System.out.println("mynullchar is " + rs.getString("mynullchar"));
    System.out.println("mynullvarchar is " + rs.getString("mynullvarchar"));
    System.out.println("mynulldatetime is " + rs.getTimestamp("mynulldatetime"));
    System.out.println("mynullsmalldatetime is " + rs.getTimestamp("mynullsmalldatetime"));
    System.out.println("mynulldecimal10_3 is " + rs.getObject("mynulldecimal10_3"));
    System.out.println("mynullnumeric15_10 is " + rs.getObject("mynullnumeric15_10"));
    System.out.println("mynullfloat6 is " + rs.getDouble("mynullfloat6"));
    System.out.println("mynullfloat14 is " + rs.getDouble("mynullfloat14"));
    System.out.println("mynullreal is " + rs.getDouble("mynullreal"));
    System.out.println("mynullint is " + rs.getInt("mynullint"));
    System.out.println("mynullsmallint is " + rs.getShort("mynullsmallint"));
    System.out.println("mynulltinyint is " + rs.getByte("mynulltinyint"));
    System.out.println("mynullmoney is " + rs.getObject("mynullmoney"));
    System.out.println("mynullsmallmoney is " + rs.getObject("mynullsmallmoney"));
    System.out.println("mynulltext is " + rs.getObject("mynulltext"));
    System.out.println("mynullimage is " + rs.getObject("mynullimage"));
  }
  public void testCallStoredProcedures0028() throws Exception {
    Statement   stmt = con.createStatement();
    ResultSet   rs       = null;
    
    boolean isResultSet;
    int updateCount;
    
    int resultSetCount=0;
    int rowCount=0;
    int numberOfUpdates=0;
    
    
    isResultSet = stmt.execute("EXEC sp_who");
    System.out.println("execute(EXEC sp_who) returned: " + isResultSet);
    
    updateCount=stmt.getUpdateCount();
    
    while (isResultSet || (updateCount!=-1)) {
      if (isResultSet) {
        resultSetCount++;
        rs = stmt.getResultSet();
        
        ResultSetMetaData rsMeta =  rs.getMetaData();
        int columnCount = rsMeta.getColumnCount();
        System.out.println("columnCount: " +
        Integer.toString(columnCount));
        for(int n=1; n<= columnCount; n++) {
          System.out.println(Integer.toString(n) + ": " +
          rsMeta.getColumnName(n));
        }
        
        while(rs.next()) {
          rowCount++;
          for(int n=1; n<= columnCount; n++) {
            System.out.println(Integer.toString(n) + ": " +
            rs.getString(n));
          }
        }
        
      }
      else {
        numberOfUpdates++;
        System.out.println("UpdateCount: " +
        Integer.toString(updateCount));
      }
      isResultSet=stmt.getMoreResults();
      updateCount = stmt.getUpdateCount();
    }
    
    System.out.println("resultSetCount: " + resultSetCount);
    System.out.println("Total rowCount: " + rowCount);
    System.out.println("Number of updates: " + numberOfUpdates);
    
    
    assertTrue((rowCount>=1) && (numberOfUpdates==0) && (resultSetCount==1));
  }
  public void testxx0029() throws Exception {
    Statement   stmt = con.createStatement();
    int         i;
    int         j;
    int         count    = 0;
    ResultSet   rs       = null;
    
    boolean isResultSet;
    int updateCount;
    
    int resultSetCount=0;
    int rowCount=0;
    int numberOfUpdates=0;
    
    
    System.out.println("before execute DROP PROCEDURE");
    
    try {
      isResultSet =stmt.execute("DROP PROCEDURE t0029_p1");
      updateCount = stmt.getUpdateCount();
      do {
        System.out.println("DROP PROCEDURE isResultSet: " + isResultSet);
        System.out.println("DROP PROCEDURE updateCount: " + updateCount);
        isResultSet = stmt.getMoreResults();
        updateCount = stmt.getUpdateCount();
      } while (((updateCount!=-1) && !isResultSet) || isResultSet);
    }
    catch(SQLException e) {
    }
    
    try {
      isResultSet =stmt.execute("DROP PROCEDURE t0029_p2");
      updateCount = stmt.getUpdateCount();
      do {
        System.out.println("DROP PROCEDURE isResultSet: " + isResultSet);
        System.out.println("DROP PROCEDURE updateCount: " + updateCount);
        isResultSet = stmt.getMoreResults();
        updateCount = stmt.getUpdateCount();
      } while (((updateCount!=-1) && !isResultSet) || isResultSet);
    }
    catch(SQLException e) {
    }
    
    
    dropTable("t0029_t1");
    
    isResultSet =
    stmt.execute(
    " create table t0029_t1                       " +
    "  (t1 datetime not null,                     " +
    "   t2 datetime null,                         " +
    "   t3 smalldatetime not null,                " +
    "   t4 smalldatetime null,                    " +
    "   t5 text null)                             ");
    updateCount = stmt.getUpdateCount();
    do {
      System.out.println("CREATE TABLE isResultSet: " + isResultSet);
      System.out.println("CREATE TABLE updateCount: " + updateCount);
      isResultSet = stmt.getMoreResults();
      updateCount = stmt.getUpdateCount();
    } while (((updateCount!=-1) && !isResultSet) || isResultSet);
    
    
    isResultSet =
    stmt.execute(
    "CREATE PROCEDURE t0029_p1                    " +
    "AS                                           " +
    "                                             " +
    " insert into t0029_t1 values                " +
    " ('1999-01-07', '1998-09-09 15:35:05',       " +
    " getdate(), '1998-09-09 15:35:00', null)     " +
    "                                             " +
    " update t0029_t1 set t1='1999-01-01'         " +
    "                                             " +
    " insert into t0029_t1 values                 " +
    " ('1999-01-08', '1998-09-09 15:35:05',       " +
    " getdate(), '1998-09-09 15:35:00','456')     " +
    "                                             " +
    " update t0029_t1 set t2='1999-01-02'         " +
    "                                             " +
    " declare @ptr varbinary(16)                  " +
    " select @ptr=textptr(t5) from t0029_t1       " +
    "   where t1='1999-01-08'                     " +
    " writetext t0029_t1.t5 @ptr with log '123'   " +
    "                                             " +
    "                                             ");
    
    updateCount = stmt.getUpdateCount();
    do {
      System.out.println("CREATE PROCEDURE isResultSet: " + isResultSet);
      System.out.println("CREATE PROCEDURE updateCount: " + updateCount);
      isResultSet = stmt.getMoreResults();
      updateCount = stmt.getUpdateCount();
    } while (((updateCount!=-1) && !isResultSet) || isResultSet);
    
    
    isResultSet =
    stmt.execute(
    "CREATE PROCEDURE t0029_p2                    " +
    "AS                                           " +
    " EXEC t0029_p1                               " +
    " SELECT * FROM t0029_t1                      " +
    "                                             " +
    "                                             " +
    "                                             ");
    
    updateCount = stmt.getUpdateCount();
    do {
      System.out.println("CREATE PROCEDURE isResultSet: " + isResultSet);
      System.out.println("CREATE PROCEDURE updateCount: " + updateCount);
      isResultSet = stmt.getMoreResults();
      updateCount = stmt.getUpdateCount();
    } while (((updateCount!=-1) && !isResultSet) || isResultSet);
    
    
    isResultSet = stmt.execute( "EXEC  t0029_p2  ");
    
    System.out.println("execute(EXEC t0029_p2) returned: " + isResultSet);
    
    updateCount=stmt.getUpdateCount();
    
    while (isResultSet || (updateCount!=-1)) {
      if (isResultSet) {
        resultSetCount++;
        rs = stmt.getResultSet();
        
        ResultSetMetaData rsMeta =  rs.getMetaData();
        int columnCount = rsMeta.getColumnCount();
        System.out.println("columnCount: " +
        Integer.toString(columnCount));
        for(int n=1; n<= columnCount; n++) {
          System.out.println(Integer.toString(n) + ": " +
          rsMeta.getColumnName(n));
        }
        
        while(rs.next()) {
          rowCount++;
          for(int n=1; n<= columnCount; n++) {
            System.out.println(Integer.toString(n) + ": " +
            rs.getString(n));
          }
        }
        
      }
      else {
        numberOfUpdates++;
        System.out.println("UpdateCount: " +
        Integer.toString(updateCount));
      }
      isResultSet=stmt.getMoreResults();
      updateCount = stmt.getUpdateCount();
    }
    
    System.out.println("resultSetCount: " + resultSetCount);
    System.out.println("Total rowCount: " + rowCount);
    System.out.println("Number of updates: " + numberOfUpdates);
    
    
    assertTrue((resultSetCount==1) &&
    (rowCount==2) &&
    (numberOfUpdates==1));
  }
  
  public void testDataTypesByResultSetMetaData0030() throws Exception {
    boolean passed = true;
    Statement   stmt = con.createStatement();
    int         count    = 0;
    ResultSet   rs;
    
    
    String sql = ("select " +
    " convert(tinyint, 2),  " +
    " convert(smallint, 5)  ");
    
    rs = stmt.executeQuery(sql);
    if (!rs.next()) {
      passed = false;
    }
    else {
      ResultSetMetaData meta = rs.getMetaData();
      
      if (meta.getColumnType(1)!=java.sql.Types.TINYINT) {
        System.out.println("tinyint column was read as "
        + meta.getColumnType(1));
        passed = false;
      }
      if (meta.getColumnType(2)!=java.sql.Types.SMALLINT) {
        System.out.println("smallint column was read as "
        + meta.getColumnType(2));
        passed = false;
      }
      if (rs.getInt(1) != 2) {
        System.out.println("Bogus value read for tinyint");
        passed = false;
      }
      if (rs.getInt(2) != 5) {
        System.out.println("Bogus value read for smallint");
        passed = false;
      }
    }
    assertTrue(passed);
  }
  public void testTextColumns0031() throws Exception {
    Statement   stmt = con.createStatement();
    boolean   passed = true;
    
    
    System.out.println("Starting test t0031-  test text columns");
    
    
    int         count    = 0;
    dropTable("t0031");
    
    count = stmt.executeUpdate("create table t0031                " +
    "  (t_nullable      text null,     " +
    "   t_notnull       text not null, " +
    "   i               int not null)  ");
    System.out.println("Creating table affected " + count + " rows");
    
    stmt.executeUpdate("insert into t0031 values(null, '',   1)");
    stmt.executeUpdate("insert into t0031 values(null, 'b1', 2)");
    stmt.executeUpdate("insert into t0031 values('',   '',   3)");
    stmt.executeUpdate("insert into t0031 values('',   'b2', 4)");
    stmt.executeUpdate("insert into t0031 values('a1', '',   5)");
    stmt.executeUpdate("insert into t0031 values('a2', 'b3', 6)");
    
    
    ResultSet  rs = stmt.executeQuery("select * from t0031 " +
    " order by i ");
    
    
    
    if (!rs.next()) {
      throw new SQLException("Failed");
    }
    passed = passed && (rs.getString(1) == null);
    passed = passed && (rs.getString(2).equals(""));
    passed = passed && (rs.getInt(3) == 1);
    
    if (!rs.next()) {
      throw new SQLException("Failed");
    }
    passed = passed && (rs.getString(1) == null);
    passed = passed && (rs.getString(2).equals("b1"));
    passed = passed && (rs.getInt(3) == 2);
    
    if (!rs.next()) {
      throw new SQLException("Failed");
    }
    passed = passed && (rs.getString(1).equals(""));
    passed = passed && (rs.getString(2).equals(""));
    passed = passed && (rs.getInt(3) == 3);
    
    if (!rs.next()) {
      throw new SQLException("Failed");
    }
    passed = passed && (rs.getString(1).equals(""));
    passed = passed && (rs.getString(2).equals("b2"));
    passed = passed && (rs.getInt(3) == 4);
    
    if (!rs.next()) {
      throw new SQLException("Failed");
    }
    passed = passed && (rs.getString(1).equals("a1"));
    passed = passed && (rs.getString(2).equals(""));
    passed = passed && (rs.getInt(3) == 5);
    
    if (!rs.next()) {
      throw new SQLException("Failed");
    }
    passed = passed && (rs.getString(1).equals("a2"));
    passed = passed && (rs.getString(2).equals("b3"));
    passed = passed && (rs.getInt(3) == 6);
    
    assertTrue(passed);
  }
  
  public void testSpHelpSysUsers0032() throws Exception {
    Statement   stmt = con.createStatement();
    boolean   passed = true;
    boolean   isResultSet;
    boolean   done = false;
    int       i;
    int       updateCount = 0;
    
    
    System.out.println("Starting test t0032-  test sp_help sysusers");
    
    
    int         count    = 0;
    
    
    isResultSet = stmt.execute("sp_help sysusers");
    
    System.out.println("Executed the statement.  rc is " + isResultSet);
    
    do {
      if (isResultSet) {
        System.out.println("About to call getResultSet");
        ResultSet          rs   = stmt.getResultSet();
        ResultSetMetaData  meta = rs.getMetaData();
        updateCount = 0;
        while(rs.next()) {
          for(i=1; i<=meta.getColumnCount(); i++) {
            System.out.print(rs.getString(i) + "\t");
          }
          System.out.println("");
        }
        System.out.println("Done processing the result set");
      }
      else {
        System.out.println("About to call getUpdateCount()");
        updateCount = stmt.getUpdateCount();
        System.out.println("Updated " + updateCount + " rows");
      }
      System.out.println("About to call getMoreResults()");
      isResultSet = stmt.getMoreResults();
      done = !isResultSet && updateCount==-1;
    } while (!done);
    
    assertTrue(passed);
  }
  static String longString(char ch) {
    int                 i;
    String              str255 = "";
    
    for(i=0; i<255; i++) {
      str255 = str255 + ch;
    }
    return str255;
  }
  public void testExceptionByUpdate0033() throws Exception {
    boolean passed;
    Statement   stmt = con.createStatement();
    System.out.println("Starting test t0033-  make sure Statement.executeUpdate() throws excpetion");
    
    try {
      passed = false;
      stmt.executeUpdate("I am sure this is an error");
    }
    catch (SQLException e) {
      System.out.println("The execption is " + e.getMessage());
      passed = true;
    }
    assertTrue(passed);
    
  }
  
  public void testInsertConflict0049() throws Exception {
    Statement   stmt = con.createStatement();
    
    
    int         i;
    int         count    = 0;
    dropTable("t0049a");
    dropTable("t0049b");
    
    String query =
    "create table t0049a(                    " +
    "  a integer identity(1,1) primary key,  " +
    "  b char    not null)";
    
    count = stmt.executeUpdate(query);
    System.out.println("Creating table affected " + count + " rows");
    
    
    query =
    "create table t0049b(                    " +
    "  a integer not null,                   " +
    "  c char    not null,                   " +
    "  foreign key (a) references t0049a(a)) ";
    count = stmt.executeUpdate(query);
    System.out.println("Creating table affected " + count + " rows");
    
    
    query = "insert into t0049b (a, c) values (?, ?)";
    java.sql.PreparedStatement pstmt = con.prepareStatement(query);
    
    try {
      pstmt.setInt(1, 1);
      pstmt.setString(2, "a");
      count = pstmt.executeUpdate();
    }
    catch(SQLException e) {
      if (! (e.getMessage().startsWith("INSERT statement conflicted"))) {
        throw e;
      }
    }
    
    query = "insert into t0049a (b) values ('a')";
    count = stmt.executeUpdate(query);
    System.out.println("insert affected " + count + " rows");
    
    pstmt.setInt(1, 1);
    pstmt.setString(2, "a");
    count = pstmt.executeUpdate();
    
    
  }
  
  public void testxx0050() throws Exception {
    Statement   stmt = con.createStatement();
    boolean passed = true;
    int         i;
    int         count    = 0;
    
    try {
      stmt.executeUpdate("drop procedure p0050");
    }
    catch (SQLException e) {
      if (! (e.getMessage().startsWith("Cannot drop the procedure 'p0050', because it does"))) {
        throw e;
      }
    }
    dropTable("t0050b");
    dropTable("t0050a");
    
    String query =
    "create table t0050a(                    " +
    "  a integer identity(1,1) primary key,  " +
    "  b char    not null)";
    
    count = stmt.executeUpdate(query);
    System.out.println("Creating table affected " + count + " rows");
    
    
    query =
    "create table t0050b(                    " +
    "  a integer not null,                   " +
    "  c char    not null,                   " +
    "  foreign key (a) references t0050a(a)) ";
    count = stmt.executeUpdate(query);
    System.out.println("Creating table affected " + count + " rows");
    
    query =
    "create procedure p0050 (@a integer, @c char) as " +
    "   insert into t0050b (a, c) values (@a, @c)    ";
    count = stmt.executeUpdate(query);
    System.out.println("Creating procedure affected " + count + " rows");
    
    
    query = "exec p0050 ?, ?";
    java.sql.PreparedStatement pstmt = con.prepareStatement(query);
    
    try {
      pstmt.setInt(1, 1);
      pstmt.setString(2, "a");
      count = pstmt.executeUpdate();
    }
    catch(SQLException e) {
      if (! (e.getMessage().startsWith("INSERT statement conflicted"))) {
        throw e;
      }
    }
    
    query = "insert into t0050a (b) values ('a')";
    count = stmt.executeUpdate(query);
    System.out.println("insert affected " + count + " rows");
    
    pstmt.setInt(1, 1);
    pstmt.setString(2, "a");
    count = pstmt.executeUpdate();
    assertTrue(passed);
  }
  
  public void testxx0051() throws Exception {
    boolean passed = true;
    int         i;
    int         count    = 0;
    Statement   stmt     = con.createStatement();
    
    try {
      String           types[] = {"TABLE"};
      DatabaseMetaData dbMetaData = con.getMetaData( );
      ResultSet        rs         = dbMetaData.getTables( null, "%", "t%", types);
      
      while(rs.next()) {
        System.out.println("Table " + rs.getString(3));
        System.out.println("  catalog " + rs.getString(1));
        System.out.println("  schema  " + rs.getString(2));
        System.out.println("  name    " + rs.getString(3));
        System.out.println("  type    " + rs.getString(4));
        System.out.println("  remarks " + rs.getString(5));
      }
    }
    catch(java.sql.SQLException e) {
      passed = false;
      System.out.println("Exception caught.  " + e.getMessage());
      e.printStackTrace();
    }
    assertTrue(passed);
  }
  public void testxx0055() throws Exception {
    boolean passed = true;
    int         i;
    int         count    = 0;
    Statement   stmt     = con.createStatement();
    
    try {
      String           expectedNames[] = {
        "TABLE_CAT",
        "TABLE_SCHEM",
        "TABLE_NAME",
        "TABLE_TYPE",
        "REMARKS"
      };
      String           types[] = {"TABLE"};
      DatabaseMetaData dbMetaData = con.getMetaData();
      ResultSet        rs         = dbMetaData.getTables( null, "%", "t%", types);
      ResultSetMetaData rsMetaData = rs.getMetaData();
      
      if (rsMetaData.getColumnCount() != 5) {
        if (passed) {
          passed = false;
          System.out.println("Bad column count.  Should be 5, was "
          + rsMetaData.getColumnCount());
        }
      }
      
      for(i=0; passed && i<expectedNames.length; i++) {
        if (! rsMetaData.getColumnName(i+1).equals(expectedNames[i])) {
          passed = false;
          System.out.println("Bad name for column " + (i+1) + ".  "
          + "Was " + rsMetaData.getColumnName(i+1)
          + ", expected "
          + expectedNames[i]);
        }
      }
    }
    catch(java.sql.SQLException e) {
      passed = false;
      System.out.println("Exception caught.  " + e.getMessage());
      e.printStackTrace();
    }
    assertTrue(passed);
  }
  
  public void testxx0052() throws Exception {
    boolean passed = true;
    
    // ugly, I know
    byte[] image = {
      (byte)0x47, (byte)0x49, (byte)0x46, (byte)0x38,
      (byte)0x39, (byte)0x61, (byte)0x0A, (byte)0x00,
      (byte)0x0A, (byte)0x00, (byte)0x80, (byte)0xFF,
      (byte)0x00, (byte)0xD7, (byte)0x3D, (byte)0x1B,
      (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x2C,
      (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00,
      (byte)0x0A, (byte)0x00, (byte)0x0A, (byte)0x00,
      (byte)0x00, (byte)0x02, (byte)0x08, (byte)0x84,
      (byte)0x8F, (byte)0xA9, (byte)0xCB, (byte)0xED,
      (byte)0x0F, (byte)0x63, (byte)0x2B, (byte)0x00,
      (byte)0x3B,
    };
    
    int         i;
    int         count    = 0;
    Statement   stmt     = con.createStatement();
    
    dropTable("t0052");
    
    try {
      String sql =
      "create table t0052 (                                  " +
      " myvarchar                varchar(2000) not null,     " +
      " myvarbinary              varbinary(2000) not null)   ";
      
      stmt.executeUpdate(sql);
      
      sql =
      "insert into t0052               " +
      "  (myvarchar,                   " +
      "   myvarbinary)                 " +
      " values                         " +
      "  (\'This is a test with german umlauts ‰ˆ¸\', " +
      "   0x4749463839610A000A0080FF00D73D1B0000002C000000000A000A00000208848FA9CBED0F632B003B" +
      "  )";
      stmt.executeUpdate(sql);
      
      sql = "select * from t0052";
      ResultSet rs = stmt.executeQuery(sql);
      if (!rs.next()) {
        passed = false;
      }
      else {
        System.out.println("Testing getAsciiStream()");
        InputStream in = rs.getAsciiStream("myvarchar");
        String expect = "This is a test with german umlauts ???";
        byte[] toRead = new byte[expect.length()];
        count = in.read(toRead);
        if (count == expect.length()) {
          for (i=0; i<expect.length(); i++) {
            if (expect.charAt(i) != toRead[i]) {
              passed = false;
              System.out.println("Expected "+expect.charAt(i)
              + " but was "
              + toRead[i]);
            }
          }
        } else {
          passed = false;
          System.out.println("Premature end in "
          + "getAsciiStream(\"myvarchar\") "
          + count + " instead of "
          + expect.length());
        }
        in.close();
        
        in = rs.getAsciiStream(2);
        toRead = new byte[41];
        count = in.read(toRead);
        if (count == 41) {
          for (i=0; i<41; i++) {
            if (toRead[i] != (toRead[i] & 0x7F)) {
              passed = false;
              System.out.println("Non ASCII characters in getAsciiStream");
              break;
            }
          }
        } else {
          passed = false;
          System.out.println("Premature end in getAsciiStream(1) "
          +count+" instead of 41");
        }
        in.close();
        
        System.out.println("Testing getUnicodeStream()");
        in = rs.getUnicodeStream("myvarchar");
        Reader reader = new InputStreamReader(in, "UTF8");
        expect = "This is a test with german umlauts ‰ˆ¸";
        char[] charsToRead = new char[expect.length()];
        count = reader.read(charsToRead, 0, expect.length());
        if (count == expect.length()) {
          String result = new String(charsToRead);
          if (!expect.equals(result)) {
            passed = false;
            System.out.println("Expected "+ expect
            + " but was " + result);
          }
        } else {
          passed = false;
          System.out.println("Premature end in "
          + "getUnicodeStream(\"myvarchar\") "
          + count + " instead of "
          + expect.length());
        }
        reader.close();
        
                /* Cannot think of a meaningfull test */
        in = rs.getUnicodeStream(2);
        in.close();
        
        System.out.println("Testing getBinaryStream()");
        
                /* Cannot think of a meaningfull test */
        in = rs.getBinaryStream("myvarchar");
        in.close();
        
        in = rs.getBinaryStream(2);
        count = 0;
        toRead = new byte[image.length];
        do {
          int actuallyRead = in.read(toRead, count,
          image.length-count);
          if (actuallyRead == -1) {
            passed = false;
            System.out.println("Premature end in "
            +" getBinaryStream(2) "
            + count +" instead of "
            + image.length);
            break;
          }
          count += actuallyRead;
        } while (count < image.length);
        
        for (i=0; i<count; i++) {
          if (toRead[i] != image[i]) {
            passed = false;
            System.out.println("Expected "+toRead[i]
            + "but was "+image[i]);
            break;
          }
        }
        in.close();
        
        System.out.println("Testing getCharacterStream()");
        try {
          Method getCharacterStreamString =
          ResultSet.class.getMethod("getCharacterStream",
          new Class[] {String.class});
          Method getCharacterStreamInt =
          ResultSet.class.getMethod("getCharacterStream",
          new Class[] {Integer.TYPE});
          
          reader =
          (Reader)getCharacterStreamString.invoke(rs,
          new Object[] {"myvarchar"});
          expect = "This is a test with german umlauts ‰ˆ¸";
          charsToRead = new char[expect.length()];
          count = reader.read(charsToRead, 0, expect.length());
          if (count == expect.length()) {
            String result = new String(charsToRead);
            if (!expect.equals(result)) {
              passed = false;
              System.out.println("Expected "+ expect
              + " but was " + result);
            }
          } else {
            passed = false;
            System.out.println("Premature end in "
            + "getCharacterStream(\"myvarchar\") "
            + count + " instead of "
            + expect.length());
          }
          reader.close();
          
                    /* Cannot think of a meaningfull test */
          reader =
          (Reader)getCharacterStreamInt.invoke(rs,
          new Object[] {new Integer(2)});
          reader.close();
        } catch (NoSuchMethodException e) {
          System.out.println("JDBC 2 only");
        } catch (Throwable t) {
          passed = false;
          System.out.println("Exception: "+t.getMessage());
        }
      }
      rs.close();
      
    }
    catch(java.sql.SQLException e) {
      passed = false;
      System.out.println("Exception caught.  " + e.getMessage());
      e.printStackTrace();
    }
    assertTrue(passed);
  }
  public void testxx0053() throws Exception {
    boolean passed = true;
    
    int         count    = 0;
    Statement   stmt     = con.createStatement();
    
    dropTable("t0053");
    try  {
      String sql =
      "create table t0053 (                                  " +
      " myvarchar                varchar(2000)  not null,    " +
      " mynchar                  nchar(2000)    not null,    " +
      " mynvarchar               nvarchar(2000) not null,    " +
      " myntext                  ntext          not null     " +
      " )   ";
      
      stmt.executeUpdate(sql);
      
      sql =
      "insert into t0053               " +
      "  (myvarchar,                   " +
      "   mynchar,                     " +
      "   mynvarchar,                  " +
      "   myntext)                     " +
      " values                         " +
      "  (\'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\',     " +
      "   \'‰ˆ¸ƒ÷‹\',                  " +
      "   \'‰ˆ¸ƒ÷‹\',                  " +
      "   \'‰ˆ¸ƒ÷‹\'                   " +
      "  )";
      stmt.executeUpdate(sql);
      
      sql = "select * from t0053";
      ResultSet rs = stmt.executeQuery(sql);
      if (!rs.next()) {
        passed = false;
      }
      else {
        System.err.print("Testing varchars > 255 chars: ");
        String test = rs.getString(1);
        if (test.length() == 270) {
          System.err.println("passed");
        } else {
          System.err.println("failed");
          passed = false;
        }
        
        System.err.print("Testing nchar: ");
        test = rs.getString(2);
        if (test.length() == 2000 && "‰ˆ¸ƒ÷‹".equals(test.trim())) {
          System.err.println("passed");
        } else {
          System.err.print("failed, got \'");
          System.err.print(test.trim());
          System.err.println("\' instead of \'‰ˆ¸ƒ÷‹\'");
          passed = false;
        }
        
        System.err.print("Testing nvarchar: ");
        test = rs.getString(3);
        if (test.length() == 6 && "‰ˆ¸ƒ÷‹".equals(test)) {
          System.err.println("passed");
        } else {
          System.err.print("failed, got \'");
          System.err.print(test);
          System.err.println("\' instead of \'‰ˆ¸ƒ÷‹\'");
          passed = false;
        }
        
        System.err.print("Testing ntext: ");
        test = rs.getString(4);
        if (test.length() == 6 && "‰ˆ¸ƒ÷‹".equals(test)) {
          System.err.println("passed");
        } else {
          System.err.print("failed, got \'");
          System.err.print(test);
          System.err.println("\' instead of \'‰ˆ¸ƒ÷‹\'");
          passed = false;
        }
      }
    }
    catch(java.sql.SQLException e) {
      passed = false;
      System.out.println("Exception caught.  " + e.getMessage());
      e.printStackTrace();
    }
    assertTrue(passed);
  }
  public void testxx005x() throws Exception 
  {
    boolean    passed = true;

    System.out.println("test getting a DECIMAL as a long from the database.");

    int         count    = 0;
    Statement   stmt     = con.createStatement();

    ResultSet  rs;

    rs = stmt.executeQuery("select convert(DECIMAL(4,0), 0)");
    if (!rs.next())
    {
       passed = false;
    }
    else
    {
       long l = rs.getLong(1);
       if (l != 0) 
       {
           passed = false;
       }
    }

    rs = stmt.executeQuery("select convert(DECIMAL(4,0), 1)");
    if (!rs.next())
    {
       passed = false;
    }
    else
    {
       long l = rs.getLong(1);
       if (l != 1) 
       {
           passed = false;
       }
    }

    rs = stmt.executeQuery("select convert(DECIMAL(4,0), -1)");
    if (!rs.next())
    {
       passed = false;
    }
    else
    {
       long l = rs.getLong(1);
       if (l != -1) 
       {
           passed = false;
       }
    }
    assertTrue(passed);
  }
  
  public void testxx0057() throws Exception {
    boolean    passed = true;

    System.out.println("test putting a zero length string into a parameter");

    // open the database

    int         count    = 0;
    Statement   stmt     = con.createStatement();

    dropTable("t0057");

    count = stmt.executeUpdate("create table t0057          "
                               + " (a varchar(10) not null, "
                               + "  b char(10)    not null) ");
    System.out.println("Creating table affected " + count + " rows");

    PreparedStatement  pStmt = con.prepareStatement( 
       "insert into t0057 values (?, ?)");      
    pStmt.setString(1, "");
    pStmt.setString(2, "");
    count = pStmt.executeUpdate();
    System.out.println("Added " + count + " rows");
    if (count != 1) 
    {
       System.out.println("Failed to add rows");
       passed = false;
    }
    else
    {
       pStmt = con.prepareStatement("select a, b from t0057");

       ResultSet  rs = pStmt.executeQuery();
       if (!rs.next())
       {
          passed = false;
          System.out.println("Couldn't read rows from table.");
       }
       else
       {
          System.out.println("a is |" + rs.getString("a") + "|");
          System.out.println("b is |" + rs.getString("b") + "|");
          passed = passed && (rs.getString("a").equals(""));
          passed = passed && (rs.getString("b").equals("          "));
       }
    }
    assertTrue(passed);
      
  }
  public void testxx0059() throws Exception 
  {
    boolean passed = true;


    int         i;
    int         count    = 0;
    Statement   stmt     = con.createStatement();

    try
    {
       DatabaseMetaData  dbMetaData = con.getMetaData( );
       ResultSet         rs         = dbMetaData.getSchemas();
       ResultSetMetaData rsm        = rs.getMetaData();

       passed = passed && rsm.getColumnCount()==1;
       passed = passed && rsm.getColumnName(1).equals("TABLE_SCHEM");

       while(rs.next())
       {
          System.out.println("schema " + rs.getString(1));
       }
    }
    catch(java.sql.SQLException e)
    {
       passed = false;
       System.out.println("Exception caught.  " + e.getMessage());
       e.printStackTrace();
    }
    assertTrue(passed);
  }
  
  
}
