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

/**
 *
 * @author  builder
 * @version 
 */
public class CSUnitTest extends DatabaseTestCase {

  public CSUnitTest(String name)
  {
          super(name);
  }

  public static void main(String args[]) 
  {
    try {
          Logger.setActive(true);
    }
    catch (java.io.IOException ex) {
      throw new RuntimeException("Unexpected Exception " + ex + " occured in main");
    }
    junit.textui.TestRunner.run(CSUnitTest.class);
  }
  
  /*
  public void testMaxRows0003() throws Exception
  {
    dropTable("t0003");
    Statement stmt = con.createStatement();
    
    int count = stmt.executeUpdate("create table t0003              "
                                 + "  (i  integer not null)       ");
    PreparedStatement  pStmt = con.prepareStatement(
       "insert into t0003 values (?)");

    final int rowsToAdd = 100;
    count = 0;
    for(int i=1; i<=rowsToAdd; i++)
    {
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
    while(rs.next())
    {
       int   n = rs.getInt("i");
       count++;
    }
    assertTrue (count == rowLimit);    
  }

  public void testGetAsciiStream0018() throws Exception
  {
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
       if (!rs.next())
       {
          assertTrue("should get Result",false);
       }
       else
       {
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
  
   */
  public void testMoneyHandling0019() throws Exception
  {
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

      if (smallmoney.length != money.length)
      {
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
      
      for(i=0; i<money.length; i++)
      {
         stmt.executeUpdate("insert into t0019 values (" +
                            i + ", " + money[i] + ",   " +
                            smallmoney[i] + ")         ");
      }
      

      ResultSet rs = stmt.executeQuery("select * from t0019 order by i");
       
      for(i=0; rs.next(); i++)
      {
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
    
  
}
