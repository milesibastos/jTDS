package net.sourceforge.jtds.test;

import java.sql.*;
import java.util.Map;
import java.math.BigDecimal;

public class DatabaseTestCase extends TestBase
{
	protected static Map typemap = null;
	
	public DatabaseTestCase(String name)
	{
		super(name);
	}

	
	protected void dropTable(String tablename) throws SQLException
	{
          String sobName = "sysobjects";
          if (tablename.startsWith("#"))
            sobName = "tempdb.dbo.sysobjects";
          Statement stmt = con.createStatement();
          stmt.executeUpdate(
                  "if exists (select * from " + sobName + " where name like '" + tablename + "%' and type = 'U') "
                  + " drop table " + tablename);
          stmt.close();
	}
	
	protected void dropProcedure(String procname) throws SQLException
	{
          String sobName = "sysobjects";
          if (procname.startsWith("#"))
            sobName = "tempdb.dbo.sysobjects";
          Statement stmt = con.createStatement();
          stmt.executeUpdate(
                  "if exists (select * from " + sobName + " where name like '" + procname + "%' and type = 'P') "
                  + " drop procedure " + procname);
          stmt.close();
	}
	
	// return -1 if a1<a2, 0 if a1==a2, 1 if a1>a2
	static int compareBytes(byte a1[], byte a2[])
	{
		if (a1 == a2)
		{
			return 0;
		}
		if (a1 == null && a2 != null)
		{
			return -1;
		}
		if (a1 != null && a2 == null)
		{
			return 1;
		}

		int  length = (a1.length < a2.length ? a1.length : a2.length);
		for (int i = 0; i < length; i++)
		{
			if (a1[i] != a2[i])
			{
				return ((a1[i] & 0xff) > (a2[i] & 0xff) ? 1 : -1);
			}
		}
		if (a1.length == a2.length)
		{
			return 0;
		}
		if (a1.length < a2.length)
		{
			return -1;
		}
		return 1;
	}
	
	protected static Map getTypemap()
	{
		if (typemap != null)
			return typemap;

		Map map = new java.util.HashMap(15);
		map.put(BigDecimal.class,         new Integer(java.sql.Types.DECIMAL));
		map.put(Boolean.class,            new Integer(java.sql.Types.BIT));
		map.put(Byte.class,               new Integer(java.sql.Types.TINYINT));
		map.put(byte[].class,             new Integer(java.sql.Types.VARBINARY));
		map.put(java.sql.Date.class,      new Integer(java.sql.Types.DATE));
		map.put(double.class,             new Integer(java.sql.Types.DOUBLE));
		map.put(Double.class,             new Integer(java.sql.Types.DOUBLE));
		map.put(float.class,              new Integer(java.sql.Types.REAL));
		map.put(Float.class,              new Integer(java.sql.Types.REAL));
		map.put(Integer.class,            new Integer(java.sql.Types.INTEGER));
		map.put(Long.class,               new Integer(java.sql.Types.NUMERIC));
		map.put(Short.class,              new Integer(java.sql.Types.SMALLINT));
		map.put(String.class,             new Integer(java.sql.Types.VARCHAR));
		map.put(java.sql.Timestamp.class, new Integer(java.sql.Types.TIMESTAMP));

		typemap = map;
		return typemap;
	}

	protected static int getType(Object o) throws SQLException
	{
		if (o == null)
		{
			throw new SQLException("You must specify a type for a null parameter");
		}

		Map map = getTypemap();
		Object ot = map.get(o.getClass());
		if (ot == null)
		{
			throw new SQLException("Support for this type is not implemented");
		}
		return ((Integer)ot).intValue();
	}
	
	protected String getLongString(int length)
	{
		String  result = "";
		for (int i = 0; i < length; i++)
		{
			result = result + "a";
		}
		return result;
	}
	
	protected String getLongString(char ch) 
	{
		String str255 = "";
		for (int i = 0; i < 255; i++)
		{
			str255 = str255 + ch;
		}
		return str255;
	}	
}
