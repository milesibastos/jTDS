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
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
// Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
//

package net.sourceforge.jtds.jdbc;

import junit.framework.Test;
import junit.framework.TestSuite;

import java.io.UnsupportedEncodingException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

/**
 * Test case to illustrate use of TDS 9 support
 */
public class Tds9Test extends TestBase
{

   public static Test suite()
   {
      String tds = props.getProperty( Messages.get( Driver.TDS ) );

      if( tds == null || Double.valueOf( tds ) >= Double.valueOf( DefaultProperties.TDS_VERSION_90 ) )
      {
         return new TestSuite( Tds9Test.class );
      }

      return new TestSuite();
   }

   public Tds9Test( String name )
   {
      super( name );
   }

   /**
    * Test UDT.
    * To run this test you will need to compile and install the following
    * C# class into the target server:
    *
    * namespace UDT
    * {
    * using System;
    * using System.Data.SqlTypes;
    * using Microsoft.SqlServer.Server;
    * using System.Runtime.InteropServices;
    * [SqlUserDefinedType(Format.Native, IsByteOrdered = true)]
    * [Serializable]
    * [StructLayout(LayoutKind.Sequential)]
    *
    * public struct JtdsUDT : INullable
    * {
    *     int x;
    *     private bool valueSet;
    *
    *    public override string ToString()
    *     {
    *         return (valueSet) ? String.Format("{0:d}", x) : "null";
    *     }
    *
    *     public bool IsNull
    *     {
    *         get { return !valueSet; }
    *     }
    *
    *     public static JtdsUDT Null
    *     {
    *         get { return new JtdsUDT(); }
    *     }
    *
    *     public static JtdsUDT Parse(SqlString s)
    *     {
    *         if (s.IsNull || s.Value.ToLower() == "null")
    *         {
    *             return Null;
    *         }
    *         JtdsUDT t = new JtdsUDT();
    *         t.x = int.Parse(s.ToString());
    *         t.valueSet = true;
    *         return t;
    *     }
    *
    * }
    * }
    *
    * Use the following TSQL to install the type:
    *  create assembly JtdsUDT from 'C:\JtdsUDT.dll'
    *  go
    *  create type JtdsUDT external name JtdsUDT.[UDT.JtdsUDT]
    *  go
    *
    */
   public void testReadUDT() throws Exception
   {
      if( supportsTDS9() )
      {
         try
         {
            dropTable( "jtds_UDT" );
            Statement stmt = con.createStatement();
            try
            {
               stmt.execute( "CREATE TABLE jtds_UDT (id int primary key, u JtdsUDT)" );
            }
            catch( SQLException e )
            {
               if( e.getMessage().indexOf( "Cannot find data type JtdsUDT" ) > 0 )
               {
                  System.err.println( "User Data Type JtdsUDT not installed" );
                  return;
               }
               throw e;
            }
            assertEquals( 1, stmt.executeUpdate( "INSERT INTO jtds_UDT VALUES (1, null)" ) );
            assertEquals( 1, stmt.executeUpdate( "INSERT INTO jtds_UDT VALUES (2, '511')" ) );
            ResultSet rs = stmt.executeQuery( "SELECT * FROM jtds_UDT ORDER BY id ASC" );
            assertNotNull( rs );
            ResultSetMetaData rsmd = rs.getMetaData();
            assertFalse( "isAutoIncrement", rsmd.isAutoIncrement( 2 ) );
            assertFalse( "isCaseSensitive", rsmd.isCaseSensitive( 2 ) );
            assertFalse( "isCurrency", rsmd.isCurrency( 2 ) );
            assertFalse( "isDefinitelyWritable", rsmd.isDefinitelyWritable( 2 ) );
            assertFalse( "isReadOnly", rsmd.isReadOnly( 2 ) );
            assertFalse( "isSearchable", rsmd.isSearchable( 2 ) );
            assertFalse( "isSigned", rsmd.isSigned( 2 ) );
            assertTrue( "isWritable", rsmd.isWritable( 2 ) );
            assertEquals( "getCatalogName", "", rsmd.getCatalogName( 2 ) );
            assertEquals( "getColumnClassName", "[B", rsmd.getColumnClassName( 2 ) );
            assertEquals( "getColumnDisplaySize", 16000, rsmd.getColumnDisplaySize( 2 ) );
            assertEquals( "getColumnLabel", "u", rsmd.getColumnLabel( 2 ) );
            assertEquals( "getColumnName", "u", rsmd.getColumnName( 2 ) );
            assertEquals( "getColumnType", Types.VARBINARY, rsmd.getColumnType( 2 ) );
            assertEquals( "getColumnTypeName", "JtdsUDT", rsmd.getColumnTypeName( 2 ) );
            assertEquals( "getPrecision", 8000, rsmd.getPrecision( 2 ) );
            assertEquals( "getScale", 0, rsmd.getScale( 2 ) );
            assertEquals( "getSchemaName", "", rsmd.getSchemaName( 2 ) );
            assertEquals( "getTableName", "", rsmd.getTableName( 2 ) );
            assertTrue( rs.next() );
            assertNull( rs.getBytes( 2 ) );
            assertTrue( rs.next() );
            assertEquals( "800001FF01", rs.getString( 2 ) );
            assertFalse( rs.next() );
            rs.close();
            stmt.close();
         }
         finally
         {
            dropTable( "jtds_UDT" );
         }
      }
   }

   /**
    *
    */
   public void testXMLFunction() throws Exception
   {
      if( supportsTDS9() )
      {
         try
         {
            dropTable( "jtds_UDT" );
            dropFunction( "f_xmlret" );
            Statement stmt = con.createStatement();
            stmt.execute( "CREATE TABLE jtds_UDT (id int primary key, u JtdsUDT)" );
            stmt.executeUpdate( "INSERT INTO jtds_UDT VALUES(1, '511')" );
            stmt.execute( "CREATE FUNCTION f_xmlret(@data xml) RETURNS xml AS\r\n" + "BEGIN\r\n" + "RETURN (SELECT u FROM jtds_UDT WHERE id = 1)\r\n" + "END" );
            stmt.close();
            CallableStatement cstmt = con.prepareCall( "{?=call f_xmlret(?)}" );
            cstmt.registerOutParameter( 1, java.sql.Types.VARCHAR );
            String xml = "<body>test</body>";
            cstmt.setString( 2, xml );
            cstmt.execute();
            assertEquals( xml, cstmt.getString( 1 ) );
            cstmt.close();
         }
         finally
         {
            dropFunction( "f_xmlret" );
            dropTable( "jtds_UDT" );
         }
      }
   }

   public void testReadXML() throws Exception
   {
      if( supportsTDS9() )
      {
         Statement stmt = con.createStatement();
         stmt.execute( "CREATE TABLE #TEST (id int primary key, x xml)" );
         assertEquals( 1, stmt.executeUpdate( "INSERT INTO #TEST VALUES (1, null)" ) );
         assertEquals( 1, stmt.executeUpdate( "INSERT INTO #TEST VALUES (2, '<body>short xml</body>')" ) );
         StringBuffer buf = new StringBuffer( 9000 );
         buf.append( "INSERT INTO #TEST VALUES (3, '<body>" );
         for( int i = 0; i < 8000; i++ )
         {
            buf.append( 'X' );
         }
         buf.append( "</body>')" );
         assertEquals( 1, stmt.executeUpdate( buf.toString() ) );
         ResultSet rs = stmt.executeQuery( "SELECT * FROM #TEST ORDER BY id ASC" );
         assertNotNull( rs );
         ResultSetMetaData rsmd = rs.getMetaData();
         assertFalse( "isAutoIncrement", rsmd.isAutoIncrement( 2 ) );
         assertTrue( "isCaseSensitive", rsmd.isCaseSensitive( 2 ) );
         assertFalse( "isCurrency", rsmd.isCurrency( 2 ) );
         assertFalse( "isDefinitelyWritable", rsmd.isDefinitelyWritable( 2 ) );
         assertFalse( "isReadOnly", rsmd.isReadOnly( 2 ) );
         assertFalse( "isSearchable", rsmd.isSearchable( 2 ) );
         assertFalse( "isSigned", rsmd.isSigned( 2 ) );
         assertTrue( "isWritable", rsmd.isWritable( 2 ) );
         assertEquals( "getCatalogName", "", rsmd.getCatalogName( 2 ) );
         assertEquals( "getColumnClassName", "java.sql.Clob", rsmd.getColumnClassName( 2 ) );
         assertEquals( "getColumnDisplaySize", 1073741823, rsmd.getColumnDisplaySize( 2 ) );
         assertEquals( "getColumnLabel", "x", rsmd.getColumnLabel( 2 ) );
         assertEquals( "getColumnName", "x", rsmd.getColumnName( 2 ) );
         assertEquals( "getColumnType", Types.CLOB, rsmd.getColumnType( 2 ) );
         assertEquals( "getColumnTypeName", "xml", rsmd.getColumnTypeName( 2 ) );
         assertEquals( "getPrecision", 1073741823, rsmd.getPrecision( 2 ) );
         assertEquals( "getScale", 0, rsmd.getScale( 2 ) );
         assertEquals( "getSchemaName", "", rsmd.getSchemaName( 2 ) );
         assertEquals( "getTableName", "", rsmd.getTableName( 2 ) );
         assertTrue( rs.next() );
         assertNull( rs.getString( 2 ) );
         assertTrue( rs.next() );
         assertEquals( "<body>short xml</body>", rs.getString( 2 ) );
         assertTrue( rs.next() );
         assertEquals( buf.toString().substring( 30, 8043 ), rs.getString( 2 ) );
         assertFalse( rs.next() );
         rs.close();
         stmt.close();
      }
   }

   public void testReadVarcharMax() throws Exception
   {
      if( supportsTDS9() )
      {
         Statement stmt = con.createStatement();
         stmt.execute( "CREATE TABLE #TEST (id int primary key, s varchar(max))" );
         assertEquals( 1, stmt.executeUpdate( "INSERT INTO #TEST VALUES (1, null)" ) );
         assertEquals( 1, stmt.executeUpdate( "INSERT INTO #TEST VALUES (2, 'short string')" ) );
         StringBuffer buf = new StringBuffer( 9000 );
         buf.append( "INSERT INTO #TEST VALUES (3, '>" );
         for( int i = 0; i < 8000; i++ )
         {
            buf.append( 'X' );
         }
         buf.append( "<')" );
         assertEquals( 1, stmt.executeUpdate( buf.toString() ) );
         ResultSet rs = stmt.executeQuery( "SELECT * FROM #TEST ORDER BY id ASC" );
         assertNotNull( rs );
         ResultSetMetaData rsmd = rs.getMetaData();
         assertFalse( "isAutoIncrement", rsmd.isAutoIncrement( 2 ) );
         assertFalse( "isCaseSensitive", rsmd.isCaseSensitive( 2 ) );
         assertFalse( "isCurrency", rsmd.isCurrency( 2 ) );
         assertFalse( "isDefinitelyWritable", rsmd.isDefinitelyWritable( 2 ) );
         assertFalse( "isReadOnly", rsmd.isReadOnly( 2 ) );
         assertTrue( "isSearchable", rsmd.isSearchable( 2 ) );
         assertFalse( "isSigned", rsmd.isSigned( 2 ) );
         assertTrue( "isWritable", rsmd.isWritable( 2 ) );
         assertEquals( "getCatalogName", "", rsmd.getCatalogName( 2 ) );
         assertEquals( "getColumnClassName", "java.sql.Clob", rsmd.getColumnClassName( 2 ) );
         assertEquals( "getColumnDisplaySize", 2147483647, rsmd.getColumnDisplaySize( 2 ) );
         assertEquals( "getColumnLabel", "s", rsmd.getColumnLabel( 2 ) );
         assertEquals( "getColumnName", "s", rsmd.getColumnName( 2 ) );
         assertEquals( "getColumnType", Types.CLOB, rsmd.getColumnType( 2 ) );
         assertEquals( "getColumnTypeName", "varchar", rsmd.getColumnTypeName( 2 ) );
         assertEquals( "getPrecision", 2147483647, rsmd.getPrecision( 2 ) );
         assertEquals( "getScale", 0, rsmd.getScale( 2 ) );
         assertEquals( "getSchemaName", "", rsmd.getSchemaName( 2 ) );
         assertEquals( "getTableName", "", rsmd.getTableName( 2 ) );
         assertTrue( rs.next() );
         assertNull( rs.getString( 2 ) );
         assertTrue( rs.next() );
         assertEquals( "short string", rs.getString( 2 ) );
         assertTrue( rs.next() );
         assertEquals( buf.toString().substring( 30, 8032 ), rs.getString( 2 ) );
         assertFalse( rs.next() );
         rs.close();
         stmt.close();
      }
   }

   public void testReadNvarcharMax() throws Exception
   {
      if( supportsTDS9() )
      {
         Statement stmt = con.createStatement();
         stmt.execute( "CREATE TABLE #TEST (id int primary key, s nvarchar(max))" );
         assertEquals( 1, stmt.executeUpdate( "INSERT INTO #TEST VALUES (1, null)" ) );
         assertEquals( 1, stmt.executeUpdate( "INSERT INTO #TEST VALUES (2, N'short string')" ) );
         StringBuffer buf = new StringBuffer( 9000 );
         buf.append( "INSERT INTO #TEST VALUES (3, N'>" );
         for( int i = 0; i < 8000; i++ )
         {
            buf.append( 'X' );
         }
         buf.append( "<')" );
         assertEquals( 1, stmt.executeUpdate( buf.toString() ) );
         ResultSet rs = stmt.executeQuery( "SELECT * FROM #TEST ORDER BY id ASC" );
         assertNotNull( rs );
         ResultSetMetaData rsmd = rs.getMetaData();
         assertFalse( "isAutoIncrement", rsmd.isAutoIncrement( 2 ) );
         assertFalse( "isCaseSensitive", rsmd.isCaseSensitive( 2 ) );
         assertFalse( "isCurrency", rsmd.isCurrency( 2 ) );
         assertFalse( "isDefinitelyWritable", rsmd.isDefinitelyWritable( 2 ) );
         assertFalse( "isReadOnly", rsmd.isReadOnly( 2 ) );
         assertTrue( "isSearchable", rsmd.isSearchable( 2 ) );
         assertFalse( "isSigned", rsmd.isSigned( 2 ) );
         assertTrue( "isWritable", rsmd.isWritable( 2 ) );
         assertEquals( "getCatalogName", "", rsmd.getCatalogName( 2 ) );
         assertEquals( "getColumnClassName", "java.sql.Clob", rsmd.getColumnClassName( 2 ) );
         assertEquals( "getColumnDisplaySize", 1073741823, rsmd.getColumnDisplaySize( 2 ) );
         assertEquals( "getColumnLabel", "s", rsmd.getColumnLabel( 2 ) );
         assertEquals( "getColumnName", "s", rsmd.getColumnName( 2 ) );
         assertEquals( "getColumnType", Types.CLOB, rsmd.getColumnType( 2 ) );
         assertEquals( "getColumnTypeName", "nvarchar", rsmd.getColumnTypeName( 2 ) );
         assertEquals( "getPrecision", 1073741823, rsmd.getPrecision( 2 ) );
         assertEquals( "getScale", 0, rsmd.getScale( 2 ) );
         assertEquals( "getSchemaName", "", rsmd.getSchemaName( 2 ) );
         assertEquals( "getTableName", "", rsmd.getTableName( 2 ) );
         assertTrue( rs.next() );
         assertNull( rs.getString( 2 ) );
         assertTrue( rs.next() );
         assertEquals( "short string", rs.getString( 2 ) );
         assertTrue( rs.next() );
         assertEquals( buf.toString().substring( 31, 8033 ), rs.getString( 2 ) );
         assertFalse( rs.next() );
         rs.close();
         stmt.close();
      }
   }

   public void testReadVarbinaryMax() throws Exception
   {
      if( supportsTDS9() )
      {
         Statement stmt = con.createStatement();
         stmt.execute( "CREATE TABLE #TEST (id int primary key, s varbinary(max))" );
         assertEquals( 1, stmt.executeUpdate( "INSERT INTO #TEST VALUES (1, null)" ) );
         assertEquals( 1, stmt.executeUpdate( "INSERT INTO #TEST VALUES (2, 0x41424344)" ) );
         StringBuffer buf = new StringBuffer( 18000 );
         buf.append( "INSERT INTO #TEST VALUES (3, 0x41" );
         for( int i = 0; i < 8000; i++ )
         {
            buf.append( "58" );
         }
         buf.append( "41)" );
         assertEquals( 1, stmt.executeUpdate( buf.toString() ) );
         ResultSet rs = stmt.executeQuery( "SELECT * FROM #TEST ORDER BY id ASC" );
         assertNotNull( rs );
         ResultSetMetaData rsmd = rs.getMetaData();
         assertFalse( "isAutoIncrement", rsmd.isAutoIncrement( 2 ) );
         assertFalse( "isCaseSensitive", rsmd.isCaseSensitive( 2 ) );
         assertFalse( "isCurrency", rsmd.isCurrency( 2 ) );
         assertFalse( "isDefinitelyWritable", rsmd.isDefinitelyWritable( 2 ) );
         assertFalse( "isReadOnly", rsmd.isReadOnly( 2 ) );
         assertTrue( "isSearchable", rsmd.isSearchable( 2 ) );
         assertFalse( "isSigned", rsmd.isSigned( 2 ) );
         assertTrue( "isWritable", rsmd.isWritable( 2 ) );
         assertEquals( "getCatalogName", "", rsmd.getCatalogName( 2 ) );
         assertEquals( "getColumnClassName", "java.sql.Blob", rsmd.getColumnClassName( 2 ) );
         assertEquals( "getColumnDisplaySize", 2147483647, rsmd.getColumnDisplaySize( 2 ) );
         assertEquals( "getColumnLabel", "s", rsmd.getColumnLabel( 2 ) );
         assertEquals( "getColumnName", "s", rsmd.getColumnName( 2 ) );
         assertEquals( "getColumnType", Types.BLOB, rsmd.getColumnType( 2 ) );
         assertEquals( "getColumnTypeName", "varbinary", rsmd.getColumnTypeName( 2 ) );
         assertEquals( "getPrecision", 2147483647, rsmd.getPrecision( 2 ) );
         assertEquals( "getScale", 0, rsmd.getScale( 2 ) );
         assertEquals( "getSchemaName", "", rsmd.getSchemaName( 2 ) );
         assertEquals( "getTableName", "", rsmd.getTableName( 2 ) );
         assertTrue( rs.next() );
         assertNull( rs.getString( 2 ) );
         assertTrue( rs.next() );
         assertEquals( "41424344", rs.getString( 2 ) );
         assertTrue( rs.next() );
         assertEquals( buf.toString().substring( 31, 16035 ), rs.getString( 2 ) );
         assertFalse( rs.next() );
         rs.close();
         stmt.close();
      }
   }

   public void testWriteVarcharMax() throws Exception
   {
      if( supportsTDS9() )
      {
         Statement stmt = con.createStatement();
         stmt.execute( "CREATE TABLE #TEST (id int primary key, s varchar(max))" );
         PreparedStatement pstmt = con.prepareStatement( "INSERT INTO #TEST VALUES (?,?)" );
         pstmt.setInt( 1, 1 );
         pstmt.setNull( 2, Types.VARCHAR );
         assertEquals( 1, pstmt.executeUpdate() );
         pstmt.setInt( 1, 2 );
         pstmt.setString( 2, "Short String" );
         assertEquals( 1, pstmt.executeUpdate() );
         StringBuffer buf = new StringBuffer( 9000 );
         buf.append( ">" );
         for( int i = 0; i < 8000; i++ )
         {
            buf.append( "X" );
         }
         buf.append( "<" );
         pstmt.setInt( 1, 3 );
         pstmt.setString( 2, buf.toString() );
         assertEquals( 1, pstmt.executeUpdate() );
         pstmt.close();
         ResultSet rs = stmt.executeQuery( "SELECT * FROM #TEST ORDER BY id ASC" );
         assertNotNull( rs );
         assertTrue( rs.next() );
         assertNull( rs.getString( 2 ) );
         assertTrue( rs.next() );
         assertEquals( "Short String", rs.getString( 2 ) );
         assertTrue( rs.next() );
         assertEquals( buf.toString(), rs.getString( 2 ) );
         assertFalse( rs.next() );
         rs.close();
         stmt.close();
      }
   }

   public void testWriteNvarcharMax() throws Exception
   {
      if( supportsTDS9() )
      {
         Statement stmt = con.createStatement();
         stmt.execute( "CREATE TABLE #TEST (id int primary key, s nvarchar(max))" );
         PreparedStatement pstmt = con.prepareStatement( "INSERT INTO #TEST VALUES (?,?)" );
         pstmt.setInt( 1, 1 );
         pstmt.setNull( 2, Types.VARCHAR );
         assertEquals( 1, pstmt.executeUpdate() );
         pstmt.setInt( 1, 2 );
         pstmt.setString( 2, "Short String" );
         assertEquals( 1, pstmt.executeUpdate() );
         StringBuffer buf = new StringBuffer( 9000 );
         buf.append( ">" );
         for( int i = 0; i < 4000; i++ )
         {
            buf.append( "X" );
         }
         buf.append( "<" );
         pstmt.setInt( 1, 3 );
         pstmt.setString( 2, buf.toString() );
         assertEquals( 1, pstmt.executeUpdate() );
         pstmt.close();
         ResultSet rs = stmt.executeQuery( "SELECT * FROM #TEST ORDER BY id ASC" );
         assertNotNull( rs );
         assertTrue( rs.next() );
         assertNull( rs.getString( 2 ) );
         assertTrue( rs.next() );
         assertEquals( "Short String", rs.getString( 2 ) );
         assertTrue( rs.next() );
         assertEquals( buf.toString(), rs.getString( 2 ) );
         assertFalse( rs.next() );
         rs.close();
         stmt.close();
      }
   }

   public void testWriteVarbinaryMax() throws Exception
   {
      if( supportsTDS9() )
      {
         Statement stmt = con.createStatement();
         stmt.execute( "CREATE TABLE #TEST (id int primary key, s varbinary(max))" );
         PreparedStatement pstmt = con.prepareStatement( "INSERT INTO #TEST VALUES (?,?)" );
         pstmt.setInt( 1, 1 );
         pstmt.setNull( 2, Types.VARBINARY );
         assertEquals( 1, pstmt.executeUpdate() );
         pstmt.setInt( 1, 2 );
         pstmt.setBytes( 2, new byte[] { 0x41, 0x42, 0x43, 0x44 } );
         assertEquals( 1, pstmt.executeUpdate() );
         StringBuffer buf = new StringBuffer( 9000 );
         buf.append( ">" );
         for( int i = 0; i < 8000; i++ )
         {
            buf.append( "X" );
         }
         buf.append( "<" );
         pstmt.setInt( 1, 3 );
         pstmt.setBytes( 2, buf.toString().getBytes() );
         assertEquals( 1, pstmt.executeUpdate() );
         pstmt.close();
         ResultSet rs = stmt.executeQuery( "SELECT * FROM #TEST ORDER BY id ASC" );
         assertNotNull( rs );
         assertTrue( rs.next() );
         assertNull( rs.getBytes( 2 ) );
         assertTrue( rs.next() );
         assertEquals( "ABCD", new String( rs.getBytes( 2 ) ) );
         assertTrue( rs.next() );
         assertTrue( buf.toString().equals( new String( rs.getBytes( 2 ) ) ) );
         assertFalse( rs.next() );
         rs.close();
         stmt.close();
      }
   }

   public void testWriteXML() throws Exception
   {
      if( supportsTDS9() )
      {
         Statement stmt = con.createStatement();
         stmt.execute( "CREATE TABLE #TEST (id int primary key, s xml)" );
         PreparedStatement pstmt = con.prepareStatement( "INSERT INTO #TEST VALUES (?,?)" );
         pstmt.setInt( 1, 1 );
         pstmt.setNull( 2, Types.VARCHAR );
         assertEquals( 1, pstmt.executeUpdate() );
         pstmt.setInt( 1, 2 );
         pstmt.setString( 2, "<body>Short text</body>" );
         assertEquals( 1, pstmt.executeUpdate() );
         StringBuffer buf = new StringBuffer( 9000 );
         buf.append( "<body>" );
         for( int i = 0; i < 4000; i++ )
         {
            buf.append( "X" );
         }
         buf.append( "</body>" );
         pstmt.setInt( 1, 3 );
         pstmt.setString( 2, buf.toString() );
         assertEquals( 1, pstmt.executeUpdate() );
         pstmt.setInt( 1, 4 );
         byte[] data = null;
         try
         {
            data = buf.toString().getBytes( "UTF-16LE" );
         }
         catch( UnsupportedEncodingException e )
         {
            // Will never happen
         }
         byte[] data2 = new byte[data.length + 2];
         data2[0] = (byte) 0xFF;
         data2[1] = (byte) 0xFE;
         System.arraycopy( data, 0, data2, 2, data.length );
         pstmt.setBytes( 2, data2 );
         assertEquals( 1, pstmt.executeUpdate() );
         pstmt.close();
         ResultSet rs = stmt.executeQuery( "SELECT * FROM #TEST ORDER BY id ASC" );
         assertNotNull( rs );
         assertTrue( rs.next() );
         assertNull( rs.getString( 2 ) );
         assertTrue( rs.next() );
         assertEquals( "<body>Short text</body>", rs.getString( 2 ) );
         assertTrue( rs.next() );
         assertTrue( buf.toString().equals( rs.getString( 2 ) ) );
         assertTrue( rs.next() );
         assertTrue( buf.toString().equals( rs.getString( 2 ) ) );
         assertFalse( rs.next() );
         rs.close();
         stmt.close();
      }
   }

   public void testWriteText() throws Exception
   {
      if( supportsTDS9() )
      {
         Statement stmt = con.createStatement();
         stmt.execute( "CREATE TABLE #TEST (id int primary key, s text)" );
         PreparedStatement pstmt = con.prepareStatement( "INSERT INTO #TEST VALUES (?,?)" );
         pstmt.setInt( 1, 1 );
         pstmt.setNull( 2, Types.VARCHAR );
         assertEquals( 1, pstmt.executeUpdate() );
         pstmt.setInt( 1, 2 );
         pstmt.setString( 2, "Short String" );
         assertEquals( 1, pstmt.executeUpdate() );
         StringBuffer buf = new StringBuffer( 9000 );
         buf.append( ">" );
         for( int i = 0; i < 8000; i++ )
         {
            buf.append( "X" );
         }
         buf.append( "<" );
         pstmt.setInt( 1, 3 );
         pstmt.setString( 2, buf.toString() );
         assertEquals( 1, pstmt.executeUpdate() );
         pstmt.close();
         ResultSet rs = stmt.executeQuery( "SELECT * FROM #TEST ORDER BY id ASC" );
         assertNotNull( rs );
         assertTrue( rs.next() );
         assertNull( rs.getString( 2 ) );
         assertTrue( rs.next() );
         assertEquals( "Short String", rs.getString( 2 ) );
         assertTrue( rs.next() );
         assertEquals( buf.toString(), rs.getString( 2 ) );
         assertFalse( rs.next() );
         rs.close();
         stmt.close();
      }
   }

   public void testWriteNtext() throws Exception
   {
      if( supportsTDS9() )
      {
         Statement stmt = con.createStatement();
         stmt.execute( "CREATE TABLE #TEST (id int primary key, s ntext)" );
         PreparedStatement pstmt = con.prepareStatement( "INSERT INTO #TEST VALUES (?,?)" );
         pstmt.setInt( 1, 1 );
         pstmt.setNull( 2, Types.VARCHAR );
         assertEquals( 1, pstmt.executeUpdate() );
         pstmt.setInt( 1, 2 );
         pstmt.setString( 2, "Short String" );
         assertEquals( 1, pstmt.executeUpdate() );
         StringBuffer buf = new StringBuffer( 9000 );
         buf.append( ">" );
         for( int i = 0; i < 4000; i++ )
         {
            buf.append( "X" );
         }
         buf.append( "<" );
         pstmt.setInt( 1, 3 );
         pstmt.setString( 2, buf.toString() );
         assertEquals( 1, pstmt.executeUpdate() );
         pstmt.close();
         ResultSet rs = stmt.executeQuery( "SELECT * FROM #TEST ORDER BY id ASC" );
         assertNotNull( rs );
         assertTrue( rs.next() );
         assertNull( rs.getString( 2 ) );
         assertTrue( rs.next() );
         assertEquals( "Short String", rs.getString( 2 ) );
         assertTrue( rs.next() );
         assertEquals( buf.toString(), rs.getString( 2 ) );
         assertFalse( rs.next() );
         rs.close();
         stmt.close();
      }
   }

   public void testImage() throws Exception
   {
      if( supportsTDS9() )
      {
         Statement stmt = con.createStatement();
         stmt.execute( "CREATE TABLE #TEST (id int primary key, s image)" );
         PreparedStatement pstmt = con.prepareStatement( "INSERT INTO #TEST VALUES (?,?)" );
         pstmt.setInt( 1, 1 );
         pstmt.setNull( 2, Types.VARBINARY );
         assertEquals( 1, pstmt.executeUpdate() );
         pstmt.setInt( 1, 2 );
         pstmt.setBytes( 2, new byte[] { 0x41, 0x42, 0x43, 0x44 } );
         assertEquals( 1, pstmt.executeUpdate() );
         StringBuffer buf = new StringBuffer( 9000 );
         buf.append( ">" );
         for( int i = 0; i < 8000; i++ )
         {
            buf.append( "X" );
         }
         buf.append( "<" );
         pstmt.setInt( 1, 3 );
         pstmt.setBytes( 2, buf.toString().getBytes() );
         assertEquals( 1, pstmt.executeUpdate() );
         pstmt.close();
         ResultSet rs = stmt.executeQuery( "SELECT * FROM #TEST ORDER BY id ASC" );
         assertNotNull( rs );
         assertTrue( rs.next() );
         assertNull( rs.getBytes( 2 ) );
         assertTrue( rs.next() );
         assertEquals( "ABCD", new String( rs.getBytes( 2 ) ) );
         assertTrue( rs.next() );
         assertTrue( buf.toString().equals( new String( rs.getBytes( 2 ) ) ) );
         assertFalse( rs.next() );
         rs.close();
         stmt.close();
      }
   }

   /**
    * SQL 2005 allows varchar(max) as the output parameter of a stored
    * procedure. Test this functionality now.
    */
   public void testVarcharMaxOutput() throws Exception
   {
      if( supportsTDS9() )
      {
         props.put( "SENDSTRINGPARAMETERSASUNICODE", "false" );
         Connection conn = getConnection( props );
         Statement stmt = conn.createStatement();
         stmt.execute( "CREATE PROC #sp_test @in varchar(max), @out varchar(max) output as set @out = @in" );
         StringBuffer buf = new StringBuffer( 9000 );
         buf.append( '<' );
         for( int i = 0; i < 8000; i++ )
         {
            buf.append( 'X' );
         }
         buf.append( '>' );
         CallableStatement cstmt = conn.prepareCall( "{call #sp_test(?,?)}" );
         cstmt.setString( 1, buf.toString() );
         cstmt.registerOutParameter( 2, Types.LONGVARCHAR );
         cstmt.execute();
         assertTrue( buf.toString().equals( cstmt.getString( 2 ) ) );
         cstmt.close();
         stmt.close();
      }
   }

   /**
    * SQL 2005 allows nvarchar(max) as the output parameter of a stored
    * procedure. Test this functionality now.
    */
   public void testNvarcharMaxOutput() throws Exception
   {
      if( supportsTDS9() )
      {
         Statement stmt = con.createStatement();
         stmt.execute( "CREATE PROC #sp_test @in nvarchar(max), @out nvarchar(max) output as set @out = @in" );
         StringBuffer buf = new StringBuffer( 5000 );
         buf.append( '<' );
         for( int i = 0; i < 4000; i++ )
         {
            buf.append( 'X' );
         }
         buf.append( '>' );
         CallableStatement cstmt = con.prepareCall( "{call #sp_test(?,?)}" );
         cstmt.setString( 1, buf.toString() );
         cstmt.registerOutParameter( 2, Types.LONGVARCHAR );
         cstmt.execute();
         assertTrue( buf.toString().equals( cstmt.getString( 2 ) ) );
         cstmt.close();
         stmt.close();
      }
   }

   /**
    * SQL 2005 allows varbinary(max) as the output parameter of a stored
    * procedure. Test this functionality now.
    */
   public void testVarbinaryMaxOutput() throws Exception
   {
      if( supportsTDS9() )
      {
         Statement stmt = con.createStatement();
         stmt.execute( "CREATE PROC #sp_test @in varbinary(max), @out varbinary(max) output as set @out = @in" );
         StringBuffer buf = new StringBuffer( 5000 );
         buf.append( '<' );
         for( int i = 0; i < 8000; i++ )
         {
            buf.append( 'X' );
         }
         buf.append( '>' );
         CallableStatement cstmt = con.prepareCall( "{call #sp_test(?,?)}" );
         cstmt.setBytes( 1, buf.toString().getBytes() );
         cstmt.registerOutParameter( 2, Types.LONGVARBINARY );
         cstmt.execute();
         assertTrue( buf.toString().equals( new String( cstmt.getBytes( 2 ) ) ) );
         cstmt.close();
         stmt.close();
      }
   }

   /**
    * SQL 2005 allows varbinary(max) as the output parameter of a stored
    * procedure. As xml columns will not be converted to varchar or nvarchar
    * automatically the driver must supply an xml output parameter. JDBC 4
    * includes a new Types constant called SQLXML internal value 2009. In
    * anticipation of JDBC 4 this driver now supports using this constant to set
    * the correct type of output parameter.
    */
   public void testXMLout() throws Exception
   {
      if( supportsTDS9() )
      {
         Statement stmt = con.createStatement();
         stmt.execute( "CREATE PROC #sp_test @in xml, @out xml output AS set @out = @in" );
         CallableStatement cstmt = con.prepareCall( "{call #sp_test(?, ?)}" );
         String xml = "<body>simple xml test</body>";
         cstmt.setString( 1, xml );
         // cstmt.registerOutParameter(2, JtdsCallableStatement.SQLXML);
         cstmt.registerOutParameter( 2, Types.SQLXML );
         cstmt.execute();
         assertEquals( xml, cstmt.getString( 2 ) );
      }
   }

   /**/
   public static void main( String[] args )
   {
      junit.textui.TestRunner.run( Tds9Test.class );
   }


   private boolean supportsTDS9()
      throws NumberFormatException, SQLException
   {
      return con.getMetaData().getDatabaseProductName().toLowerCase().contains( "microsoft" ) && Integer.parseInt( con.getMetaData().getDatabaseProductVersion().split( "\\." )[0] ) >= 9;
   }

}