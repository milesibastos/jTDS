//
// Copyright 1998 CDS Networks, Inc., Medford Oregon
//
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
// 1. Redistributions of source code must retain the above copyright
//    notice, this list of conditions and the following disclaimer.
// 2. Redistributions in binary form must reproduce the above copyright
//    notice, this list of conditions and the following disclaimer in the
//    documentation and/or other materials provided with the distribution.
// 3. All advertising materials mentioning features or use of this software
//    must display the following acknowledgement:
//      This product includes software developed by CDS Networks, Inc.
// 4. The name of CDS Networks, Inc.  may not be used to endorse or promote
//    products derived from this software without specific prior
//    written permission.
//
// THIS SOFTWARE IS PROVIDED BY CDS NETWORKS, INC. ``AS IS'' AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED.  IN NO EVENT SHALL CDS NETWORKS, INC. BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
// OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
// HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
// LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
// OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
// SUCH DAMAGE.
//


package com.internetcds.jdbc.tds;

import java.sql.*;


/**
 * A ResultSetMetaData object can be used to find out about the types
 * and properties of the columns in a ResultSet.
 *
 * @author Craig Spannring
 * @version $Id: TdsResultSetMetaData.java,v 1.9 2002-09-19 23:27:38 alin_sinpalean Exp $
 */
public class TdsResultSetMetaData implements java.sql.ResultSetMetaData
{
   public static final String cvsVersion = "$Id: TdsResultSetMetaData.java,v 1.9 2002-09-19 23:27:38 alin_sinpalean Exp $";

   /**
    * Does not allow NULL values.
    */
   public static final int columnNoNulls = 0;

   /**
    * Allows NULL values.
    */
   public static final int columnNullable = 1;

   /**
    * Nullability unknown.
    */
   public static final int columnNullableUnknown = 2;

   private Columns  columnsInfo;


   public TdsResultSetMetaData(Columns columns_)
   {
      columnsInfo = columns_;
   }

   /**
    * What's a column's table's catalog name?
    *
    * @param column the first column is 1, the second is 2, ...
    * @return column name or "" if not applicable.
    * @exception SQLException if a database-access error occurs.
    */
   public String getCatalogName(int column) throws SQLException
   {
      String res = columnsInfo.getCatalog(column);
      return res==null ? "" : res;
   }

   /**
    * What's the number of columns in the ResultSet?
    *
    * @return the number
    * @exception SQLException if a database-access error occurs.
    */
   public int getColumnCount() throws SQLException
   {
      return columnsInfo.fakeColumnCount();
   }

   /**
    * What's the column's normal max width in chars?
    *
    * @param column the first column is 1, the second is 2, ...
    * @return max width
    * @exception SQLException if a database-access error occurs.
    */
   public int getColumnDisplaySize(int column) throws SQLException
   {
      return columnsInfo.getDisplaySize(column);
   }

   /**
    * What's the suggested column title for use in printouts and
    * displays?
    *
    * @param column the first column is 1, the second is 2, ...
    * @return true if so
    * @exception SQLException if a database-access error occurs.
    */
   public String getColumnLabel(int column) throws SQLException
   {
      return columnsInfo.getLabel(column);
   }

   /**
    * What's a column's name?
    *
    * @param column the first column is 1, the second is 2, ...
    * @return column name
    * @exception SQLException if a database-access error occurs.
    */
   public String getColumnName(int column) throws SQLException
   {
      return columnsInfo.getName(column);
   }

   /**
    * What's a column's SQL type?
    *
    * @param column the first column is 1, the second is 2, ...
    * @return SQL type
    * @exception SQLException if a database-access error occurs.
    */
   public int getColumnType(int column) throws SQLException
   {
      switch( columnsInfo.getNativeType(column) )
      {
         case Tds.SYBNCHAR:
         case Tds.SYBNTEXT:
         case Tds.SYBNVARCHAR:
         case Tds.SYBUNIQUEID:
            return Types.OTHER;
         default:
            return columnsInfo.getJdbcType(column);
      }
   }

   /**
    * What's a column's data source specific type name?
    *
    * @param column the first column is 1, the second is 2, ...
    * @return type name
    * @exception SQLException if a database-access error occurs.
    */
   public String getColumnTypeName(int column) throws SQLException
   {
      if( columnsInfo.isAutoIncrement(column).booleanValue() )
         return getCleanTypeName(column)+" identity";
      return getCleanTypeName(column);
   }

   /**
    * Get the native data type name (i.e. without 'identity').
    */
   private String getCleanTypeName(int column) throws SQLException
   {
      switch (columnsInfo.getNativeType(column))
      {
         case Tds.SYBVOID: return "void";
         case Tds.SYBIMAGE: return "image";
         case Tds.SYBTEXT: return "text";
         case Tds.SYBUNIQUEID: return "uniqueidentifier";
         case Tds.SYBVARBINARY: return "varbinary";
         case Tds.SYBVARCHAR: return "varchar";
         case Tds.SYBBINARY: return "binary";
         case Tds.SYBCHAR: return "char";
         case Tds.SYBINT1: return "tinyint";
         case Tds.SYBBIT: case Tds.SYBBITN: return "bit";
         case Tds.SYBINT2: return "smallint";
         case Tds.SYBINT4: return "int";
         case Tds.SYBDATETIME4: return "smalldatetime";
         case Tds.SYBREAL: return "real";
         case Tds.SYBMONEY: return "money";
         case Tds.SYBDATETIME: return "datetime";
         case Tds.SYBFLT8: return "float";
         case Tds.SYBDECIMAL: return "decimal";
         case Tds.SYBNUMERIC: return "numeric";
         case Tds.SYBMONEY4: return "smallmoney";
         case Tds.SYBNCHAR: return "nchar";
         case Tds.SYBNTEXT: return "ntext";
         case Tds.SYBNVARCHAR: return "nvarchar";
         case Tds.SYBSMALLMONEY: return "smallmoney";
         case Tds.SYBINTN:
         {
            switch( columnsInfo.getBufferSize(column) )
            {
                case 1: return "tinyint";
                case 2: return "smallint";
                case 4: return "int";
            }
            break;
         }
         case Tds.SYBFLTN:
         {
            switch( columnsInfo.getBufferSize(column) )
            {
                case 4: return "real";
                case 8: return "float";
            }
            break;
         }
         case Tds.SYBMONEYN:
         {
            switch( columnsInfo.getBufferSize(column) )
            {
                case 4: return "smallmoney";
                case 8: return "money";
            }
            break;
         }
         case Tds.SYBDATETIMN:
         {
            switch( columnsInfo.getBufferSize(column) )
            {
                case 4: return "smalldatetime";
                case 8: return "datetime";
            }
            break;
         }
      }

      throw new SQLException("Unknown native type for column "+column+": "
         +columnsInfo.getNativeType(column));
   }

   /**
    * What's a column's number of decimal digits?
    *
    * @param column the first column is 1, the second is 2, ...
    * @return precision
    * @exception SQLException if a database-access error occurs.
    */
   public int getPrecision(int column) throws SQLException
   {
      return columnsInfo.getPrecision(column);
   }

   /**
    * What's a column's number of digits to right of the decimal point?
    *
    * @param column the first column is 1, the second is 2, ...
    * @return scale
    * @exception SQLException if a database-access error occurs.
    */
   public int getScale(int column) throws SQLException
   {
      int res = columnsInfo.getScale(column);
      return res<0 ? 0 : res;
   }

   /**
    * What's a column's table's schema?
    *
    * @param column the first column is 1, the second is 2, ...
    * @return schema name or "" if not applicable
    * @exception SQLException if a database-access error occurs.
    */
   public String getSchemaName(int column) throws SQLException
   {
      String res = columnsInfo.getSchema(column);
      return res==null ? "" : res;
   }

   /**
    * What's a column's table name?
    *
    * @return table name or "" if not applicable
    * @exception SQLException if a database-access error occurs.
    */
   public String getTableName(int column) throws SQLException
   {
      String res = columnsInfo.getTableName(column);
      return res==null ? "" : res;
   }

   /**
    * Is the column automatically numbered, thus read-only?
    *
    * @param column the first column is 1, the second is 2, ...
    * @return true if so
    * @exception SQLException if a database-access error occurs.
    */
   public boolean isAutoIncrement(int column) throws SQLException
   {
      return columnsInfo.isAutoIncrement(column).booleanValue();
   }

   /**
    * Does a column's case matter?
    *
    * @param column the first column is 1, the second is 2, ...
    * @return true if so
    * @exception SQLException if a database-access error occurs.
    */
   public boolean isCaseSensitive(int column) throws SQLException
   {
      return columnsInfo.isCaseSensitive(column).booleanValue();
   }

   /**
    * Is the column a cash value?
    *
    * @param column the first column is 1, the second is 2, ...
    * @return true if so
    * @exception SQLException if a database-access error occurs.
    */
   public boolean isCurrency(int column) throws SQLException
   {
      switch (columnsInfo.getNativeType(column))
      {
         case Tds.SYBMONEY:
         case Tds.SYBMONEYN:
         case Tds.SYBMONEY4:
         case Tds.SYBSMALLMONEY:
         {
            return true;
         }
         default:
         {
            return false;
         }
      }
   }

   /**
    * Will a write on the column definitely succeed?
    *
    * @param column the first column is 1, the second is 2, ...
    * @return true if so
    * @exception SQLException if a database-access error occurs.
    */
   public boolean isDefinitelyWritable(int column) throws SQLException
   {
      return false;
   }

   /**
    * Can you put a NULL in this column?
    *
    * @param column the first column is 1, the second is 2, ...
    * @return columnNoNulls, columnNullable or columnNullableUnknown
    * @exception SQLException if a database-access error occurs.
    */
   public int isNullable(int column) throws SQLException
   {
      return columnsInfo.isNullable(column);
   }

   /**
    * Is a column definitely not writable?
    *
    * @param column the first column is 1, the second is 2, ...
    * @return true if so
    * @exception SQLException if a database-access error occurs.
    */
   public boolean isReadOnly(int column) throws SQLException
   {
      return columnsInfo.isReadOnly(column).booleanValue();
   }

   /**
    * Can the column be used in a where clause?
    *
    * @param column the first column is 1, the second is 2, ...
    * @return true if so
    * @exception SQLException if a database-access error occurs.
    */
   public boolean isSearchable(int column) throws SQLException
   {
      return columnsInfo.getNativeType(column) != Tds.SYBIMAGE;
   }

   /**
    * Is the column a signed number?
    *
    * @param column the first column is 1, the second is 2, ...
    * @return true if so
    * @exception SQLException if a database-access error occurs.
    */
   public boolean isSigned(int column) throws SQLException
   {
      switch( columnsInfo.getNativeType(column) )
      {
         case Tds.SYBDECIMAL:
         case Tds.SYBNUMERIC:
         case Tds.SYBMONEYN:
         case Tds.SYBMONEY:
         case Tds.SYBMONEY4:
         case Tds.SYBSMALLMONEY:
         case Tds.SYBFLTN:
         case Tds.SYBFLT8:
         case Tds.SYBREAL:
         case Tds.SYBINT2:
         case Tds.SYBINT4:
            return true;

         case Tds.SYBBIT:
         case Tds.SYBBITN:
         case Tds.SYBNVARCHAR:
         case Tds.SYBVARCHAR:
         case Tds.SYBNCHAR:
         case Tds.SYBCHAR:
         case Tds.SYBBINARY:
         case Tds.SYBVARBINARY:
         case Tds.SYBDATETIMN:
         case Tds.SYBDATETIME:
         case Tds.SYBDATETIME4:
         case Tds.SYBUNIQUEID:
         case Tds.SYBINT1:
         case Tds.SYBIMAGE:
         case Tds.SYBTEXT:
         case Tds.SYBNTEXT:
            return false;

         case Tds.SYBINTN:
            return columnsInfo.getBufferSize(column) > 1;

         default:
            throw new SQLException("Unknown column type.");
      }
   }

   /**
    * Is it possible for a write on the column to succeed?
    *
    * @param column the first column is 1, the second is 2, ...
    * @return true if so
    * @exception SQLException if a database-access error occurs.
    */
   public boolean isWritable(int column) throws SQLException
   {
      return !columnsInfo.isReadOnly(column).booleanValue();
   }

   /**
    * JDBC 2.0
    *
    * <p>Returns the fully-qualified name of the Java class whose instances
    * are manufactured if the method <code>ResultSet.getObject</code>
    * is called to retrieve a value
    * from the column.  <code>ResultSet.getObject</code> may return a subclass of the
    * class returned by this method.
    *
    * @return the fully-qualified name of the class in the Java programming
    *         language that would be used by the method
    * <code>ResultSet.getObject</code> to retrieve the value in the specified
    * column. This is the class name used for custom mapping.
    * @exception SQLException if a database access error occurs
    */
   public String getColumnClassName(int column) throws SQLException
   {
      switch( columnsInfo.getJdbcType(column) )
      {
         case Types.BIT:
            return "java.lang.Boolean";

         case Types.TINYINT:
         case Types.SMALLINT:
         case Types.INTEGER:
            return "java.lang.Integer";

         case Types.BIGINT:
         case Types.NUMERIC:
         case Types.DECIMAL:
            return "java.math.BigDecimal";

         case Types.FLOAT:
         case Types.DOUBLE:
            return "java.lang.Double";

         case Types.REAL:
            return "java.lang.Float";

         case Types.CHAR:
         case Types.VARCHAR:
         case Types.LONGVARCHAR:
            return "java.lang.String";

         case Types.DATE:
         case Types.TIME:
         case Types.TIMESTAMP:
            return "java.sql.Timestamp";

         case Types.BINARY:
         case Types.VARBINARY:
         case Types.LONGVARBINARY:
            return "byte[]";

         case Types.JAVA_OBJECT:
         case Types.DISTINCT:
         case Types.STRUCT:
         case Types.ARRAY:
         case Types.BLOB:
         case Types.CLOB:
         case Types.REF:
         default:
            // SAfe Or should this be null?
            return "";
      }
   }
}
