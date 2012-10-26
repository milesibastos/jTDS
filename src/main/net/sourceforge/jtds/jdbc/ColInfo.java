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

package net.sourceforge.jtds.jdbc;

import java.util.Arrays;

/**
 * <p> Instances of this class serve as descriptor for result set columns. </p>
 *
 * @author
 *    Mike Hutchinson, Holger Rehn
 */
public class ColInfo implements Cloneable
{

   /////////////////////////////////////////////////////////////////////////////
   // public instance fields
   /////////////////////////////////////////////////////////////////////////////

   /**
    * Internal TDS data type
    */
   int         tdsType;

   /**
    * JDBC type constant from java.sql.Types
    */
   int         jdbcType;

   /**
    * Column actual table name
    */
   String      realName;

   /**
    * Column label / name
    */
   String      name;

   /**
    * Table name owning this column
    */
   String      tableName;

   /**
    * Database owning this column
    */
   String      catalog;

   /**
    * User owning this column
    */
   String      schema;

   /**
    * Column data type supports SQL NULL
    */
   int         nullable;

   /**
    * Column name is case sensitive
    */
   boolean     isCaseSensitive;

   /**
    * Column may be updated
    */
   boolean     isWriteable;

   /**
    * Column is an identity column
    */
   boolean     isIdentity;

   /**
    * Column may be used as a key
    */
   boolean     isKey;

   /**
    * Column should be hidden
    */
   boolean     isHidden;

   /**
    * Database ID for UDT
    */
   int         userType;

   /**
    * MS SQL2000 collation
    */
   byte[]      collation;

   /**
    * Character set descriptor (if different from default)
    */
   CharsetInfo charsetInfo;

   /**
    * Column display size
    */
   int         displaySize;

   /**
    * Column buffer (max) size
    */
   int         bufferSize;

   /**
    * Column decimal precision
    */
   int         precision;

   /**
    * Column decimal scale
    */
   int         scale;

   /**
    * The SQL type name for this column.
    */
   String      sqlType;

   /////////////////////////////////////////////////////////////////////////////
   // overridden methods of class Object
   /////////////////////////////////////////////////////////////////////////////

   @Override
   public String toString()
   {
      return name;
   }

   @Override
   public int hashCode()
   {
      return System.identityHashCode( this );
   }

   @Override
   public boolean equals( Object other )
   {
      if( ! ( other instanceof ColInfo ) )
         return false;

      if( other == this )
         return true;

      ColInfo o = (ColInfo) other;

      return

          // compare primitive fields
             tdsType         == o.tdsType
          && jdbcType        == o.jdbcType
          && nullable        == o.nullable
          && userType        == o.userType
          && displaySize     == o.displaySize
          && bufferSize      == o.bufferSize
          && precision       == o.precision
          && scale           == o.scale
          && isCaseSensitive == o.isCaseSensitive
          && isWriteable     == o.isWriteable
          && isIdentity      == o.isIdentity
          && isKey           == o.isKey
          && isHidden        == o.isHidden

          // compare non-primitive fields
          && compare( realName   , o.realName    )
          && compare( name       , o.name        )
          && compare( tableName  , o.tableName   )
          && compare( catalog    , o.catalog     )
          && compare( schema     , o.schema      )
          && compare( sqlType    , o.sqlType     )
          && compare( charsetInfo, o.charsetInfo )

          // compare collation
          && Arrays.equals( collation, o.collation );
   }

   /////////////////////////////////////////////////////////////////////////////
   // private methods
   /////////////////////////////////////////////////////////////////////////////

   /**
    * <p> Compares two object. </p>
    *
    * @return
    *    {@code true} if either both values are {@code null} or comparing them
    *    using the equals method of <i>o1</i> returns {@code true}
    */
   private final boolean compare( Object o1, Object o2 )
   {
      return o1 == o2 || o1 != null && o1.equals( o2 );
   }

}