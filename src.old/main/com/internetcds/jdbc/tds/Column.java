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

import java.sql.ResultSetMetaData;


public class Column
{
   public static final String cvsVersion = "$Id: Column.java,v 1.4 2002-08-30 10:27:18 alin_sinpalean Exp $";

   private String  catalog       = null;
   private String  schema        = null;
   private String  name          = null;
   private String  label         = null;
   private String  tableName     = null;
   private int     displaySize   = -1;
   private int     bufferSize    = -1;
   private int     type          = -1;
   private int     precision     = -1;
   private int     scale         = -1;
   private Boolean readOnly      = null;
   private Boolean autoIncrement = null;
   private Boolean caseSensitive = null;
   private int     nullable      = java.sql.ResultSetMetaData.columnNullableUnknown;

   public Column() {}

   public void setCatalog(String value)
   {
      catalog = value;
   }

   public String getCatalog()
   {
      return catalog;
   }

   public void setSchema(String value)
   {
      schema = value;
   }

   public String getSchema()
   {
      return schema;
   }

   public void setName(String value)
   {
      name = value;
   }

   public String getName()
   {
      return name;
   }

   public void setDisplaySize(int value)
   {
      displaySize = value;
   }

   public int getDisplaySize()
   {
      return displaySize;
   }

   public void setBufferSize(int value)
   {
      bufferSize = value;
   }

   public int getBufferSize()
   {
      return bufferSize;
   }

   public void setLabel(String value)
   {
      label = value;
   }

   public String getLabel()
   {
      return label;
   }

   public void setType(int value)
   {
      type = value;
   }

   public int getType()
   {
      return type;
   }

   public void setPrecision(int value)
   {
      precision = value;
   }

   public int getPrecision()
   {
      return precision;
   }

   public void setScale(int value)
   {
      scale = value;
   }

   public int getScale()
   {
      return scale;
   }

   public Boolean isAutoIncrement ()
   {
      return autoIncrement;
   }

   public void setAutoIncrement (boolean flag)
   {
      autoIncrement = flag ? Boolean.TRUE : Boolean.FALSE;
   }

   public int isNullable ()
   {
      return nullable;
   }

   public void setNullable (int flag)
   {
      nullable = flag;
   }

   public Boolean isReadOnly ()
   {
      return readOnly;
   }

   public void setReadOnly (boolean flag)
   {
      readOnly = flag ? Boolean.TRUE : Boolean.FALSE;
   }

   public Boolean isCaseSensitive()
   {
      return caseSensitive;
   }

   public void setCaseSensitive(boolean flag)
   {
      caseSensitive = flag ? Boolean.TRUE : Boolean.FALSE;
   }

   public void setTableName(String name)
   {
      tableName = name;
   }

   public String getTableName()
   {
      return tableName;
   }
}
