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
   public static final String cvsVersion = "$Id: Column.java,v 1.1.1.1 2001-08-10 01:44:27 skizz Exp $";


   private String  name;
   private boolean haveName = false;
   private int     displaySize;
   private boolean haveDisplaySize = false;
   private String  label;
   private boolean haveLabel = false;
   private int     type;
   private boolean haveType = false;
   private int     precision;
   private boolean havePrecision    = false;
   private int     scale;   
   private boolean haveScale        = false;
   private boolean readOnly         = false;
   private boolean readOnlySet      = false;
   private boolean autoIncrement    = false;
   private boolean autoIncrementSet = false;
   private int     nullable = java.sql.ResultSetMetaData.columnNullableUnknown;

   public Column()
   {
      name         = null;
      displaySize  = -1;
      label        = null;
      type         = -1;
      precision    = -1;
      scale        = -1;
   }

   public void setName(String value)
   {
      name     = value;
      haveName = true;
   }

   public String getName()
   {
      return name;
   }

   public void setDisplaySize(int value)
   {
      displaySize     = value;
      haveDisplaySize = true;
   }

   public int getDisplaySize()
   {
      return displaySize;
   }

   public void setLabel(String value)
   {
      label     = value;
      haveLabel = true;
   }

   public String getLabel()
   {
      return label;
   }

   public void setType(int value)
   {
      // don't convert from 
      type     = value;
      haveType = true;
   }

   public int getType()
   {
      return type;
   }
   
   public void setPrecision(int value)
   {
      precision     = value;
      havePrecision = true;
   }
   
   public int getPrecision()
   {
      return precision;
   }
   
   public void setScale(int value)
   {
      scale     = value;
      haveScale = true;
   }
   
   public int getScale()
   {
      return scale;
   }
   
   public boolean isAutoIncrement () 
   {
      return autoIncrement;
   }
   
   public void setAutoIncrement (boolean flag)
   {
      autoIncrementSet = true;
      autoIncrement    = flag;
   }
   
   public boolean autoIncrementWasSet()
   {
      return autoIncrementSet;
   }

   
   public int isNullable ()
   {
      return nullable;
   }
   
   public void setNullable (int flag)
   {
      nullable = flag;
   }
   
   public boolean isReadOnly () 
   {
      return readOnly;
   }
   
   public void setReadOnly (boolean flag)
   {
      readOnlySet = true;
      readOnly    = flag;
   }

   public boolean readOnlyWasSet()
   {
      return readOnlySet;
   }
}
