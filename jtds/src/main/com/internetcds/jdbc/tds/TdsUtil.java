//                                                                            
// Copyright 2000 Craig Spannring, Bozeman Montana
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
//      This product includes software developed by Craig Spannring        
// 4. The name of Craig Spannring may not be used to endorse or promote   
//    products derived from this software without specific prior              
//    written permission.                                                     
//                                                                            
// THIS SOFTWARE IS PROVIDED BY CRAIG SPANNRING ``AS IS'' AND              
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE      
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE 
// ARE DISCLAIMED.  IN NO EVENT SHALL CRAIG SPANNRING BE LIABLE            
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

public class TdsUtil
{
   public static final String cvsVersion = "$Id: TdsUtil.java,v 1.1.1.1 2001-08-10 01:44:38 skizz Exp $";


   static public String javaSqlTypeToString(int type)
   {
      switch(type)
      {
         case java.sql.Types.BIT:              return "BIT";
         case java.sql.Types.TINYINT:          return "TINYINT";
         case java.sql.Types.SMALLINT:         return "SMALLINT";
         case java.sql.Types.INTEGER:          return "INTEGER";
         case java.sql.Types.BIGINT:           return "BIGINT";
         case java.sql.Types.FLOAT:            return "FLOAT";
         case java.sql.Types.REAL:             return "REAL";
         case java.sql.Types.DOUBLE:           return "DOUBLE";
         case java.sql.Types.NUMERIC:          return "NUMERIC";
         case java.sql.Types.DECIMAL:          return "DECIMAL";
         case java.sql.Types.CHAR:             return "CHAR";
         case java.sql.Types.VARCHAR:          return "VARCHAR";
         case java.sql.Types.LONGVARCHAR:      return "LONGVARCHAR";
         case java.sql.Types.DATE:             return "DATE";
         case java.sql.Types.TIME:             return "TIME";
         case java.sql.Types.TIMESTAMP:        return "TIMESTAMP";
         case java.sql.Types.BINARY:           return "BINARY";
         case java.sql.Types.VARBINARY:        return "VARBINARY";
         case java.sql.Types.LONGVARBINARY:    return "LONGVARBINARY";
         case java.sql.Types.NULL:             return "NULL";
         case java.sql.Types.OTHER:            return "OTHER";
         default:                              return "UNKNOWN" + type;
      }
   } /* javaSqlTypeToString()  */
}

