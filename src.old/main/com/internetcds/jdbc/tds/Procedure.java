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
import java.util.StringTokenizer;
import java.util.Vector;


public class Procedure
{
   public static final String cvsVersion = "$Id: Procedure.java,v 1.3 2001-09-18 08:38:07 aschoerk Exp $";


   ParameterListItem  parameterList[]    = null;
   String             sqlProcedureName   = null;
   String             rawQueryString     = null;
   String             procedureString    = "";
   
   private Tds        tds                = null;
   private boolean    hasLobs            = false;

   private void init(String         rawSql_,
                String              sqlProcedureName_,
                ParameterListItem[] parameterList_,
                Tds                 tds_)
      throws SQLException
   {
      int   i;
      
      
      rawQueryString   = rawSql_;
      sqlProcedureName = sqlProcedureName_;
      tds              = tds_;

      // need a clone to keep the maxlength information for char data
      parameterList    = new ParameterListItem[parameterList_.length];
      for (i=0; i<parameterList.length; i++) 
      {
         parameterList[i] = (ParameterListItem)parameterList_[i].clone();
      }


      // make sure all the parameters are set
      ParameterUtils.verifyThatParametersAreSet(parameterList);
      
      // make sure we have the same number of placeholders as
      // item in the parameter list.
      if (parameterList.length !=
          ParameterUtils.countParameters(rawQueryString))
      {
         throw new SQLException("Number of parametrs in sql statement "
                                + "does not match the number of parameters "
                                + " in parameter list");
      }

      // Create the procedure text
      {
         procedureString = "create proc " + sqlProcedureName;
      
         // Create actual to formal parameter mapping
         ParameterUtils.createParameterMapping(rawQueryString, parameterList,
                                               tds);
      
         // copy back the formal types and check for LOBs
         for (i=0; i<parameterList.length; i++) 
         {
            parameterList_[i].formalType = parameterList[i].formalType;

            if (parameterList[i].formalType.equalsIgnoreCase("image") ||
                parameterList[i].formalType.equalsIgnoreCase("text")  ||
                parameterList[i].formalType.equalsIgnoreCase("ntext")) 
            {
               hasLobs = true;
            }
         }
      
         // add the formal parameter list to the sql string
         procedureString = procedureString
            + createFormalParameterList(parameterList);
      
         procedureString = procedureString + " as ";
            
         // Go through the raw sql and substitute a parameter name
         // for each '?' placeholder in the raw sql
      
         StringTokenizer   st = new StringTokenizer(rawQueryString,
                                                    "'?\\", true);
         final int         normal   = 1;
         final int         inString = 2;
         final int         inEscape = 3;
         int               state = normal;
         String            current;
         int               nextParameterIndex = 0;
      
         while (st.hasMoreTokens())
         {
            current = st.nextToken();
            switch (state)
            {
               case normal:
               {
                  if (current.equals("?"))
                  {
                     procedureString = procedureString + "@" 
                        + parameterList[nextParameterIndex].formalName;
                     nextParameterIndex++;
                  }
                  else if (current.equals("'"))
                  {
                     procedureString = procedureString + current;
                     state = inString;
                  }
                  else
                  {
                     procedureString = procedureString + current;
                  }
                  break;
               }
               case inString:
               {
                  if (current.equals("'"))
                  {
                     state = normal;
                  }
                  else if (current.equals("\\"))
                  {
                     state = inEscape;
                  }
                  else
                  {
                     // nop
                  }
                  procedureString = procedureString + current;
                  break;
               }
               case inEscape:
               {
                  state = inString;
                  procedureString = procedureString + current;
                  break;
               }
               default:
               {
                  throw new SQLException("Internal error.  Bad State " + state);
               }
            }
         }
      }
   }

   public Procedure(
      String              rawSql_,
      String              sqlProcedureName_,
      ParameterListItem[] parameterList_,
      Tds                 tds_)
      throws SQLException
   {
      init(rawSql_, 
           sqlProcedureName_,
           parameterList_,
           tds_);
   }
   
   public String getPreparedSqlString()
   {
      return procedureString;
   }

   public String getProcedureName()
   {
      return sqlProcedureName;
   }

   public ParameterListItem getParameter(int index)
   {
      return parameterList[index];
   }

   private String createFormalParameterList(
      ParameterListItem[]  parameterList)
   {
      int    i;
      String result;


      if (parameterList.length == 0)
      {
         result = "";
      }
      else
      {
         result = "("; 
         for(i=0; i<parameterList.length; i++)
         {
            if (i>0)
            {
               result = result + ", ";
            }
            result =
               result + "@" + parameterList[i].formalName + " "
               + parameterList[i].formalType;
            if (parameterList[i].isOutput)
            {
               result = result + " output";
            }
         }
         result = result + ")";
      }
      return result;
   }

   /**
    * This method checks to see if the actual parameters are compatible
    * with the formal parameters for this procedure.
    *
    */
   public boolean compatibleParameters(
      ParameterListItem actualParams[])
      throws SQLException
   {
      int     i;
      boolean isOkay = true;

      isOkay = parameterList.length == actualParams.length;

      for(i=0; isOkay && i<actualParams.length; i++)
      {
          if ((parameterList[i].formalType.startsWith("char") ||
               parameterList[i].formalType.startsWith("varchar") ||
               parameterList[i].formalType.startsWith("text") ||
               parameterList[i].formalType.startsWith("nchar") ||
               parameterList[i].formalType.startsWith("nvarchar") ||
               parameterList[i].formalType.startsWith("ntext"))
              && (actualParams[i].type == java.sql.Types.CHAR || 
                  actualParams[i].type == java.sql.Types.VARCHAR ||
                  actualParams[i].type == java.sql.Types.LONGVARCHAR))
          {
             isOkay = parameterList[i].maxLength >= actualParams[i].maxLength;
          }
          else 
          {
               isOkay = 
                   parameterList[i].formalType.equalsIgnoreCase(actualParams[i].formalType);
          }
      }
      return isOkay;
   }


   public ParameterListItem[] getParameterList()
   {
      return parameterList;
   }

   /**
    * Does the formal parameter list include LOB parameters?
    */
   public boolean hasLobParameters() {return hasLobs;}
}
