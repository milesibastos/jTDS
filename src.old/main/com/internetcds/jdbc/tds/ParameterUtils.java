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
// import java.util.Vector;

public class ParameterUtils
{
   public static final String cvsVersion = "$Id: ParameterUtils.java,v 1.6 2002-09-26 14:10:31 alin_sinpalean Exp $";


   /**
    * Count the number of parameters in a prepared sql statement.
    *
    * @return number of parameter placeholders in sql statement.
    * @exception SQLException thrown if there is a problem parsing statment.
    */
   public static int countParameters(String sql)
      throws java.sql.SQLException
   {
      //
      // This is method is implemented as a very simple finite state machine.
      //

      int               result = 0;

      if (sql == null)
      {
         throw new SQLException("No statement");
      }
      else
      {
         StringTokenizer   st = new StringTokenizer(sql, "'?\\", true);
         final int         normal   = 1;
         final int         inString = 2;
         final int         inEscape = 3;
         int               state = normal;
         String            current;

         while (st.hasMoreTokens())
         {
            current = st.nextToken();
            switch (state)
            {
               case normal:
               {
                  if (current.equals("?"))
                  {
                     result++;
                  }
                  else if (current.equals("'"))
                  {
                     state = inString;
                  }
                  else
                  {
                     // nop
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
                  break;
               }
               case inEscape:
               {
                  state = inString;
                  break;
               }
               default:
               {
                  throw new SQLException("Internal error.  Bad State " + state);
               }
            }
         }
      }

      return result;
   }

   /**
    * check that all items in parameterList have been given a value
    *
    * @exception SQLException thrown if one or more parameters aren't set
    */
   public static void verifyThatParametersAreSet(
      ParameterListItem[] parameterList)
      throws SQLException
   {
      int     i;
      boolean okay = true;

      for(i=0; okay && i<parameterList.length; i++)
      {
         if (parameterList[i].isOutput)
         {
            // Allow output parameters to be "not set"
            parameterList[i].isSet = true;
         }
         okay = okay && parameterList[i].isSet;
         if (!okay)
         {
            throw new SQLException("parameter #" + (i+1)
                                   + " has not been set");
         }
      }
   }

   /**
    * create the formal parameters for a parameter list.
    *
    * This method takes a sql string and a parameter list containing
    * the actual parameters and creates the formal parameters for
    * a stored procedure.  The formal parameters will later be used
    * when the stored procedure is submitted for creation on the server.
    *
    * @param rawQueryString   (in-only)
    * @param parameterList    (update)
    *
    */
   public static void createParameterMapping(
      String              rawQueryString,
      ParameterListItem[] parameterList,
      Tds tds)
      throws SQLException
   {
      int    i;
      String nextFormal;
      int    nextParameterNumber = 0;
      int    tdsVer              = tds.getTdsVer();
      EncodingHelper encoder     = tds.getEncoder();

      for(i=0; i<parameterList.length; i++)
      {
         do
         {
            nextParameterNumber++;
            nextFormal = "P" + nextParameterNumber;
         } while (-1 != rawQueryString.indexOf(nextFormal));

         parameterList[i].formalName = nextFormal;

         switch (parameterList[i].type)
         {
            case java.sql.Types.VARCHAR:
            case java.sql.Types.CHAR:
            {
               String value = (String)parameterList[i].value;
               if (value == null && tdsVer != Tds.TDS70)
               {
                   // use the smalles case possible for nulls
                   parameterList[i].formalType = "varchar(255)";
                   parameterList[i].maxLength = 255;

               }
               else if (tdsVer == Tds.TDS70)
               {
                   /*
                    * SQL Server 7 can handle Unicode so use it wherever
                    * possible
                    */

                   if (value == null || value.length() < 4001)
                   {
                       parameterList[i].formalType = "nvarchar(4000)";
                       parameterList[i].maxLength = 4000;
                   }
                   else if (value.length() < 8001
                              && !encoder.isDBCS()
                              && encoder.canBeConverted(value))
                   {
                       parameterList[i].formalType = "varchar(8000)";
                       parameterList[i].maxLength = 8000;
                   }
                   else
                   {
                       parameterList[i].formalType = "ntext";
                       parameterList[i].maxLength = Integer.MAX_VALUE;
                   }
               }
               else
               {
                   int len = value.length();
                   if (encoder.isDBCS() &&  len > 127 && len < 256)
                   {
                       len = encoder.getBytes(value).length;
                   }

                   if (len < 256)
                   {
                       parameterList[i].formalType = "varchar(255)";
                       parameterList[i].maxLength = 255;
                   }
                   else
                   {
                       parameterList[i].formalType = "text";
                       parameterList[i].maxLength = Integer.MAX_VALUE;
                   }
               }
               break;
            }
            case java.sql.Types.LONGVARCHAR:
            {
               if (tdsVer == Tds.TDS70)
               {
                   parameterList[i].formalType = "ntext";
               }
               else
               {
                   parameterList[i].formalType = "text";
               }
               parameterList[i].maxLength = Integer.MAX_VALUE;
               break;
            }
            case java.sql.Types.INTEGER:
            {
               parameterList[i].formalType = "integer";
               break;
            }
            case java.sql.Types.FLOAT:
            case java.sql.Types.REAL:
            {
               parameterList[i].formalType = "real";
               break;
            }
            case java.sql.Types.DOUBLE:
            {
               parameterList[i].formalType = "float";
               break;
            }
            case java.sql.Types.TIMESTAMP:
            case java.sql.Types.DATE:
            case java.sql.Types.TIME:
            {
               parameterList[i].formalType = "datetime";
               break;
            }
            case java.sql.Types.LONGVARBINARY:
            case java.sql.Types.VARBINARY:
            {
               parameterList[i].formalType = "image";
               break;
            }
            case java.sql.Types.BIT:
            {
               parameterList[i].formalType = "bit";
               break;
            }
            case java.sql.Types.BIGINT: {
               parameterList[i].formalType = "decimal(28,10)";
               break;
            }
            case java.sql.Types.SMALLINT:
            {
               parameterList[i].formalType = "smallint";
               break;
            }
            case java.sql.Types.TINYINT:
            {
               parameterList[i].formalType = "tinyint";
               break;
            }
            case java.sql.Types.DECIMAL:
            case java.sql.Types.NUMERIC:
            {
               parameterList[i].formalType = "decimal(28,10)";
               break;
            }
            case java.sql.Types.BINARY:
            case java.sql.Types.NULL:
            case java.sql.Types.OTHER:
            {
               throw new SQLException("Not implemented (type is java.sql.Types."
                                      + TdsUtil.javaSqlTypeToString(parameterList[i].type) + ")");
            }
            default:
            {
               throw new SQLException("Internal error.  Unrecognized type "
                                      + parameterList[i].type);
            }
         }
      }
   }
}

