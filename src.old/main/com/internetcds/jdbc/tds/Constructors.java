//
// Copyright 1999 Craig Spannring
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



public class Constructors
{
   public static final String cvsVersion = "$Id: Constructors.java,v 1.5 2001-09-10 06:08:18 aschoerk Exp $";

   static boolean   dejavu  = false;

   static final int JDBC2_0 = 2;
   static final int JDBC1_0 = 1;

   private static int    jdbcVersion = JDBC1_0;
   private static String jdbcVersionName = null;

   private static java.lang.reflect.Constructor resultSetCtor         = null;
   private static java.lang.reflect.Constructor preparedStatementCtor = null;
   private static java.lang.reflect.Constructor callableStatementCtor = null;
   private static java.lang.reflect.Constructor connectionCtor        = null;


   static private java.lang.reflect.Constructor getCtor(
      String classNamePrefix,
      Class  paramTypes[])
      throws java.lang.ClassNotFoundException, java.lang.NoSuchMethodException
   {
      java.lang.reflect.Constructor result = null;
      String className                     = null;
      Class  theClass                      = null;

      className = (classNamePrefix );
      theClass = java.lang.Class.forName(className);

      result = theClass.getConstructor(paramTypes);

      return result;
   } /* getCtor()  */


   static private void init()
      throws java.sql.SQLException
   {
      try
      {
         Class resultSetParamTypes[] =
         {
            java.lang.Class.forName("com.internetcds.jdbc.tds.Tds"),
            java.lang.Class.forName("com.internetcds.jdbc.tds.TdsStatement"),
            java.lang.Class.forName("com.internetcds.jdbc.tds.Columns")
         };
// No longer used
//         Class preparedStatementParamTypes[] =
//         {
//            java.lang.Class.forName("java.sql.Connection"),
//            java.lang.Class.forName("com.internetcds.jdbc.tds.Tds"),
//            java.lang.Class.forName("java.lang.String")
//         };
//         Class callableStatementParamTypes[] =
//         {
//            java.lang.Class.forName("java.sql.Connection"),
//            java.lang.Class.forName("com.internetcds.jdbc.tds.Tds"),
//            java.lang.Class.forName("java.lang.String")
//         };
         Class connectionParamTypes[] =
         {
            java.lang.Class.forName("java.util.Properties")
         };


         try
         {
            Class statement = java.lang.Class.forName("java.sql.Statement");
            java.lang.reflect.Method execBatch =
                statement.getDeclaredMethod("executeBatch", new Class[0]);

            jdbcVersion     = JDBC2_0;
            jdbcVersionName = "2_0";
         }
         catch (NoSuchMethodException nsme)
         {
            jdbcVersion     = JDBC1_0;
            jdbcVersionName = "1_0";
         }

// No longer used
//         try
//         {
//            preparedStatementCtor = getCtor("com.internetcds.jdbc.tds.PreparedStatement_base",
//                                            preparedStatementParamTypes);
//            callableStatementCtor = getCtor("com.internetcds.jdbc.tds.CallableStatement_base",
//                                            callableStatementParamTypes);
//         }
//         catch(java.lang.ClassNotFoundException e)
//         {
//            if (jdbcVersion == JDBC2_0)
//            {
//               //
//               // If we couldn't find the 2.0 classes, let's try to fall back
//               // to JDBC 1.0
//               //
//               jdbcVersion     = JDBC1_0;
//               jdbcVersionName = "1_0";
//               preparedStatementCtor = getCtor("com.internetcds.jdbc.tds.PreparedStatement_base",
//                                               preparedStatementParamTypes);
//               callableStatementCtor = getCtor("com.internetcds.jdbc.tds.CallableStatement_base",
//                                               callableStatementParamTypes);
//            }
//         }
      }
      catch(java.lang.ClassNotFoundException e)
      {
         System.err.println("Couldn't find the class"); // XXX remove println
         throw new java.sql.SQLException(e.getMessage());
      }
//      catch(java.lang.NoSuchMethodException e)
//      {
//         System.err.println("Couldn't find a constructor");
//         throw new java.sql.SQLException(e.getMessage());
//      }

      dejavu = true;
   } /* init()  */



   public static java.sql.ResultSet newResultSet(
      Tds                  tds_,
      java.sql.Statement   stmt_,
      Columns              columns_) throws java.sql.SQLException
   {
      if (!dejavu)
      {
         init();
      }

      java.sql.ResultSet  result = null;
      try
      {
         Object  params[] = {tds_, stmt_, columns_};

         result = (java.sql.ResultSet)resultSetCtor.newInstance(params);
      }
      catch (java.lang.reflect.InvocationTargetException e)
      {
         throw new java.sql.SQLException(e.getTargetException().getMessage());
      }
      catch (Throwable e)
      {
         e.printStackTrace();
         throw new java.sql.SQLException(e.getMessage());
      }
      return result;
   }

   public static java.sql.CallableStatement newCallableStatement(
      Object                        cx_,
      com.internetcds.jdbc.tds.Tds  tds_,
      java.lang.String              sql_) throws java.sql.SQLException
   {
      if (!dejavu)
      {
         init();
      }

      return new CallableStatement_base( (TdsConnection)cx_, sql_ );

   }


   public static java.sql.PreparedStatement newPreparedStatement(
      Object                        cx_,
      java.lang.String              sql_) throws java.sql.SQLException
   {
      if (!dejavu)
      {
         init();
      }

      return new PreparedStatement_base( (TdsConnection)cx_, sql_ );
   }


    public static java.sql.Connection newConnection(
       java.util.Properties           props_)
        throws java.sql.SQLException, com.internetcds.jdbc.tds.TdsException
   {
      if (!dejavu)
      {
         init();
      }

      try
      {
         Object  params[] =
         {
            props_,
         };

         return (java.sql.Connection)connectionCtor.newInstance(params);
      }
      catch (java.lang.reflect.InvocationTargetException e)
      {
         throw new java.sql.SQLException(e.getTargetException().getMessage());
      }
      catch (Throwable e)
      {
         throw new java.sql.SQLException(e.getMessage());
      }
   } /* newConnection()  */

}

