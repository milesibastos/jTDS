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


public class ParameterListItem implements Cloneable
{
   public static final String cvsVersion = "$Id: ParameterListItem.java,v 1.3 2001-09-18 08:38:07 aschoerk Exp $";


   //   public int      length     = -1;            


   /************************************************************
    * Information about the formal parameter
    *************************************************************/
   // maximum allowed length for the procedure's formal parameter.
   // For example if the procedure has a parameter 
   //     P1 varchar(25)
   // the maximum length will be 25.  The length of the actual 
   // parameter can be up to 25 characters.
   // NOTE!!! As of Jan. 29, 1999 this was only used for VARCHAR parameters.
   //
   public int      maxLength  = -1;                 

   // formal name of the stored proc parameter.  Example P1
   public String   formalName = null;               

   // SQL type of the formal parameter.  Example-  char(10)
   public String   formalType = null;               


   /************************************************************
    * Information about the actual parameter
    *************************************************************/
   // The JDBC type of the actual parameter given in the setXXX() method.
   public int      type       = java.sql.Types.NULL;

   // True if an actual parameter has been given to this parameter
   // by one of the setXXX() methods.  Note-  All parameters for 
   // a procedure must be given values before the procedure can 
   // be called.
   public boolean  isSet      = false;              

   // Value bound to the PreparedStatement parameter with one
   // of the setXXX() methods.
   public Object   value      = null;               

   // True if the actual parameter is an input parameter
   public boolean  isInput    = true;

   // True if the actual parameter is an output parameter.  This value
   // is set by one of the registerOutParam functions.
   public boolean  isOutput   = false;

   /**
    * unset all information about the parameter.  
    *
    * This includes the formal and actual parameter information.
    */
   public void clear()
   {
      type       = java.sql.Types.NULL;
      //  length     = -1;            
      maxLength  = -1;                 
      isSet      = false;              
      value      = null;               
      formalName = null;               
      formalType = null;               
      isInput    = true;               
      isOutput   = false;              
   }

   public Object clone()
   {
      try 
      {
         return super.clone();
      }
      catch(java.lang.CloneNotSupportedException e)
      {
         System.err.println("Serious problem.  Couldn't clone a Cloneable object");
         System.exit(1);
         return null;
      }
   }

}
