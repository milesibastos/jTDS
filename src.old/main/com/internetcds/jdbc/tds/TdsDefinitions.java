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



/**
 * constants from the 4.2 TDS protocol
 *
 * @version  $Id: TdsDefinitions.java,v 1.1.1.1 2001-08-10 01:53:28 skizz Exp $
 * @author Craig Spannring
 * @author The FreeTDS project.
 */
interface TdsDefinitions
{
   public static final String cvsVersion = "$Id: TdsDefinitions.java,v 1.1.1.1 2001-08-10 01:53:28 skizz Exp $";

   //
   // Define the type of database the driver is connection to.
   //
   public static final int SQLSERVER = 1;
   public static final int SYBASE    = 2;

   //
   // Versions of the TDS protocol.  Keep the values in order so code
   // can recognize versions at or after a specified version.
   //
   public static final int TDS42     = 42;
   public static final int TDS50     = 50;
   public static final int TDS70     = 70;

   //
   // Sub packet types
   //
   static final byte TDS_RET_STAT_TOKEN  = (byte)0x79; // 121 
   static final byte TDS_PROCID          = (byte)0x7C; // 124 TDS_PROCID
   static final byte TDS7_RESULT_TOKEN   = (byte)129;   // 0x81 TDS 7.0 only
   static final byte TDS_COL_NAME_TOKEN  = (byte)0xA0;  // 160 TDS 4.2 only
   static final byte TDS_COL_INFO_TOKEN  = (byte)161;  // 0xA1 TDS 4.2 only 
   static final byte TDS_TABNAME         = (byte)164;  // 0xA4 
   static final byte TDS_UNKNOWN_0xA5    = (byte)0xA5;  // 0xA5
   static final byte TDS_UNKNOWN_0xA7    = (byte)0xA7; // 
   static final byte TDS_UNKNOWN_0xA8    = (byte)0xA8; // 
   static final byte TDS_ORDER           = (byte)169;  // 0xA9 TDS_ORDER
   static final byte TDS_ERR_TOKEN       = (byte)170;  // 0xAA
   static final byte TDS_MSG_TOKEN       = (byte)171;  // 0xAB
   static final byte TDS_TEXT_UPD_TOKEN  = (byte)172;  // 0xAC write- updatetext
   static final byte TDS_LOGIN_ACK_TOKEN = (byte)173;   // 0xAD
   static final byte TDS_CONTROL         = (byte)174;   // 0xAE TDS_CONTROL
   static final byte TDS_ROW_TOKEN       = (byte)209;   // 0xD1
   static final byte TDS_UNKNOWN_0xE2    = (byte)226;   // 0xE2
   static final byte TDS_ENV_CHG_TOKEN   = (byte)227;   // 0xE3
   static final byte TDS_MSG50_TOKEN     = (byte)229;   // 0xE5
   static final byte TDS_RESULT_TOKEN    = (byte)238;   // 0xEE
   static final byte TDS_END_TOKEN       = (byte)253;   // 0xFD TDS_DONE
   static final byte TDS_DONEPROC        = (byte)254;   // 0xFE TDS_DONEPROC
   static final byte TDS_DONEINPROC      = (byte)255;   // 0xFF TDS_DONEINPROC
   // end of sub packet types



   //
   // Native Column types
   //
   static final byte SYBVOID        =  31;   // 0x1F
   static final byte SYBIMAGE       =  34;   // 0x22
   static final byte SYBTEXT        =  35;   // 0x23
   static final byte SYBVARBINARY   =  37;   // 0x25
   static final byte SYBINTN        =  38;   // 0x26 
   static final byte SYBVARCHAR     =  39;   // 0x27
   static final byte SYBBINARY      =  45;   // 0x2D
   static final byte SYBCHAR        =  47;   // 0x2F
   static final byte SYBINT1        =  48;   // 0x30
   static final byte SYBBIT         =  50;   // 0x32
   static final byte SYBINT2        =  52;   // 0x34
   static final byte SYBINT4        =  56;   // 0x38
   static final byte SYBDATETIME4   =  58;   // 0x3A
   static final byte SYBREAL        =  59;   // 0x3B
   static final byte SYBMONEY       =  60;   // 0x3C (does not allow nulls?)
   static final byte SYBDATETIME    =  61;   // 0x3D
   static final byte SYBFLT8        =  62;   // 0x3E
   static final byte SYBNTEXT       =  99;   // 0x63
   static final byte SYBNVARCHAR    = 103;   // 0x67
   static final byte SYBBITN        = 104;   // 0x68
   static final byte SYBDECIMAL     = 106;   // 0x6A
   static final byte SYBNUMERIC     = 108;   // 0x6C
   static final byte SYBFLTN        = 109;   // 0x6D
   static final byte SYBMONEYN      = 110;   // 0x6E
   static final byte SYBDATETIMN    = 111;   // 0x6F
   static final byte SYBMONEY4      = 112;   // 0x70
   static final byte SYBNCHAR       = -17;   // 0xEF
   // XXX should SYBMONEY4 be 122 instead of 112?
   static final byte SYBSMALLMONEY  = 122;   // 0x7A 
   // end of column types
}
