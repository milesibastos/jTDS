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


package net.sourceforge.jtds.jdbc;



/**
 * Constants from the TDS protocol.
 *
 * @version  $Id: TdsDefinitions.java,v 1.11 2004-04-04 22:12:03 alin_sinpalean Exp $
 * @author Craig Spannring
 * @author The FreeTDS project.
 */
interface TdsDefinitions
{
   String cvsVersion = "$Id: TdsDefinitions.java,v 1.11 2004-04-04 22:12:03 alin_sinpalean Exp $";

   //
   // Define the type of database the driver is connection to.
   //
   int SQLSERVER = 1;
   int SYBASE    = 2;

   //
   // Versions of the TDS protocol.  Keep the values in order so code
   // can recognize versions at or after a specified version.
   //
   int TDS42     = 42;
   int TDS50     = 50;
   int TDS70     = 70;

   //
   // Sub packet types
   //
   byte TDS_LANG_TOKEN      = (byte)33;   // 0x21 ? TDS 5.0 only
   byte TDS_CLOSE_TOKEN     = (byte)113;  // 0x71 ? TDS 5.0 only? ct_close()
   byte TDS_RETURNSTATUS    = (byte)121;  // 0x79 RETURNSTATUS
   byte TDS_PROCID          = (byte)124;  // 0x7C TDS_PROCID
   byte TDS_COLMETADATA     = (byte)129;  // 0x81 COLMETADATA TDS 7.0 only
   byte TDS_COL_NAME_TOKEN  = (byte)160;  // 0xA0 TDS 4.2 only
   byte TDS_COL_INFO_TOKEN  = (byte)161;  // 0xA1 TDS 4.2 only
   byte TDS_TABNAME         = (byte)164;  // 0xA4 TABNAME
   byte TDS_COLINFO         = (byte)165;  // 0xA5 COLINFO
   byte TDS_UNKNOWN_0xA7    = (byte)167;  // 0xA7
   byte TDS_UNKNOWN_0xA8    = (byte)168;  // 0xA8
   byte TDS_ORDER           = (byte)169;  // 0xA9 ORDER
   byte TDS_ERROR           = (byte)170;  // 0xAA ERROR
   byte TDS_INFO            = (byte)171;  // 0xAB INFO
   byte TDS_PARAM           = (byte)172;  // 0xAC
   byte TDS_LOGINACK        = (byte)173;  // 0xAD LOGINACK
   byte TDS_CONTROL         = (byte)174;  // 0xAE TDS_CONTROL
   byte TDS_ROW             = (byte)209;  // 0xD1 ROW
   byte TDS_ALTROW          = (byte)211;  // 0xD3 ALTROW Compute Result Row
   byte TDS_CAP_TOKEN       = (byte)226;  // 0xE2
   byte TDS_ENVCHANGE       = (byte)227;  // 0xE3 ENVCHANGE
   byte TDS_MSG50_TOKEN     = (byte)229;  // 0xE5
   byte TDS_AUTH_TOKEN      = (byte)237;  // 0xED mdb: NTLM challenge
   byte TDS_RESULT_TOKEN    = (byte)238;  // 0xEE
   byte TDS_DONE            = (byte)253;  // 0xFD DONE
   byte TDS_DONEPROC        = (byte)254;  // 0xFE DONEPROC
   byte TDS_DONEINPROC      = (byte)255;  // 0xFF DONEINPROC
   // end of sub packet types

   byte TDS_ENV_DATABASE  = (byte)1;
   byte TDS_ENV_CHARSET   = (byte)3;
   byte TDS_ENV_BLOCKSIZE = (byte)4;


   //
   // Native Column types
   //
   byte SYBVOID        =  31;   // 0x1F
   byte SYBIMAGE       =  34;   // 0x22
   byte SYBTEXT        =  35;   // 0x23
   byte SYBUNIQUEID    =  36;   // 0x24 - uniqueidentifier
   byte SYBVARBINARY   =  37;   // 0x25
   byte SYBINTN        =  38;   // 0x26
   byte SYBVARCHAR     =  39;   // 0x27
   byte SYBBINARY      =  45;   // 0x2D
   byte SYBCHAR        =  47;   // 0x2F
   byte SYBINT1        =  48;   // 0x30
   byte SYBBIT         =  50;   // 0x32
   byte SYBINT2        =  52;   // 0x34
   byte SYBINT4        =  56;   // 0x38
   byte SYBDATETIME4   =  58;   // 0x3A
   byte SYBREAL        =  59;   // 0x3B
   byte SYBMONEY       =  60;   // 0x3C (does not allow nulls?)
   byte SYBDATETIME    =  61;   // 0x3D
   byte SYBFLT8        =  62;   // 0x3E
   byte SYBNTEXT       =  99;   // 0x63
   byte SYBNVARCHAR    = 103;   // 0x67
   byte SYBBITN        = 104;   // 0x68
   byte SYBDECIMAL     = 106;   // 0x6A
   byte SYBNUMERIC     = 108;   // 0x6C
   byte SYBFLTN        = 109;   // 0x6D
   byte SYBMONEYN      = 110;   // 0x6E
   byte SYBDATETIMN    = 111;   // 0x6F
   byte SYBMONEY4      = 112;   // 0x70
   byte SYBNCHAR       = -17;   // 0xEF

   // according to srv.h here are some additional types
   byte SYBBIGVARBINARY = (byte)0xA5;
   byte SYBBIGVARCHAR   = (byte)0xA7;
   byte SYBBIGBINARY    = (byte)0xAD;
   byte SYBBIGCHAR      = (byte)0xAF;
   byte SYBBIGNVARCHAR  = (byte)0xE7;

   // XXX should SYBMONEY4 be 122 instead of 112?
   byte SYBSMALLMONEY  = 122;   // 0x7A
   // end of column types

   String PROP_SERVERNAME = "SERVERNAME",
          PROP_INSTANCE   = "INSTANCE",
          PROP_SERVERTYPE = "SERVERTYPE",
          PROP_PORT       = "PORTNUMBER",
          PROP_DBNAME     = "DATABASENAME",
          PROP_USER       = "USER",
          PROP_PASSWORD   = "PASSWORD",
          PROP_CHARSET    = "CHARSET",
          PROP_APPNAME    = "APPNAME",
          PROP_PROGNAME   = "PROGNAME",
          PROP_TDS        = "TDS",
          PROP_DOMAIN     = "DOMAIN", //if present, indicates NTLM auth
          PROP_LAST_UPDATE_COUNT = "LASTUPDATECOUNT",
          PROP_USEUNICODE = "SENDSTRINGPARAMETERSASUNICODE",
          PROP_MAC_ADDR   = "MACADDRESS";

    // Opcodes returned by TDS_DONE/TDS_DONEPROC/TDS_DONEINPROC
    int OPCODE_SELECT       = 0xC1;
    int OPCODE_CREATE_TABLE = 0xC6;
    int OPCODE_DROP_TABLE   = 0xC7;
    int OPCODE_ALTER_TABLE  = 0xD8;
    int OPCODE_CREATE_PROC  = 0xDE;
    int OPCODE_DROP_PROC    = 0xDF;
    int OPCODE_END_PROC     = 0xE0;
}
