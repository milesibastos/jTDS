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
   public static final String cvsVersion = "$Id: TdsUtil.java,v 1.3 2002-08-14 11:44:21 alin_sinpalean Exp $";


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

    /**
     * Converts a 16-byte array representing a UNIQUEIDENTIFIER value into the
     * corresponding <code>String</code>.
     * <p>
     * Example:
     * <ul>
     *   <li>Binary format: <code>0xff19966f868b11d0b42d00c04fc964ff</code>
     *   <li>Character string format: <code>'6F9619FF-8B86-D011-B42D-00C04FC964FF'</code>
     * </ul>
     */
    public static String uniqueIdToString(byte bin[])
    {
        char hex[] =
        {
            '0', '1', '2', '3', '4', '5', '6', '7',
            '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'
        };
        char buf[] = new char[36];

        buf[0]  = hex[bin[3] >> 4 & 0xf];
        buf[1]  = hex[bin[3] & 0xf];
        buf[2]  = hex[bin[2] >> 4 & 0xf];
        buf[3]  = hex[bin[2] & 0xf];
        buf[4]  = hex[bin[1] >> 4 & 0xf];
        buf[5]  = hex[bin[1] & 0xf];
        buf[6]  = hex[bin[0] >> 4 & 0xf];
        buf[7]  = hex[bin[0] & 0xf];
        buf[8]  = '-';
        buf[9]  = hex[bin[5] >> 4 & 0xf];
        buf[10] = hex[bin[5] & 0xf];
        buf[11] = hex[bin[4] >> 4 & 0xf];
        buf[12] = hex[bin[4] & 0xf];
        buf[13] = '-';
        buf[14] = hex[bin[7] >> 4 & 0xf];
        buf[15] = hex[bin[7] & 0xf];
        buf[16] = hex[bin[6] >> 4 & 0xf];
        buf[17] = hex[bin[6] & 0xf];
        buf[18] = '-';
        buf[19] = hex[bin[8] >> 4 & 0xf];
        buf[20] = hex[bin[8] & 0xf];
        buf[21] = hex[bin[9] >> 4 & 0xf];
        buf[22] = hex[bin[9] & 0xf];
        buf[23] = '-';
        buf[24] = hex[bin[10] >> 4 & 0xf];
        buf[25] = hex[bin[10] & 0xf];
        buf[26] = hex[bin[11] >> 4 & 0xf];
        buf[27] = hex[bin[11] & 0xf];
        buf[28] = hex[bin[12] >> 4 & 0xf];
        buf[29] = hex[bin[12] & 0xf];
        buf[30] = hex[bin[13] >> 4 & 0xf];
        buf[31] = hex[bin[13] & 0xf];
        buf[32] = hex[bin[14] >> 4 & 0xf];
        buf[33] = hex[bin[14] & 0xf];
        buf[34] = hex[bin[15] >> 4 & 0xf];
        buf[35] = hex[bin[15] & 0xf];

        return new String(buf);
    }
}

