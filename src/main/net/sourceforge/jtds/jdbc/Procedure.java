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
// MJH - 2003
// Add signature variable to use as key for procedure cache.
// Remove local storage of rawQueryString and TDS
// simplify constructor
// Use StringBuffer to make string handling more efficient
// Delete faulty compatibleParameters method as not used anywhere
// Tidy up code and remove redundant method calls

package net.sourceforge.jtds.jdbc;

import java.sql.*;

public class Procedure {
    public static final String cvsVersion = "$Id: Procedure.java,v 1.11 2004-03-25 20:23:50 alin_sinpalean Exp $";

    // next number to return from the getUniqueId() method
    private static long  id = 1;

//    private ParameterListItem _parameterList[]; MJH 14/03/04 Local copy no needed
    private String _sqlProcedureName; // Intentionally not initialized
    private String _procedureString; // Intentionally not initialized
    private String _signature; // Intentionally not initialized

    public Procedure(String rawQueryString,
                     String signature,
                     ParameterListItem[] parameterListIn,
                     Tds tds)
    throws SQLException {
        _signature = signature;
        _sqlProcedureName = getUniqueProcedureName(tds);

        // Create actual to formal parameter mapping
        // SAfe No need for this. It's already done by PreparedStatement_base.
        // ParameterUtils.createParameterMapping(rawQueryString, parameterListIn, tds);

        // MJH - 14/03/04 Local copy of parameters is no longer needed.
        // copy back the formal types and check for LOBs
//        _parameterList = new ParameterListItem[parameterListIn.length];
//      for (int i = 0; i < _parameterList.length; i++) {
//            _parameterList[i] = (ParameterListItem) parameterListIn[i].clone();
//            _parameterList[i].value = null; // MJH allow value to be garbage collected
//        }

        // Create the procedure text; allocate a buffer large enough for the raw string
        // plus a little extra for the procedure name and parameters.
        StringBuffer procedureString = new StringBuffer(rawQueryString.length()
                                                        + 128);

        procedureString.append("create proc ");
        procedureString.append(_sqlProcedureName);

        // add the formal parameter list to the sql string
        createFormalParameterList(parameterListIn, procedureString);
        procedureString.append(" as ");

// MJH 14/03/04
// The code below is no longer needed as the raw query string has alread
// had the parameter markers replaced with variables in the escape processor
        // Go through the raw sql and substitute a parameter name
        // for each '?' placeholder in the raw sql
//        boolean inString = false;
//        char[] chars = rawQueryString.toCharArray(); // avoid getfield opcode
//        int nextParameterIndex = 0;

//        for (int i = 0; i < chars.length; i++) {
//            char ch = chars[i]; // avoid getfield opcode

//            if (!inString) {
//                if (ch == '?') {
//                    procedureString.append(_parameterList[nextParameterIndex].formalName);
//                    nextParameterIndex++;
//                } else {
//                    if (ch == '\'') {
//                        inString = true;
//                    }
//
//                    procedureString.append(ch);
//                }
//            } else {
                // NOTE: This works even if a single quote is being used as an escape
                // character since the next single quote found will force the state
                // to be inString == true again.
//                if (ch == '\'') {
//                    inString = false;
//               }
//
//                procedureString.append(ch);
//            }
//        }

        procedureString.append(rawQueryString);
        _procedureString = procedureString.toString();
    }

    public String getPreparedSqlString() {
        return _procedureString;
    }

    public String getProcedureName() {
        return _sqlProcedureName;
    }

    public String getSignature() {
        return _signature;
    }
// MJH 14/03/04 - Delete as not needed.
//    public ParameterListItem[] getParameterList() {
//        return _parameterList;
//    }

    private void createFormalParameterList(ParameterListItem[] parameterList,
                                           StringBuffer result) {
        if (parameterList.length > 0) {
            result.append('(');

            for (int i = 0; i < parameterList.length; i++) {
                if (i > 0) {
                    result.append(", ");
                }

                result.append(parameterList[i].formalName);
                result.append(' ');
                result.append(parameterList[i].formalType);

                if (parameterList[i].isOutput) {
                    result.append(" output");
                }
            }

            result.append(')');
        }
    }

    /**
     * Create a new and unique name for a store procedure. This routine will
     * return a unique name for a stored procedure that will be associated with
     * a PreparedStatement().
     * <p>
     * Since SQLServer supports temporary procedure names we can just use
     * UniqueId.getUniqueId() to generate a unique (for the connection) name.
     * <p>
     * Sybase does not support temporary procedure names so we will have to
     * have a per user table devoted to storing user specific stored
     * procedures. The table name will be of the form
     * database.user.jdbc_temp_stored_proc_names. The table will be defined as
     * <code>CREATE TABLE database.user.jdbc_temp_stored_proc_names ( id
     * NUMERIC(10, 0) IDENTITY; session int not null; name char(29) )</code>
     * This routine will use that table to track names that are being used.
     *
     * @return the UniqueProcedureName value
     */
    private String getUniqueProcedureName(Tds tds) {
        if (tds.getServerType() == Tds.SYBASE) {
            String id = getNewId();

            // Fix for bug 822544 ???

            // Since Sybase does not support temporary stored procedures, construct
            // permenant stored procedures with a UUID as the name.
            // Since I do not have access to Sybase I was unable to test this...

            // Prepend "jTDS" and to the new id.  Since stored procedure names in Sybase
            // can only be 30 characters long, use only the last 26 characters of the new
            // id and prefix the name with "jTDS" so that the procedures can be deleted
            // easily by a system administrator.
            return "jTDS" + id.substring(id.length() - 26);
        } else {
            return "#jdbc#" + getUniqueId();
        }
    }

    /**
     * return a unique number.
     * <p>
     * The number is unique for a given invocation of the JVM.  Currently
     * the number is monotonically increasing, but I make no guarantees
     * that it will be that way in future releases.
     *
     * @return unique number
     */
    public static synchronized long getUniqueId() {
       return id++;
    }

    /**
     * Returns a string id that is guaranteed to be as unique at the
     * <code>java.rmi.dgc.VMID</code> class.
     */
    public static String getNewId() {
        String id = new java.rmi.dgc.VMID().toString();
        int pos = id.lastIndexOf(':');
        String count = id.substring(pos + 1);

        if (count.charAt(0) == '-') {
            count = "n" + count.substring(1);
        } else {
            count = "p" + count;
        }

        id = id.substring(0, pos) + count;

        while ((pos = id.indexOf(':')) != -1) {
            id = id.substring(0, pos) + id.substring(pos + 1, id.length());
        }

        return id;
    }
}
