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
import java.util.StringTokenizer;

public class Procedure {
    public static final String cvsVersion = "$Id: Procedure.java,v 1.6 2004-02-05 19:00:31 alin_sinpalean Exp $";

    private ParameterListItem _parameterList[]; // Intentionally not initialized
    private String _sqlProcedureName; // Intentionally not initialized
    private String _procedureString; // Intentionally not initialized
    private String _signature; // Intentionally not initialized
    private boolean _hasLobs = false;

    public Procedure(String rawQueryString,
                     String signature,
                     String sqlProcedureName,
                     ParameterListItem[] parameterListIn,
                     Tds tds)
    throws SQLException {
        _signature = signature;
        _sqlProcedureName = sqlProcedureName;

        // Create actual to formal parameter mapping
        // SAfe No need for this. It's already done by PreparedStatement_base.
        // ParameterUtils.createParameterMapping(rawQueryString, parameterListIn, tds);

        // MJH - OK now clone parameter details minus data values.
        // copy back the formal types and check for LOBs
        _parameterList = new ParameterListItem[parameterListIn.length];

        for (int i = 0; i < _parameterList.length; i++) {
            _parameterList[i] = (ParameterListItem) parameterListIn[i].clone();
            _parameterList[i].value = null; // MJH allow value to be garbage collected

            if (_parameterList[i].formalType.equalsIgnoreCase("image")
                || _parameterList[i].formalType.equalsIgnoreCase("text")
                || _parameterList[i].formalType.equalsIgnoreCase("ntext")) {
                _hasLobs = true;
            }
        }

        // Create the procedure text; allocate a buffer large enough for the raw string
        // plus a little extra for the procedure name and parameters.
        StringBuffer procedureString = new StringBuffer(rawQueryString.length()
                                                        + 128);

        procedureString.append("create proc ");
        procedureString.append(sqlProcedureName);

        // add the formal parameter list to the sql string
        createFormalParameterList(_parameterList, procedureString);
        procedureString.append(" as ");

        // Go through the raw sql and substitute a parameter name
        // for each '?' placeholder in the raw sql
        boolean inString = false;
        char[] chars = rawQueryString.toCharArray(); // avoid getfield opcode
        int nextParameterIndex = 0;

        for (int i = 0; i < chars.length; i++) {
            char ch = chars[i]; // avoid getfield opcode

            if (!inString) {
                if (ch == '?') {
                    procedureString.append('@');
                    procedureString.append(_parameterList[nextParameterIndex].formalName);
                    nextParameterIndex++;
                } else {
                    if (ch == '\'') {
                        inString = true;
                    }

                    procedureString.append(ch);
                }
            } else {
                // NOTE: This works even if a single quote is being used as an escape
                // character since the next single quote found will force the state
                // to be inString == true again.
                if (ch == '\'') {
                    inString = false;
                }

                procedureString.append(ch);
            }
        }

        _procedureString = procedureString.toString();
    }

    public String getPreparedSqlString() {
        return _procedureString;
    }

    public String getProcedureName() {
        return _sqlProcedureName;
    }

    public ParameterListItem getParameter(int index) {
        return _parameterList[index];
    }

    private void createFormalParameterList(ParameterListItem[] parameterList,
                                           StringBuffer result) {
        if (parameterList.length > 0) {
            result.append('(');

            for (int i = 0; i < parameterList.length; i++) {
                if (i > 0) {
                    result.append(", ");
                }

                result.append('@');
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

    public ParameterListItem[] getParameterList() {
        return _parameterList;
    }

    public String getSignature() {
        return _signature;
    }

    /**
     * Check if the formal parameter list includes LOB parameters.
     * MJH This method is not actually used at present.
     */
    public boolean hasLobParameters() {
        return _hasLobs;
    }
}
