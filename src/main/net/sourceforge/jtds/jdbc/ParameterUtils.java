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

import java.sql.*;

public class ParameterUtils {
    public static final String cvsVersion = "$Id: ParameterUtils.java,v 1.10 2004-02-25 01:24:47 alin_sinpalean Exp $";

    /**
     * Check that all items in parameterList have been given a value
     *
     * @exception SQLException thrown if one or more parameters aren't set
     */
    private static void verifyThatParametersAreSet(ParameterListItem[] parameterList)
            throws SQLException {
        for (int i = 0; i < parameterList.length; i++) {
            if (parameterList[i].isOutput) {
                // Allow output parameters to be "not set"
                parameterList[i].isSet = true;
            }

            if (!parameterList[i].isSet) {
                throw new SQLException("parameter #" + (i + 1) + " has not been set");
            }
        }
    }

    /**
     * Create the formal parameters for a parameter list.
     *
     * This method takes a sql string and a parameter list containing
     * the actual parameters and creates the formal parameters for
     * a stored procedure.  The formal parameters will later be used
     * when the stored procedure is submitted for creation on the server.
     *
     * @param rawQueryString   (in-only) the original SQL with '?' placehoders
     * @param parameterList    (update) the parameter list to update
     * @param tds              <code>Tds</code> the procedure will be executed
     *                         on
     * @param assignNames      if <code>true</code>, names are assigned to
     *                         parameters (for <code>PreparedStatement</code>s)
     */
    public static void createParameterMapping(String rawQueryString,
                                              ParameterListItem[] parameterList,
                                              Tds tds,
                                              boolean assignNames)
            throws SQLException {
        // First make sure the caller has filled in all the parameters.
        verifyThatParametersAreSet(parameterList);

        int nextParameterNumber = 0;
        int tdsVer = tds.getTdsVer();
        EncodingHelper encoder = tds.getEncoder();

        for (int i = 0; i < parameterList.length; i++) {
            if (assignNames) {
                String nextFormal;
                do {
                    nextParameterNumber++;
                    nextFormal = "P" + nextParameterNumber;
                } while (-1 != rawQueryString.indexOf(nextFormal));

                parameterList[i].formalName = '@' + nextFormal;
            }

            switch (parameterList[i].type) {
                case java.sql.Types.VARCHAR:
                case java.sql.Types.CHAR:
                {
                    String value = (String)parameterList[i].value;

                    if (tdsVer == Tds.TDS70) {
                        // SQL Server 7 can handle Unicode so use it wherever
                        // possible AND required
                        if ((value == null || value.length() < 4001)
                            && tds.useUnicode()) {
                            parameterList[i].formalType = "nvarchar(4000)";
                            parameterList[i].maxLength = 4000;
                        } else if (value == null ||
                                    (value.length() < 8001
                                     && !encoder.isDBCS()
                                     && encoder.canBeConverted(value))) {
                            parameterList[i].formalType = "varchar(8000)";
                            parameterList[i].maxLength = 8000;
                        } else {
                            // SAfe We'll always use NTEXT (not TEXT) because CLOB
                            //      columns can't be index columns, so there's no
                            //      performance hit
                            parameterList[i].formalType = "ntext";
                            parameterList[i].maxLength = Integer.MAX_VALUE;
                        }
                    } else {
                        int len = 0; // This is for NULL values

                        if (value != null) {
                            len = value.length();

                            if (encoder.isDBCS() && len > 127 && len < 256) {
                                len = encoder.getBytes(value).length;
                            }
                        }

                        if (len < 256) {
                            parameterList[i].formalType = "varchar(255)";
                            parameterList[i].maxLength = 255;
                        } else {
                            parameterList[i].formalType = "text";
                            parameterList[i].maxLength = Integer.MAX_VALUE;
                        }
                    }
                    break;
                }
                case java.sql.Types.LONGVARCHAR:
                {
                    if (tdsVer == Tds.TDS70) {
                        parameterList[i].formalType = "ntext";
                    } else {
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
                {
                    parameterList[i].formalType = "image";
                    break;
                }
                case java.sql.Types.BINARY:
                case java.sql.Types.VARBINARY:
                {
                    parameterList[i].formalType = "varbinary(8000)";
                    break;
                }
                case java.sql.Types.BIT:
                {
                    parameterList[i].formalType = "bit";
                    break;
                }
                case java.sql.Types.BIGINT: {
                    parameterList[i].formalType = "decimal("
                            + tds.getConnection().getMaxPrecision() + ",0)";
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
                    int scale = parameterList[i].scale;
                    if (scale == -1) {
                        // @todo Find a way to do this properly (i.e. what
                        //       happens if the scale is larger than the
                        //       precision, e.g. if the BigDecimal was created
                        //       from a float/double - try AsTest)
                        // if (parameterList[i].value instanceof BigDecimal) {
                        //     scale = ((BigDecimal) parameterList[i].value).scale();
                        //     System.out.println("scale = " + scale);
                        //     new Exception().printStackTrace();
                        // } else {
                            scale = 10;
                        // }
                    }
                    parameterList[i].formalType = "decimal("
                            + tds.getConnection().getMaxPrecision() + ","
                            + scale + ")";
                    break;
                }
                case java.sql.Types.NULL:
                case java.sql.Types.OTHER:
                {
                    throw new SQLException("Not implemented (type is java.sql.Types."
                                           + TdsUtil.javaSqlTypeToString(parameterList[i].type)
                                           + ")");
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
