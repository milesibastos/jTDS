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


public class Procedure
{
   	public static final String cvsVersion = "$Id: Procedure.java,v 1.4 2003-12-22 00:33:06 alin_sinpalean Exp $";


   	private ParameterListItem  parameterList[]    = null;
   	private String             sqlProcedureName   = null;
   	private String             procedureString    = "";
   	private String             signature          = null;
   	private boolean    		   hasLobs            = false;

   	public Procedure(String         	 rawQueryString,
   					 String 			 signature,
                	 String              sqlProcedureName,
                	 ParameterListItem[] parameterListIn,
                	 Tds                 tds)
      throws SQLException
	{
   	  	this.signature        = signature;
      	this.sqlProcedureName = sqlProcedureName;

      	// Create the procedure text
		StringBuffer procedureString = new StringBuffer(128);

        procedureString.append("create proc ");
        procedureString.append(sqlProcedureName);

        // Create actual to formal parameter mapping
        ParameterUtils.createParameterMapping(rawQueryString, parameterListIn, tds);

        // MJH - OK now clone parameter details minus data values.
        // copy back the formal types and check for LOBs
        parameterList    = new ParameterListItem[parameterListIn.length];
        for (int i = 0; i < parameterList.length; i++)
        {
        	parameterList[i] = (ParameterListItem)parameterListIn[i].clone();
            parameterList[i].value = null; // MJH allow value to be garbage collected
            if( parameterList[i].formalType.equalsIgnoreCase("image") ||
                parameterList[i].formalType.equalsIgnoreCase("text")  ||
                parameterList[i].formalType.equalsIgnoreCase("ntext") )
            {
               hasLobs = true;
            }
        }
        // add the formal parameter list to the sql string
        procedureString.append(createFormalParameterList(parameterList));

        procedureString.append(" as ");

        // Go through the raw sql and substitute a parameter name
        // for each '?' placeholder in the raw sql

        // SAfe Seems like '\' (backslash) doesn't actually work as
        //      an escape character, so we'll just ignore it
        StringTokenizer   st = new StringTokenizer(rawQueryString,
                                                    "'?", true);
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
                        procedureString.append('@');
                        procedureString.append(
                            parameterList[nextParameterIndex].formalName);
                        nextParameterIndex++;
                    }
                    else if (current.equals("'"))
                    {
                        procedureString.append(current);
                        state = inString;
                    }
                    else
                    {
                        procedureString.append(current);
                    }
                    break;
                }
                case inString:
                {
                    if (current.equals("'"))
                        state = normal;
                    // SAfe Seems like '\' (backslash) doesn't actually work as
                    //      an escape character, so we'll just ignore it
                    // else if (current.equals("\\"))
                    //     state = inEscape;

                    procedureString.append(current);
                    break;
                }
                case inEscape:
                {
                    state = inString;
                    procedureString.append(current);
                    break;
                }
                default:
                {
                    throw new SQLException("Internal error.  Bad State " + state);
                }
            }
        }

        this.procedureString = procedureString.toString();
   	}

   	public String getPreparedSqlString()
   	{
      	return this.procedureString;
   	}

   	public String getProcedureName()
   	{
      	return this.sqlProcedureName;
   	}

   	public ParameterListItem getParameter(int index)
   	{
      	return this.parameterList[index];
   	}

   	private String createFormalParameterList(ParameterListItem[]  parameterList)
   	{
      	int i;
      	StringBuffer result = new StringBuffer(128);

      	if( parameterList.length > 0 )
      	{
         	result.append('(');
        	for( i=0; i<parameterList.length; i++ )
         	{
                if( i > 0 )
                    result.append(", ");
                result.append('@');
                result.append(parameterList[i].formalName);
                result.append(' ');
                result.append(parameterList[i].formalType);
                if (parameterList[i].isOutput)
                    result.append(" output");
            }
            result.append(')');
        }

        return result.toString();
    }

    public ParameterListItem[] getParameterList()
    {
        return this.parameterList;
    }

    public String getSignature()
    {
        return this.signature;
    }

    /**
     * Check if the formal parameter list includes LOB parameters.
     * MJH This method is not actually used at present.
     */
    public boolean hasLobParameters()
    {
        return this.hasLobs;
    }
}
