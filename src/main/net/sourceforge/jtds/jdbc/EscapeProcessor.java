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

abstract public class EscapeProcessor
{
    public static final String cvsVersion = "$Id: EscapeProcessor.java,v 1.6 2004-01-27 23:16:22 bheineman Exp $";

    private static final String ESCAPE_PREFIX_DATE = "d ";
    private static final String ESCAPE_PREFIX_TIME = "t ";
    private static final String ESCAPE_PREFIX_TIMESTAMP = "ts ";
    private static final String ESCAPE_PREFIX_OUTER_JOIN = "oj ";
    private static final String ESCAPE_PREFIX_FUNCTION = "fn ";
    private static final String ESCAPE_PREFIX_CALL = "call ";
    private static final String ESCAPE_PREFIX_ESCAPE_CHAR = "escape ";

    private static final int NORMAL = 0;
    private static final int IN_STRING = 1;
    private static final int IN_ESCAPE = 2;

    private String input;

    public EscapeProcessor(String sql)
    {
        input = sql;
    }

    abstract public String expandDBSpecificFunction(String escapeSequence) throws SQLException;

    /**
     * Is the string made up only of digits?
     * <p>
     * <b>Note:</b>  Leading/trailing spaces or signs are not considered digits.
     *
     * @return true if the string has only digits, false otherwise.
     */
    private static boolean validDigits(String str)
    {
        for( int i=0; i<str.length(); i++ )
            if( !Character.isDigit(str.charAt(i)) )
                return false;

        return true;
    }

    /**
     * Given a string and an index into that string return the index
     * of the next non-whitespace character.
     *
     * @return index of next non-whitespace character.
     */
    private static int skipWhitespace(String str, int i)
    {
        while( i<str.length() && Character.isWhitespace(str.charAt(i)) )
           i++;

        return i;
    }

    /**
     * Given a string and an index into that string, advanvance the index
     * iff it is on a quote character.
     *
     * @return the next position in the string iff the current character is a
     *         quote
     */
    private static int skipQuote(String str, int i)
    {
        // skip over the leading quote if it exists
        if( i<str.length() && (str.charAt(i)=='\'' || str.charAt(i)=='"') )
            // XXX Note-  The spec appears to prohibit the quote character,
            // but many drivers allow it.  We should probably control this
            // with a flag.
            i++;

        return i;
    }

    /**
     * Convert a JDBC SQL escape date sequence into a datestring recognized by SQLServer.
     */
    private static String getDate(String str) throws SQLException
    {
        int   i;

        // skip over the "d "
        i = 2;

        // skip any additional spaces
        i = skipWhitespace(str, i);

        i = skipQuote(str, i);

        // i is now up to the point where the date had better start.
        if( str.length()-i<10 || str.charAt(i+4)!='-' || str.charAt(i+7)!='-' )
            throw new SQLException("Malformed date");

        String year  = str.substring(i, i+4);
        String month = str.substring(i+5, i+5+2);
        String day   = str.substring(i+5+3, i+5+3+2);

        // Make sure the year, month, and day are numeric
        if( !validDigits(year) || !validDigits(month) || !validDigits(day) )
            throw new SQLException("Malformed date");

        // Make sure there isn't any garbage after the date
        i = i+10;
        i = skipWhitespace(str, i);
        i = skipQuote(str, i);
        i = skipWhitespace(str, i);

        if( i<str.length() )
            throw new SQLException("Malformed date");

        return "'" + year + month + day + "'";
    }

    /**
     * Convert a JDBC SQL escape time sequence into a time string recognized by SQLServer.
     */
    private static String getTime(String str) throws SQLException
    {
        int   i;

        // skip over the "t "
        i = 2;

        // skip any additional spaces
        i = skipWhitespace(str, i);

        i = skipQuote(str, i);

        // i is now up to the point where the date had better start.
        if( str.length()-i<8 || str.charAt(i+2)!=':' || str.charAt(i+5)!=':' )
            throw new SQLException("Malformed time");

        String hour   = str.substring(i, i+2);
        String minute = str.substring(i+3, i+3+2);
        String second = str.substring(i+3+3, i+3+3+2);

        // Make sure the year, month, and day are numeric
        if( !validDigits(hour) || !validDigits(minute) || !validDigits(second) )
            throw new SQLException("Malformed time");

        // Make sure there isn't any garbage after the time
        i = i+8;
        i = skipWhitespace(str, i);
        i = skipQuote(str, i);
        i = skipWhitespace(str, i);

        if( i<str.length() )
            throw new SQLException("Malformed time");

        return "'" + hour + ":" + minute + ":" + second + "'";
    }

    /**
     * Convert a JDBC SQL escape timestamp sequence into a date-time string recognized by SQLServer.
     */
    private static String getTimestamp(String str) throws SQLException
    {
        int   i;

        // skip over the "d "
        i = 2;

        // skip any additional spaces
        i = skipWhitespace(str, i);

        i = skipQuote(str, i);

        // i is now at the point where the date had better start.
        if( (str.length()-i)<19 || str.charAt(i+4)!='-' || str.charAt(i+7)!='-' )
            throw new SQLException("Malformed date");

        String year  = str.substring(i, i+4);
        String month = str.substring(i+5, i+5+2);
        String day   = str.substring(i+5+3, i+5+3+2);

        // Make sure the year, month, and day are numeric
        if( !validDigits(year) || !validDigits(month) || !validDigits(day) )
            throw new SQLException("Malformed date");

        // Make sure there is at least one space between date and time
        i = i+10;
        if( !Character.isWhitespace(str.charAt(i)) )
            throw new SQLException("Malformed date");

        // skip the whitespace
        i = skipWhitespace(str, i);

        // see if it could be a time
        if( str.length()-i<8 || str.charAt(i+2)!=':' || str.charAt(i+5)!=':' )
            throw new SQLException("Malformed time");

        String hour     = str.substring(i, i+2);
        String minute   = str.substring(i+3, i+3+2);
        String second   = str.substring(i+3+3, i+3+3+2);
        String fraction = "000";
        i = i+8;
        if( str.length()>i && str.charAt(i)=='.' )
        {
            fraction = "";
            i++;
            while( str.length()>i && validDigits(str.substring(i,i+1)) )
                fraction = fraction + str.substring(i,++i);

            if( fraction.length() > 3 )
                fraction = fraction.substring(0,3);
            else
                while( fraction.length()<3 )
                    fraction = fraction + "0";
        }


        // Make sure there isn't any garbage after the time
        i = skipWhitespace(str, i);
        i = skipQuote(str, i);
        i = skipWhitespace(str, i);

        if (i<str.length())
        {
           throw new SQLException("Malformed date");
        }

        return "'" + year + month + day + " "
                + hour + ":" + minute + ":" + second + "." + fraction + "'";
    }

    public String expandEscape(String escapeSequence) throws SQLException
    {
        // XXX Is it always okay to trim leading and trailing blanks?
        String str    = escapeSequence.trim();
        String result = null;

        if( startsWithIgnoreCase(str, ESCAPE_PREFIX_FUNCTION) )
        {
            str = str.substring(ESCAPE_PREFIX_FUNCTION.length());
            result = expandCommonFunction(str);

            if( result == null )
                result = expandDBSpecificFunction(str);

            if( result == null )
                result = str;
        }
        else if( startsWithIgnoreCase(str, ESCAPE_PREFIX_CALL) || str.startsWith("?") )
        {
            boolean returnsVal = str.startsWith("?");

            if (returnsVal) {
                int i = skipWhitespace(str, 1);

                if (str.charAt(i) != '=') {
                    throw new SQLException("Malformed procedure call, '=' expected at "
                                           + i + ": " + escapeSequence);
                }

                i = skipWhitespace(str, i + 1);

                str = str.substring(i);

                if (!startsWithIgnoreCase(str, ESCAPE_PREFIX_CALL)) {
                    throw new SQLException("Malformed procedure call, '"
                                           + ESCAPE_PREFIX_CALL + "' expected at "
                                           + i + ": " + escapeSequence);
                }
            }

            str = str.substring(ESCAPE_PREFIX_CALL.length()).trim();

            int pPos = str.indexOf('(');
            if( pPos >= 0 )
            {
                if( str.charAt(str.length()-1) != ')' )
                    throw new SQLException("Malformed procedure call, ')' expected at "
                                           + (escapeSequence.length() - 1) + ": "
                                           + escapeSequence);
                result = "exec "+(returnsVal ? "?=" : "")+str.substring(0, pPos)+
                    " "+str.substring(pPos+1, str.length()-1);
            }
            else
                result = "exec "+(returnsVal ? "?=" : "")+str;
        }
        else if( startsWithIgnoreCase(str, ESCAPE_PREFIX_DATE) )
            result = getDate(str);
        else if( startsWithIgnoreCase(str, ESCAPE_PREFIX_TIME) )
            result = getTime(str);
        else if( startsWithIgnoreCase(str, ESCAPE_PREFIX_TIMESTAMP) )
            result = getTimestamp(str);
        else if( startsWithIgnoreCase(str, ESCAPE_PREFIX_OUTER_JOIN) )
            result = str.substring(ESCAPE_PREFIX_OUTER_JOIN.length()).trim();
        else if( startsWithIgnoreCase(str, ESCAPE_PREFIX_ESCAPE_CHAR) )
            result = getEscape(str);
        else
            throw new SQLException("Unrecognized escape sequence: " + escapeSequence);

        return result;
    }

    /**
     * Expand functions that are common to both SQLServer and Sybase
     */
    public String expandCommonFunction(String str) throws SQLException
    {
        String result = null;
        int pPos = str.indexOf('(');

        if( pPos < 0 )
            throw new SQLException("Malformed function escape, expected '(': " + str);
        else if (str.charAt(str.length() - 1) != ')')
            throw new SQLException("Malformed function escape, expected ')' at"
                                   + (str.length() - 1) + ": " + str);

        String fName = str.substring(0, pPos).trim();

        // @todo Implement this in a smarter way
        // ??Can we use HashMaps or are we trying to be java 1.0 / 1.1 compliant??
        if( fName.equalsIgnoreCase("user") )
            result = "user_name" + str.substring(pPos);
        else if( fName.equalsIgnoreCase("database") )
            result = "db_name" + str.substring(pPos);
        else if( fName.equalsIgnoreCase("ifnull") )
            result = "isnull" + str.substring(pPos);
        else if( fName.equalsIgnoreCase("now") )
            result = "getdate" + str.substring(pPos);
        else if( fName.equalsIgnoreCase("atan2") )
            result = "atn2" + str.substring(pPos);
        else if( fName.equalsIgnoreCase("length") )
            result = "len" + str.substring(pPos);
        else if( fName.equalsIgnoreCase("locate") )
            result = "charindex" + str.substring(pPos);
        else if( fName.equalsIgnoreCase("repeat") )
            result = "replicate" + str.substring(pPos);
        else if( fName.equalsIgnoreCase("insert") )
            result = "stuff" + str.substring(pPos);
        else if( fName.equalsIgnoreCase("lcase") )
            result = "lower" + str.substring(pPos);
        else if( fName.equalsIgnoreCase("ucase") )
            result = "upper" + str.substring(pPos);
        else if (fName.equalsIgnoreCase("concat"))
            result = getConcat(str.substring(pPos));

        return result;
    }

    /**
     * Returns the JDBC function CONCAT as a database concatenation string.
     * <p>
     * Per the specification, concatenation uses double quotes instead of single quotes
     * for string literals:
     * {fn CONCAT("Hot", "Java")}
     * {fn CONCAT(column1, "Java")}
     * {fn CONCAT("Hot", column2)}
     * {fn CONCAT(column1, column2)}
     * <p>
     * See: http://java.sun.com/j2se/1.3/docs/guide/jdbc/spec/jdbc-spec.frame11.html
     */
    private String getConcat(String str) throws SQLException {
        char[] chars = str.toCharArray();
        StringBuffer result = new StringBuffer(chars.length);
        boolean inString = false;

        for (int i = 0; i < chars.length; i++ ) {
            char ch = chars[i];

            if (inString) {
                if (ch == '\'') {
                    // Single quotes must be escaped with another single quote
                    result.append("''");
                } else if (ch == '"' && chars[i] + 1 != '"') {
                    // What happens when ch = '"' && chars[i] + 1 == '"'
                    // Is this a case where a double quote is escaping a double quote?
                    result.append("'");
                    inString = false;
                } else {
                    result.append(ch);
                }
            } else {
                if (ch == ',') {
                    // {fn CONCAT("Hot", "Java")}
                    // The comma       ^  separating the parameters was found, simply
                    // replace if with a '+'.
                    result.append('+');
                } else if (ch == '"') {
                    result.append("'");
                    inString = true;
                } else if (ch == '(' || ch == ')') {
                    result.append(ch);
                } else if (Character.isWhitespace(ch)) {
                    // Just ignore whitespace, there is no reason to make the database
                    // parse this data as well.
                } else {
                    throw new SQLException("Malformed concat function, charcter '"
                                           + ch + "' was not expected at " + i + ": "
                                           + str);
                }
            }
        }

        return result.toString();
    }

    /**
     * Returns ANSI ESCAPE sequence with the specified escape character
     */
    public String getEscape(String str) throws SQLException
    {
        String tmpStr = str.substring(ESCAPE_PREFIX_ESCAPE_CHAR.length()).trim();

        // Escape string should be 3 characters long unless a single quote is being used
        // (which is probably a bad idea but the ANSI specification allows it) as an
        // escape character in which case the length should be 4.  The escape
        // sequence needs to be handled in this manner (with two single quotes) or else
        // parameters will not be counted properly as the string will remain open.
        if( !((tmpStr.length()==3 && tmpStr.charAt(1)!='\'')
              || (tmpStr.length()==4 && tmpStr.charAt(1)=='\'' && tmpStr.charAt(2)=='\''))
            || tmpStr.charAt(0)!='\''
            || tmpStr.charAt(tmpStr.length() - 1)!='\'' )
            throw new SQLException("Malformed escape: " + str);

        return "ESCAPE " + tmpStr;
    }

    /**
     * Converts JDBC escape syntax to native SQL.
     * <p>
     * NOTE: This method is now structured the way it is for optimization purposes.
     * <p>
     * if (state==NORMAL) else if (state==IN_STRING) else
     * replaces the
     * switch (state) case NORMAL: case IN_STRING
     * as it is faster when there are few case statements.
     * <p>
     * Also, IN_ESCAPE is not checked for and a simple 'else' is used instead as it is the
     * only other state that can exist.
     * <p>
     * char ch = chars[i] is used in conjunction with input.toCharArray() to avoid
     * getfield opcode.
     * <p>
     * If any changes are made to this method, please test the performance of the change
     * to ensure that there is no degradation.  The cost of parsing SQL for JDBC escapes
     * needs to be as close to zero as possible.
     */
    public String nativeString() throws SQLException {
        char[] chars = input.toCharArray(); /* avoid getfield opcode */
        StringBuffer result = new StringBuffer(chars.length);
        StringBuffer escape = null;
        int state = NORMAL;

        for (int i = 0; i < chars.length; i++ ) {
            char ch = chars[i]; /* avoid getfield opcode */

            if (state == NORMAL) {
                if (ch == '{') {
                    state = IN_ESCAPE;

                    if (escape == null) {
                        escape = new StringBuffer();
                    } else {
                        escape.delete(0, escape.length());
                    }
                } else {
                   result.append(ch);

                   if (ch == '\'') {
                       state = IN_STRING;
                   }
                }
            } else if (state == IN_STRING) {
                result.append(ch);

                // NOTE: This works even if a single quote is being used as an escape
                // character since the next single quote found will force the state
                // to be == to IN_STRING again.
                if (ch == '\'') {
                    state = NORMAL;
                }
            } else { // state == IN_ESCAPE
                if (ch == '}') {
                    result.append(expandEscape(escape.toString()));
                    state = NORMAL;
                } else {
                    escape.append(ch);
                }
            }
        }

        if (state == IN_ESCAPE) {
            throw new SQLException("Syntax error in SQL escape syntax");
        }

        return result.toString();
    }


    public static boolean startsWithIgnoreCase(String s, String prefix)
    {
        if( s.length() < prefix.length() )
            return false;

        for( int i=prefix.length()-1; i>=0; i-- )
            if( Character.toLowerCase(s.charAt(i)) != Character.toLowerCase(prefix.charAt(i)) )
                return false;

        return true;
    }
}
