// jTDS JDBC Driver for Microsoft SQL Server
// Copyright (C) 2004 The jTDS Project
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either
// version 2.1 of the License, or (at your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
//
package net.sourceforge.jtds.jdbc;

import java.util.ArrayList;
import java.util.HashMap;
import java.sql.SQLException;

/**
 * Process JDBC escape strings and parameter markers in the SQL string.
 * <p>
 * This code recognizes the following escapes:
 * <ol>
 * <li>Date      {d 'yyyy-mm-dd'}
 * <li>Time      {t 'hh:mm:ss'}
 * <li>Timestamp {ts 'yyyy-mm-dd hh:mm:ss.nnn'}
 * <li>ESCAPE    {escape 'x'}
 * <li>Function  {fn xxxx([arg,arg...])}
 * NB The concat(arg, arg) operator is converted to (arg + arg)
 * <li>OuterJoin {oj .....}
 * <li>Call      {?=call proc [arg, arg...]}
 * or        {call proc [arg, arg...]}
 * </ol>
 * Notes: This code is designed to be as efficient as possible and as
 * result the validation done here is limited.
 *
 * @author Mike Hutchinson
 * @version $Id: SQLParser.java,v 1.1 2004-06-27 17:00:53 bheineman Exp $
 */
class SQLParser {
    /** Input buffer with SQL statement. */
    private char[] in;
    /** Current position in input buffer. */
    private int    s;
    /** Length of input buffer. */
    private int    len;
    /** Output buffer to contain parsed SQL. */
    private char[] out;
    /** Current position in output buffer. */
    private int    d;
    /** Parameter list to be populated. */
    private ArrayList params;
    /** Current expected terminator character. */
    private char   terminator;
    /** Procedure name in call escape. */
    private String procName;
    /** Current character. */
    private char ch = ' ';
    /** End of statement marker. CTRL/Z */
    private static final char EOF = 26;
    /** Start of next token. */
    private int tokenStart;

    /**
     * Construct a new Parser object to process the supplied SQL.
     * @param sql The SQL statement to parse.
     * @param paramList The Parameter list array to populate.
     */
    SQLParser(String sql, ArrayList paramList)
    {
        in = sql.toCharArray();
        len = in.length;
        out = new char[len+16];
        s = 0;
        d = 0;
        params = paramList;
        procName = "";
    }

    /**
     * Construct a new Parser object to parse tokens in the supplied SQL.
     * @param sql The SQL statement to parse.
     */
    SQLParser(String sql)
    {
        in = sql.toCharArray();
        len = in.length;
        s = 0;
        d = 0;
        procName = "";
    }

    /**
     * Insert a String literal in the output buffer.
     * @param txt The text to insert.
     */
    private void copyLiteral(String txt)
    {
        for (int i = 0; i < txt.length(); i++)
            out[d++] = txt.charAt(i);
    }

    /**
     * Copy over an embedded string literal unchanged.
     */
    private void copyString()
    {
        char saveTc = terminator;
        char tc = in[s];
        if (tc == '[') tc = ']';
        terminator = tc;
        do {
            out[d++] = in[s++];
            while (in[s] != tc) out[d++] = in[s++];
            out[d++] = in[s++];
        } while (s < len && in[s] == tc);
        terminator = saveTc;
    }

    /**
     * Build a new parameter item.
     * @param name Optional parameter name or null.
     * @param pos The parameter marker position in the output buffer.
     */
    private void copyParam(String name, int pos)
    {
        ParamInfo pi = new ParamInfo(pos);
        pi.name = name;
        if (pos >= 0) {
            out[d++] = in[s++];
        } else {
            pi.isRetVal = true;
            s++;
        }
        params.add(pi);       }

    /**
     * Copy an embedded stored procedure identifier over to the output buffer.
     * @return The identifier as a <code>String</code>.
     */
    private String copyProcName()
    {
        int start = d;
        do {
            if (in[s] == '"' || in[s] == '[') {
                copyString();
            } else {
                char c = in[s++];
                while (Character.isJavaIdentifierPart(c) || c == '#') {
                    out[d++] = c;
                    c = in[s++];
                }
                s--;
            }
            if (in[s] == '.') {
                while (in[s] == '.')
                    out[d++] = in[s++];
            } else  {
                break;
            }
        } while (true);
        return new String(out, start, d-start);
    }

    /**
     * Copy an embedded parameter name to the output buffer.
     * @return The identifier as a <code>String</code>.
     */
    private String copyParamName()
    {
        int start = d;
        char c = in[s++];
        while (Character.isJavaIdentifierPart(c) || c == '@') {
           out[d++] = c;
           c = in[s++];
        }
        s--;
        return new String(out, start, d-start);
    }

    /**
     * Check that the next character is as expected.
     * @param c The expected character.
     * @param copy True if found character should be copied.
     * @throws SQLException if expected characeter not found.
     */
    private void mustbe(char c, boolean copy)
        throws SQLException
    {
        if (in[s] != c)
            throw new SQLException(Support.getMessage("error.parsesql.mustbe",
                                                      String.valueOf(s),
                                                      String.valueOf(c)),
                                   "22019");
        if (copy)
            out[d++] = in[s++];
        else
            s++;
    }

    /**
     * Skip embedded white space.
     */
    private void skipWhiteSpace()
    {
        while (Character.isWhitespace(in[s])) s++;
    }

    /**
     * Process the JDBC {call procedure [(&#63;,&#63;,&#63;)]} type escape.
     * @throws SQLException
     */
    private void callEscape()
        throws SQLException
    {
        // Insert EXECUTE into SQL so that proc can be called as normal SQL
        copyLiteral("EXECUTE ");
        // Process procedure name
        procName = copyProcName();
        skipWhiteSpace();
        if (in[s] == '(') { // Optional ( )
            s++; skipWhiteSpace();
            terminator = ')';
        } else {
            terminator = '}';
        }
        // Process any parameters
        while (in[s] != terminator) {
            out[d++] = ' ';
            String name = null;
            if (in[s] == '@') {
                // Named parameter
                name = copyParamName();
                skipWhiteSpace();
                mustbe('=', true);
                skipWhiteSpace();
                if (in[s] == '?')
                    copyParam(name, d);
                else {
                    // Named param has literal value can't call as RPC
                    procName = "";
                }
            } else
            if (in[s] == '?') {
                copyParam(name, d);
            } else {
                // Literal parameter can't call as RPC
                procName = "";
            }
            // Now find terminator or comma
            while (in[s] != terminator && in[s] != ',') {
                if (in[s] == '\'' || in[s] == '[' || in[s] == '"')
                    copyString();
                else
                    out[d++] = in[s++];
            }
            if (in[s] == ',') {
                out[d++] = in[s++];
            }
            skipWhiteSpace();
        }
        if (terminator == ')')
            s++; // Elide
    }

    /**
     * Utility routine to validate date and time escapes.
     * @param mask The validation mask
     * @return True if the escape was valid and processed OK.
     */
    private boolean getDateTimeField(byte[] mask)
    {
        out[d++] = '\'';
        skipWhiteSpace();
        terminator = (in[s] == '\'' || in[s] == '"')? in[s++]: '}';
        skipWhiteSpace();
        int ptr = 0;
        while (ptr < mask.length) {
            char c = in[s++];
            if (c == ' ' && out[d-1] == ' ')
                continue; // Eliminate multiple spaces
            if (mask[ptr] == '#') {
                if (!Character.isDigit(c))
                    return false;
            } else
            if (mask[ptr] != c)
                return false;
            if (c != '-')
                out[d++] = c;
            ptr++;
        }
        if (mask.length == 19) { // Timestamp
            int digits = 0;
            if (in[s] == '.') {
                out[d++] = in[s++];
                while (Character.isDigit(in[s])) {
                    if (digits < 3) {
                        out[d++] = in[s++];
                        digits++;
                    } else
                        s++;
                }
            } else {
                out[d++] = '.';
            }
            for (; digits < 3; digits++)
                out[d++] = '0';
        }
        skipWhiteSpace();
        if (in[s] != terminator)
            return false;
        if (terminator != '}')
            s++; // Skip terminator
        skipWhiteSpace();
        out[d++] = '\'';
        return true;
    }

    /** Syntax mask for time escape. */
    private static final byte[] timeMask = {
        '#','#',':','#','#',':','#','#'
    };

    /**
     * Process the JDBC escape {t 'HH:MM:SS'}.
     * @throws SQLException
     */
    private void timeEscape()
        throws SQLException
    {

        if (!getDateTimeField(timeMask))
            throw new SQLException(Support.getMessage("error.parsesql.syntax",
                                                      "time",
                                                      String.valueOf(s)),
                                   "22019");
    }

    /** Syntax mask for date escape. */
    private static final byte[] dateMask = {
        '#','#','#','#','-','#','#','-','#','#'
    };

    /**
     * Process the JDBC escape {d 'CCCC-MM-DD'}.
     * @throws SQLException
     */
    private void dateEscape()
    throws SQLException
    {
        if (!getDateTimeField(dateMask))
            throw new SQLException(Support.getMessage("error.parsesql.syntax",
                                                      "date",
                                                      String.valueOf(s)),
                                   "22019");
    }

    /** Syntax mask for timestamp escape. */
    static final byte[] timestampMask = {
        '#','#','#','#','-','#','#','-','#','#',' ',
        '#','#',':','#','#',':','#','#'
    };

    /**
     * Process the JDBC escape {ts 'CCCC-MM-DD HH:MM:SS[.NNN]'}.
     * @throws SQLException
     */
    private void timestampEscape()
    throws SQLException
    {
        if (!getDateTimeField(timestampMask))
            throw new SQLException(Support.getMessage("error.parsesql.syntax",
                                                      "timestamp",
                                                      String.valueOf(s)),
                                   "22019");
    }

    /**
     * Process the JDBC escape {oj left outer join etc}.
     * @throws SQLException
     */
    private void outerJoinEscape()
    {
        while (in[s] != '}') {
            final char c = in[s];
            switch (c) {
                case '\'':
                case '"':
                case '[':
                    copyString();
                    break;
                default:
                    out[d++] = c;
                    s++;
                    break;
            }
        }
    }

    /** Map of jdbc to sybase function names. */
    private static HashMap fnMap = new HashMap();

    static {
        fnMap.put("user",     "user_name");
        fnMap.put("database", "db_name");
        fnMap.put("ifnull",   "isnull");
        fnMap.put("now",      "getdate");
        fnMap.put("atan2",    "atn2");
        fnMap.put("length",   "len"); // should be char_length for sybase
        fnMap.put("locate",   "charindex");
        fnMap.put("repeat",   "replicate");
        fnMap.put("insert",   "stuff");
        fnMap.put("lcase",    "lower");
        fnMap.put("ucase",    "upper");
        fnMap.put("concat",   "");
    }

    /**
     * Process the JDBC escape {fn function()}.
     * @throws SQLException
     */
    private void functionEscape()
    throws SQLException
    {
        skipWhiteSpace();
        StringBuffer nameBuf = new StringBuffer();
        while (Character.isLetterOrDigit(in[s]))
            nameBuf.append(in[s++]);
        String name = nameBuf.toString().toLowerCase();
        String val = (String)fnMap.get(name);
        copyLiteral((val != null)? val: name);
        skipWhiteSpace();
        mustbe('(', true);
        while (in[s] != ')') {
            final char c = in[s];
            switch (c) {
                case '\'':
                case '"':
                case '[':
                    copyString();
                    break;
                case ',':
                    if (name.equals("concat")) {
                        out[d++] = '+'; s++;
                    } else {
                        out[d++] = c; s++;
                    }
                    break;
                default:
                    out[d++] = c; s++;
                    break;
            }
        }
        mustbe(')', true);
    }

    /**
     * Process the JDBC escape {escape 'X'}.
     * @throws SQLException
     */
    private void likeEscape()
    throws SQLException
    {
        copyLiteral("escape ");
        skipWhiteSpace();
        if (in[s] == '\'' || in[s] == '"') {
            copyString();
        } else {
            mustbe('\'', true);
        }
        skipWhiteSpace();
    }

    /**
     * Process the JDBC escape sequences.
     * @throws SQLException
     */
    private void escape()
    throws SQLException
    {
        terminator = '}';
        String esc = "";
        s++; skipWhiteSpace();
        if (in[s] == '?') {
            copyParam("@return_status", -1);
            skipWhiteSpace();
            mustbe('=', false);
            skipWhiteSpace();
            while (Character.isLetter(in[s])) esc = esc + in[s++];
            skipWhiteSpace();
            if (esc.equalsIgnoreCase("call")) {
                callEscape();
            } else {
                throw new SQLException(Support.getMessage("error.parsesql.syntax",
                                                          "call",
                                                          String.valueOf(s)),
                                       "22019");
            }
        } else {
            while (Character.isLetter(in[s])) esc = esc + in[s++];
            skipWhiteSpace();
            if (esc.equalsIgnoreCase("call")) {
                callEscape();
            } else
            if (esc.equalsIgnoreCase("t")) {
                timeEscape();
            } else
            if (esc.equalsIgnoreCase("d")) {
                dateEscape();
            } else
            if (esc.equalsIgnoreCase("ts")) {
                timestampEscape();
            } else
            if (esc.equalsIgnoreCase("oj")) {
                outerJoinEscape();
            } else
            if (esc.equalsIgnoreCase("fn")) {
                functionEscape();
            } else
            if (esc.equalsIgnoreCase("escape")) {
                likeEscape();
            } else {
                throw new SQLException(Support.getMessage("error.parsesql.badesc",
                                                          esc,
                                                          String.valueOf(s)),
                                       "22019");
            }
        }
        mustbe('}', false);
    }

    /**
     * Parse the SQL statement processing JDBC escapes and parameter markers.
     * @return The processed SQL statement and any procedure name as elements
     * 0 and 1 of the returned <code>String[]</code>.
     * @throws SQLException
     */
    String[] parse()
    throws SQLException
    {
        try {
            while (s < len) {
                final char c = in[s];
                switch (c) {
                    case '{':
                        escape();
                        break;
                    case '[':
                    case '"':
                    case '\'':
                        copyString();
                        break;
                    case '?':
                        copyParam(null, d);
                        break;
                    default:
                        out[d++] = c; s++;
                        break;
                }
            }
            String result[] = new String[2];
            // Remove leading spaces.
            int start = 0;
            while (start < out.length && out[start] == ' ')
                start++;
            // return sql and procname
            result[0] = new String(out, start, d-start);
            result[1] = procName;
            return result;
        } catch (IndexOutOfBoundsException e) {
            // Should only come here if string is invalid in some way.
            throw new SQLException(Support.getMessage("error.parsesql.missing",
                                                      String.valueOf(terminator)),
                                   "22025");
        }
    }

    /**
     * Retrieve the next character from the SQL statement.
     * @return The next character as an <code>int</code>.
     */
    private int nextCh()
    {
        ch = (s < len)? in[s++]: EOF;
        return ch;
    }

    static final int sy_end       = 0;
    static final int sy_ident     = 1;
    static final int sy_sqlword   = 2;
    static final int sy_string    = 3;
    static final int sy_number    = 4;
    static final int sy_binary    = 5;
    static final int sy_operator  = 6;
    static final int sy_misc      = 7;
    static final int sy_comma     = 8;

    /**
     * Retrieve the next lexical token from the SQL statement.
     * <p>At present this routine is just used by PreparedStatement to parse
     * the SQL statement to locate the start of a where clause. It would be
     * easy to extend this code to do more sophisticated parsing perhaps to
     * locate table names in a 'select for update' cursor statement for example.
     * @param token The StringBuffer to receive the token value.
     * @return The token type as a <code>int</code>.
     */
    int nextToken(StringBuffer token)
    {
        int sy = sy_end;

        token.setLength(0);
        while (Character.isWhitespace(ch)) nextCh();
        tokenStart = s-1;
        switch (ch) {
            case '\'':
            case '"':
                {
                    char term = ch; nextCh();
                    do {
                        while (ch != term && ch != EOF) {
                            token.append(ch); nextCh();
                        }
                        if (ch == EOF) break;
                           nextCh();
                        if (ch != term) break;
                           token.append(ch); nextCh();
                    } while (true);
                    sy = (term == '"')? sy_ident: sy_string;
                }
                break;
            case '[':
                {
                    while (ch != ']' && ch != EOF) {
                        token.append(ch); nextCh();
                    }
                    if (ch == ']') nextCh();
                    sy = sy_ident;
                }
                break;
            case '<':
                {
                    token.append(ch); nextCh();
                    if (ch == '>' || ch == '=') {
                        token.append(ch); nextCh();
                    }
                    sy = sy_operator;
                }
                break;
            case '>':
                {
                    token.append(ch); nextCh();
                    if (ch == '=') {
                        token.append(ch); nextCh();
                    }
                    sy = sy_operator;
                }
                break;
            case '!':
                {
                    token.append(ch); nextCh();
                    if (ch == '<' || ch == '>' || ch == '=') {
                        token.append(ch); nextCh();
                    }
                    sy = sy_operator;
                }
                break;
            case '+':
            case '-':
            case '/':
            case '*':
            case '=':
            case '%':
            case '^':
            case '~':
            case '&':
            case '|':
                {
                    token.append(ch); nextCh();
                    sy = sy_operator;
                }
                break;
            case ',':
                {
                    sy = sy_comma;
                }
                break;
            case EOF:
                {
                    sy = sy_end;
                }
                break;
            default:
                if (Character.isLetter(ch) || ch == '@' || ch == '#' || ch == '_') {
                    while (Character.isLetterOrDigit(ch)
                            || ch == '@' || ch == '#' || ch == '_' || ch == '$') {
                        token.append(ch); nextCh();
                    }
                    // Need to do proper lookup here
                    String tst = token.toString().toLowerCase();
                    if (tst.equals("select") ||
                        tst.equals("from") ||
                        tst.equals("where")) {
                        sy = sy_sqlword;
                    } else {
                        sy = sy_ident;
                    }
                } else
                if (Character.isDigit(ch)) {
                    token.append(ch); nextCh();
                    if (Character.toLowerCase(ch) == 'x' && token.charAt(0) == '0'){
                        token.append(ch);
                        while (Character.isDigit(ch) ||
                            (Character.toLowerCase(ch) >= 'a' && Character.toLowerCase(ch) <= 'f')) {
                                token.append(ch); nextCh();
                        }
                        sy = sy_binary;
                    } else {
                        while (Character.isDigit(ch)) {
                            token.append(ch); nextCh();
                        }
                        if (ch == '.') {
                            token.append(ch); nextCh();
                            while (Character.isDigit(ch)) {
                                token.append(ch); nextCh();
                            }
                        }
                        if (Character.toLowerCase(ch) == 'e') {
                            token.append(ch); nextCh();
                            if (ch == '-' || ch == '+') {
                                token.append(ch); nextCh();
                            }
                            while (Character.isDigit(ch)) {
                                token.append(ch); nextCh();
                            }
                        }
                        sy = sy_number;
                    }
                } else {
                    token.append(ch); nextCh();
                    sy = sy_misc;
                }
                break;
        }
        return sy;
    }

    /**
     * Retrieve the start position in the SQL statement of the last token
     * returned by the last nextToken() method call.
     *
     * @return The token start offset in the SQL statement as an <code>int</code>.
     */
    int getTokenStart() {
        return this.tokenStart;
    }
}
