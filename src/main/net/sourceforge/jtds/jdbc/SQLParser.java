// jTDS JDBC Driver for Microsoft SQL Server and Sybase
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
 * Notes:
 * <ol>
 * <li>This code is designed to be as efficient as possible and as
 * result the validation done here is limited.
 * <li>SQL comments are parsed correctly thanks to code supplied by
 * Joel Fouse.
 * </ol>
 * @author Mike Hutchinson
 * @version $Id: SQLParser.java,v 1.19 2005-02-27 14:47:17 alin_sinpalean Exp $
 */
class SQLParser {
    /** Input buffer with SQL statement. */
    private char[] in;
    /** Current position in input buffer. */
    private int s;
    /** Length of input buffer. */
    private int len;
    /** Output buffer to contain parsed SQL. */
    private char[] out;
    /** Current position in output buffer. */
    private int d;
    /** Parameter list to be populated. */
    private ArrayList params;
    /** Current expected terminator character. */
    private char terminator;
    /** Procedure name in call escape. */
    private String procName;
    /** First SQL keyword or identifier in statement. */
    private String keyWord;
    /** First table name in from clause */
    private String tableName;
    /** Connection object for server specific parsing. */
    private ConnectionJDBC2 connection;

    /**
     * Construct a new Parser object to process the supplied SQL.
     *
     * @param sql The SQL statement to parse.
     * @param paramList The Parameter list array to populate.
     */
    SQLParser(String sql, ArrayList paramList, ConnectionJDBC2 connection) {
        in = sql.toCharArray();
        len = in.length;
        out = new char[len + 256]; // Allow extra for curdate/curtime
        s = 0;
        d = 0;
        params = paramList;
        procName = "";
        this.connection = connection;
    }

    /**
     * Insert a String literal in the output buffer.
     *
     * @param txt The text to insert.
     */
    private void copyLiteral(String txt) {
        for (int i = 0; i < txt.length(); i++) {
            char c = txt.charAt(i);

            if (c == '?') {
                // param marker embedded in escape
                ParamInfo pi = new ParamInfo(d, connection.isUseUnicode());
                params.add(pi);
            }

            out[d++] = c;
        }
    }

    /**
     * Copy over an embedded string literal unchanged.
     */
    private void copyString() {
        char saveTc = terminator;
        char tc = in[s];

        if (tc == '[') {
            tc = ']';
        }

        terminator = tc;

        do {
            out[d++] = in[s++];

            while (in[s] != tc) {
                out[d++] = in[s++];
            }

            out[d++] = in[s++];
        } while (s < len && in[s] == tc);

        terminator = saveTc;
    }

    /**
     * Copy over possible SQL keyword eg 'SELECT'
     */
    private String copyKeyWord() {
        int start = d;

        while (s < len && Character.isJavaIdentifierPart(in[s])) {
            out[d++] = in[s++];
        }

        return String.valueOf(out, start, d - start).toLowerCase();
    }

    /**
     * Build a new parameter item.
     *
     * @param name Optional parameter name or null.
     * @param pos The parameter marker position in the output buffer.
     */
    private void copyParam(String name, int pos) {
        ParamInfo pi = new ParamInfo(pos, connection.isUseUnicode());
        pi.name = name;

        if (pos >= 0) {
            out[d++] = in[s++];
        } else {
            pi.isRetVal = true;
            s++;
        }

        params.add(pi);
    }

    /**
     * Copy an embedded stored procedure identifier over to the output buffer.
     *
     * @return The identifier as a <code>String</code>.
     */
    private String copyProcName() throws SQLException {
        int start = d;

        do {
            if (in[s] == '"' || in[s] == '[') {
                copyString();
            } else {
                char c = in[s++];

                while (Character.isJavaIdentifierPart(c) || c == '#' || c == ';') {
                    out[d++] = c;
                    c = in[s++];
                }

                s--;
            }

            if (in[s] == '.') {
                while (in[s] == '.') {
                    out[d++] = in[s++];
                }
            } else  {
                break;
            }
        } while (true);

        if (d == start) {
            // Procedure name expected but found something else
            throw new SQLException(
                    Messages.get("error.parsesql.syntax", "call", String.valueOf(s)),
                    "22025");
        }

        return new String(out, start, d-start);
    }

    /**
     * Copy an embedded parameter name to the output buffer.
     *
     * @return The identifier as a <code>String</code>.
     */
    private String copyParamName() {
        int start = d;
        char c = in[s++];

        while (Character.isJavaIdentifierPart(c) || c == '@') {
           out[d++] = c;
           c = in[s++];
        }

        s--;

        return new String(out, start, d - start);
    }

    /**
     * Check that the next character is as expected.
     *
     * @param c The expected character.
     * @param copy True if found character should be copied.
     * @throws SQLException if expected characeter not found.
     */
    private void mustbe(char c, boolean copy)
        throws SQLException {
        if (in[s] != c) {
            throw new SQLException(
                    Messages.get("error.parsesql.mustbe", String.valueOf(s), String.valueOf(c)),
                    "22019");
        }

        if (copy) {
            out[d++] = in[s++];
        } else {
            s++;
        }
    }

    /**
     * Skip embedded white space.
     */
    private void skipWhiteSpace() {
        while (Character.isWhitespace(in[s])) {
            s++;
        }
    }

   /**
    * Skip single-line comments.
    */
    private void skipSingleComments() {
        while (s < len && in[s] != '\n' && in[s] != '\r') {
            // comments should be passed on to the server
            out[d++] = in[s++];
        }
    }

    /**
     * Skip multi-line comments
     */
    private void skipMultiComments() {
        int block = 0;

        do {
            if (s < len-1) {
                if (in[s] == '/' && in[s+1] == '*') {
                    block++;
                } else if (in[s] == '*' && in[s + 1] == '/') {
                    block--;
                }

                // comments should be passed on to the server
                out[d++] = in[s++];
            }
        } while (block > 0);
    }

    /**
     * Process the JDBC {call procedure [(&#63;,&#63;,&#63;)]} type escape.
     *
     * @throws SQLException
     */
    private void callEscape() throws SQLException {
        // Insert EXECUTE into SQL so that proc can be called as normal SQL
        copyLiteral("EXECUTE ");
        keyWord = "execute";
        // Process procedure name
        procName = copyProcName();
        skipWhiteSpace();

        if (in[s] == '(') { // Optional ( )
            s++;
            terminator = ')';
            skipWhiteSpace();
        } else {
            terminator = '}';
        }

        out[d++] = ' ';

        // Process any parameters
        while (in[s] != terminator) {
            String name = null;

            if (in[s] == '@') {
                // Named parameter
                name = copyParamName();
                skipWhiteSpace();
                mustbe('=', true);
                skipWhiteSpace();

                if (in[s] == '?') {
                    copyParam(name, d);
                } else {
                    // Named param has literal value can't call as RPC
                    procName = "";
                }
            } else if (in[s] == '?') {
                copyParam(name, d);
            } else {
                // Literal parameter can't call as RPC
                procName = "";
            }

            // Now find terminator or comma
            while (in[s] != terminator && in[s] != ',') {
                if (in[s] == '{') {
                    escape();
                } else if (in[s] == '\'' || in[s] == '[' || in[s] == '"') {
                    copyString();
                } else {
                    out[d++] = in[s++];
                }
            }

            if (in[s] == ',') {
                out[d++] = in[s++];
            }

            skipWhiteSpace();
        }

        if (terminator == ')') {
            s++; // Elide
        }

        terminator = '}';
        skipWhiteSpace();
    }

    /**
     * Utility routine to validate date and time escapes.
     *
     * @param mask The validation mask
     * @return True if the escape was valid and processed OK.
     */
    private boolean getDateTimeField(byte[] mask) {
        skipWhiteSpace();
        if (in[s] == '?') {
            // Allow {ts ?} type construct
            copyParam(null, d);
            skipWhiteSpace();
            return in[s] == terminator;
        }
        out[d++] = '\'';
        terminator = (in[s] == '\'' || in[s] == '"')? in[s++]: '}';
        skipWhiteSpace();
        int ptr = 0;

        while (ptr < mask.length) {
            char c = in[s++];
            if (c == ' ' && out[d-1] == ' ') {
                continue; // Eliminate multiple spaces
            }

            if (mask[ptr] == '#') {
                if (!Character.isDigit(c)) {
                    return false;
                }
            } else if (mask[ptr] != c) {
                return false;
            }

            if (c != '-') {
                out[d++] = c;
            }

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
                    } else {
                        s++;
                    }
                }
            } else {
                out[d++] = '.';
            }

            for (; digits < 3; digits++) {
                out[d++] = '0';
            }
        }

        skipWhiteSpace();

        if (in[s] != terminator) {
            return false;
        }

        if (terminator != '}') {
            s++; // Skip terminator
        }

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
     *
     * @throws SQLException
     */
    private void timeEscape() throws SQLException {

        if (!getDateTimeField(timeMask)) {
            throw new SQLException(
                    Messages.get("error.parsesql.syntax", "time", String.valueOf(s)),
                    "22019");
        }
    }

    /** Syntax mask for date escape. */
    private static final byte[] dateMask = {
        '#','#','#','#','-','#','#','-','#','#'
    };

    /**
     * Process the JDBC escape {d 'CCCC-MM-DD'}.
     *
     * @throws SQLException
     */
    private void dateEscape() throws SQLException {
        if (!getDateTimeField(dateMask)) {
            throw new SQLException(
                    Messages.get("error.parsesql.syntax", "date", String.valueOf(s)),
                    "22019");
        }
    }

    /** Syntax mask for timestamp escape. */
    static final byte[] timestampMask = {
        '#','#','#','#','-','#','#','-','#','#',' ',
        '#','#',':','#','#',':','#','#'
    };

    /**
     * Process the JDBC escape {ts 'CCCC-MM-DD HH:MM:SS[.NNN]'}.
     *
     * @throws SQLException
     */
    private void timestampEscape() throws SQLException {
        if (!getDateTimeField(timestampMask)) {
            throw new SQLException(
                    Messages.get("error.parsesql.syntax", "timestamp", String.valueOf(s)),
                    "22019");
        }
    }

    /**
     * Process the JDBC escape {oj left outer join etc}.
     *
     * @throws SQLException
     */
    private void outerJoinEscape()
        throws SQLException {
        while (in[s] != '}') {
            final char c = in[s];

            switch (c) {
                case '\'':
                case '"':
                case '[':
                    copyString();
                    break;
                case '{':
                    // Nested escape!
                    escape();
                    break;
                case '?':
                    copyParam(null, d);
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
    /** Map of jdbc to sql server function names. */
    private static HashMap msFnMap = new HashMap();
    /** Map of jdbc to server data types for convert */
    private static HashMap cvMap = new HashMap();

    static {
        // Microsoft only functions
        msFnMap.put("length", "len($)");
        msFnMap.put("truncate", "round($, 1)");
        // Common functions
        fnMap.put("user",     "user_name($)");
        fnMap.put("database", "db_name($)");
        fnMap.put("ifnull",   "isnull($)");
        fnMap.put("now",      "getdate($)");
        fnMap.put("atan2",    "atn2($)");
        fnMap.put("mod",      "($)");
        fnMap.put("length",   "char_length($)");
        fnMap.put("locate",   "charindex($)");
        fnMap.put("repeat",   "replicate($)");
        fnMap.put("insert",   "stuff($)");
        fnMap.put("lcase",    "lower($)");
        fnMap.put("ucase",    "upper($)");
        fnMap.put("concat",   "($)");
        fnMap.put("curdate",  "convert(datetime, convert(varchar, getdate(), 112))");
        fnMap.put("curtime",  "convert(datetime, convert(varchar, getdate(), 108))");
        fnMap.put("dayname",  "datename(weekday,$)");
        fnMap.put("dayofmonth",  "datepart(day,$)");
        fnMap.put("dayofweek",  "datepart(weekday,$)");
        fnMap.put("dayofyear",  "datepart(dayofyear,$)");
        fnMap.put("hour",       "datepart(hour,$)");
        fnMap.put("minute",       "datepart(minute,$)");
        fnMap.put("second",       "datepart(second,$)");
        fnMap.put("year",       "datepart(year,$)");
        fnMap.put("quarter",       "datepart(quarter,$)");
        fnMap.put("month",       "datepart(month,$)");
        fnMap.put("week",       "datepart(week,$)");
        fnMap.put("monthname",   "datename(month,$)");
        fnMap.put("timestampadd",   "dateadd($)");
        fnMap.put("timestampdiff",   "datediff($)");
        // convert jdbc to sql types
        cvMap.put("binary", "varbinary");
        cvMap.put("char", "varchar");
        cvMap.put("date", "datetime");
        cvMap.put("double", "float");
        cvMap.put("longvarbinary", "image");
        cvMap.put("longvarchar", "text");
        cvMap.put("time", "datetime");
        cvMap.put("timestamp", "timestamp");
    }

    /**
     * Process the JDBC escape {fn function()}.
     *
     * @throws SQLException
     */
    private void functionEscape() throws SQLException {
        char tc = terminator;
        skipWhiteSpace();
        StringBuffer nameBuf = new StringBuffer();
        //
        // Capture name
        //
        while (Character.isLetterOrDigit(in[s])) {
            nameBuf.append(in[s++]);
        }

        String name = nameBuf.toString().toLowerCase();
        //
        // Now collect arguments
        //
        skipWhiteSpace();
        mustbe('(', false);
        int parenCnt = 1;
        int argStart = d;
        int arg2Start = 0;
        terminator = ')';
        while (in[s] != ')' || parenCnt > 1) {
            final char c = in[s];

            switch (c) {
                case '\'':
                case '"':
                case '[':
                    copyString();
                    break;
                case '{':
                    // Process nested escapes!
                    escape();
                    break;
                case ',':
                    if (arg2Start == 0) {
                        arg2Start = d - argStart;
                    }
                    if (name.equals("concat")) {
                        out[d++] = '+'; s++;
                    } else
                    if (name.equals("mod")) {
                        out[d++] = '%'; s++;
                    } else {
                        out[d++] = c; s++;
                    }
                    break;
                case '(':
                    parenCnt++;
                    out[d++] = c; s++;
                    break;
                case ')':
                    parenCnt--;
                    out[d++] = c; s++;
                    break;
                default:
                    out[d++] = c; s++;
                    break;
            }
        }

        String args = String.valueOf(out, argStart, d-argStart).trim();

        d = argStart;
        mustbe(')', false);
        terminator = tc;
        skipWhiteSpace();

        //
        // Process convert scalar function.
        // Arguments need to be reversed and the data type
        // argument converted to an SQL server type
        //
        if (name.equals("convert") && arg2Start < args.length() - 1) {
            String arg2 = args.substring(arg2Start + 1).trim().toLowerCase();
            String dataType = (String) cvMap.get(arg2);

            if (dataType == null) {
                // Will get server error if invalid type passed
                dataType = arg2;
            }

            copyLiteral("convert(");
            copyLiteral(dataType);
            out[d++] = ',';
            copyLiteral(args.substring(0, arg2Start));
            out[d++] = ')';

            return;
        }

        //
        // See if function mapped
        //
        String fn;
        if (connection.getServerType() == Driver.SQLSERVER) {
            fn = (String)msFnMap.get(name);
            if (fn == null) {
                fn = (String)fnMap.get(name);
            }
        } else {
            fn = (String)fnMap.get(name);
        }
        if (fn == null) {
            // Not mapped so assume simple case
            copyLiteral(name);
            out[d++] = '(';
            copyLiteral(args);
            out[d++] = ')';
            return;
        }
        //
        // Process timestamp interval constants
        //
        if (args.length() > 8 && args.substring(0,8).equalsIgnoreCase("sql_tsi_")) {
            args = args.substring(8);
            if (args.length() > 11 && args.substring(0, 11).equalsIgnoreCase("frac_second")) {
                args = "millisecond" + args.substring(11);
            }
        }
        //
        // Substitute mapped function name and arguments
        //
        for (int i = 0; i < fn.length(); i++) {
            char c = fn.charAt(i);
            if (c == '$') {
                // Substitute arguments
                copyLiteral(args);
            } else {
                out[d++] = c;
            }
        }
    }

    /**
     * Process the JDBC escape {escape 'X'}.
     *
     * @throws SQLException
     */
    private void likeEscape() throws SQLException {
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
     *
     * @throws SQLException
     */
    private void escape() throws SQLException {
        char tc = terminator;
        terminator = '}';
        StringBuffer escBuf = new StringBuffer();
        s++;
        skipWhiteSpace();

        if (in[s] == '?') {
            copyParam("@return_status", -1);
            skipWhiteSpace();
            mustbe('=', false);
            skipWhiteSpace();

            while (Character.isLetter(in[s])) {
                escBuf.append(in[s++]);
            }

            skipWhiteSpace();
            String esc = escBuf.toString();

            if (esc.equalsIgnoreCase("call")) {
                callEscape();
            } else {
                throw new SQLException(Messages.get("error.parsesql.syntax",
                                                          "call",
                                                          String.valueOf(s)),
                                       "22019");
            }
        } else {
            while (Character.isLetter(in[s])) {
                escBuf.append(in[s++]);
            }

            skipWhiteSpace();
            String esc = escBuf.toString();

            if (esc.equalsIgnoreCase("call")) {
                callEscape();
            } else if (esc.equalsIgnoreCase("t")) {
                timeEscape();
            } else if (esc.equalsIgnoreCase("d")) {
                dateEscape();
            } else if (esc.equalsIgnoreCase("ts")) {
                timestampEscape();
            } else if (esc.equalsIgnoreCase("oj")) {
                outerJoinEscape();
            } else if (esc.equalsIgnoreCase("fn")) {
                functionEscape();
            } else if (esc.equalsIgnoreCase("escape")) {
                likeEscape();
            } else {
                throw new SQLException(
                        Messages.get("error.parsesql.badesc", esc, String.valueOf(s)),
                        "22019");
            }
        }

        mustbe('}', false);
        terminator = tc;
    }

    /**
     * Extract the first table name following the keyword FROM.
     *
     * @return the table name as a <code>String</code>
     */
    private String getTableName()
    {
        // FIXME If the resulting table name contains a '(' (i.e. is a function) "rollback" and return null
        StringBuffer name = new StringBuffer(128);
        char c = (s < len)? in[s]: ' ';
        while (s < len && Character.isWhitespace(c)) {
            out[d++] = c; s++; c = (s < len)? in[s]: ' ';
        }
        if (c == '{') {
            // Start of {oj ... } we can assume that there is
            // more than one table in select and therefore
            // it would not be updateable.
            return "";
        }
        //
        // Skip any leading comments before fist table name
        //
        while (c == '/' || c == '-') {
            if ( c == '/') {
                if (s+1 < len && in[s+1] == '*') {
                    skipMultiComments();
                    s++;
                } else {
                    break;
                }
            } else {
                if (s+1 < len && in[s+1] == '-') {
                    skipSingleComments();
                } else {
                    break;
                }
            }
            c = (s < len)? in[s]: ' ';
            while (s < len && Character.isWhitespace(c)) {
                out[d++] = c; s++; c = (s < len)? in[s]: ' ';
            }
        }
        if (c == '{') {
            // See comment above
            return "";
        }
        //
        // Now process table name
        //
        while (s < len) {
            if (c == '[' || c == '"') {
                int start = d;
                copyString();
                name.append(String.valueOf(out, start, d-start));
                c = (s < len)? in[s]: ' ';
            } else {
                int start = d;
                while (s < len && !Character.isWhitespace(c) && c != '.' && c != ',') {
                    out[d++] = c; s++;
                    c = (s < len)? in[s]: ' ';
                }
                name.append(String.valueOf(out, start, d-start));
            }
            while (s < len && Character.isWhitespace(c)) {
                out[d++] = c; s++;
                c = (s < len)? in[s]: ' ';
            }
            if (c != '.') {
                break;
            }
            name.append(c);
            out[d++] = c; s++;
            c = (s < len)? in[s]: ' ';
            while (s < len && Character.isWhitespace(c)) {
                out[d++] = c; s++;
                c = (s < len)? in[s]: ' ';
            }
        }
        return name.toString();
    }

    /**
     * Parse the SQL statement processing JDBC escapes and parameter markers.
     *
     * @param extractTable true to return the first table name in the FROM clause of a select
     * @return The processed SQL statement, any procedure name, the first
     * SQL keyword and (optionally) the first table name as elements 0 1, 2 and 3 of the
     * returned <code>String[]</code>.
     * @throws SQLException
     */
    String[] parse(boolean extractTable) throws SQLException {
        boolean isSelect = false;
        try {
            //
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
                    case '/':
                        if (s+1 < len && in[s+1] == '*') {
                            skipMultiComments();
                        } else {
                            out[d++] = c; s++;
                        }
                        break;
                    case '-':
                        if (s+1 < len && in[s+1] == '-') {
                            skipSingleComments();
                        } else {
                            out[d++] = c; s++;
                        }
                        break;
                    default:
                        if (Character.isLetter(c)) {
                            if (keyWord == null) {
                                keyWord = copyKeyWord();
                                if (keyWord.equals("select")) {
                                    isSelect = true;
                                }
                                break;
                            }
                            if (extractTable && isSelect) {
                                String sqlWord = copyKeyWord();
                                if (sqlWord.equals("from")) {
                                    // Ensure only first 'from' is processed
                                    extractTable = false;
                                    tableName = getTableName();
                                }
                                break;
                            }
                        }

                        out[d++] = c; s++;
                        break;
                }
            }
            //
            // Impose a reasonable maximum limit on the number of parameters
            // unless the connection is sending statements unprepared (i.e. by
            // building a plain query) and this is not a procedure call.
            //
            if (params.size() > 255
                    && connection.getPrepareSql() != TdsCore.UNPREPARED
                    && procName != null) {
                int limit = 255; // SQL 6.5 and Sybase < 12.50
                if (connection.getServerType() == Driver.SYBASE) {
                    if (connection.getDatabaseMajorVersion() > 12 ||
                            connection.getDatabaseMajorVersion() == 12 &&
                            connection.getDatabaseMinorVersion() >= 50) {
                        limit = 2000; // Actually 2048 but allow some head room
                    }
                } else {
                    if (connection.getDatabaseMajorVersion() == 7) {
                        limit = 1000; // Actually 1024
                    } else
                    if (connection.getDatabaseMajorVersion() > 7) {
                        limit = 2000; // Actually 2100
                    }

                }
                if (params.size() > limit) {
                    throw new SQLException(
                        Messages.get("error.parsesql.toomanyparams",
                                Integer.toString(limit)), "22025");
                }
            }
            String result[] = new String[4];

            // return sql and procname
            result[0] = new String(out, 0, d);
            result[1] = procName;
            result[2] = (keyWord == null)? "": keyWord;
            result[3] = tableName;
            return result;
        } catch (IndexOutOfBoundsException e) {
            // Should only come here if string is invalid in some way.
            throw new SQLException(
                    Messages.get("error.parsesql.missing", String.valueOf(terminator)),
                    "22025");
        }
    }
}
