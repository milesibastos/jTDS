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

import java.io.*;
import java.sql.*;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;

import net.sourceforge.jtds.util.*;

/**
 * This class implements the Sybase / Microsoft TDS protocol.
 * <p>
 * Implementation notes:
 * <ol>
 * <li>This class, together with TdsData, encapsulates all of the TDS specific logic
 *     required by the driver.
 * <li>This is a ground up reimplementation of the TDS protocol and is rather
 *     simpler, and hopefully easier to understand, than the original.
 * <li>The layout of the various Login packets is derived from the original code
 *     and freeTds work, and incorporates changes including the ability to login as a TDS 5.0 user.
 * <li>All network I/O errors are trapped here, reported to the log (if active)
 *     and the parent Connection object is notified that the connection should be considered
 *     closed.
 * <li>Rather than having a large number of classes one for each token, useful information
 *     about the current token is gathered together in the inner TdsToken class.
 * <li>As the rest of the driver interfaces to this code via higher-level method calls there
 *     should be know need for knowledge of the TDS protocol to leak out of this class.
 *     It is for this reason that all the TDS Token constants are private.
 * </ol>
 *
 * @author Mike Hutchinson
 * @author Matt Brinkley
 * @author Alin Sinpalean
 * @author freeTDS project
 * @version $Id: TdsCore.java,v 1.12 2004-07-29 00:30:37 ddkilzer Exp $
 */
public class TdsCore {
    /**
     * Inner static class used to hold information about TDS tokens read.
     *
     * @author Mike Hutcinson.
     */
    private static class TdsToken {
        /** The current TDS token byte. */
        private byte token;
        /** The status field from a DONE packet. */
        private byte status;
        /** The operation field from a DONE packet. */
        private byte operation;
        /** The update count from a DONE packet. */
        private int updateCount;
        /** The nonce from an NTLM challenge packet. */
        private byte[] nonce;
        /** The dynamicID from the last TDS_DYNAMIC token. */
        private String dynamicId;
        /** The dynamic parameters from the last TDS_DYNAMIC token. */
        private ColInfo[] dynamParamInfo;
        /** The dynamic parameter data from the last TDS_DYNAMIC token. */
        private ColData[] dynamParamData;

        /**
         * Retrieve the update count status.
         *
         * @return <code>boolean</code> true if the update count is valid.
         */
        boolean isUpdateCount() {
            return isEndToken() && (status & DONE_ROW_COUNT) != 0;
        }

        /**
         * Retrieve the DONE token status.
         *
         * @return <code>boolean</code> true if the current token is a DONE packet.
         */
        boolean isEndToken() {
            return token == TDS_DONE_TOKEN
                   || token == TDS_DONEINPROC_TOKEN
                   || token == TDS_DONEPROC_TOKEN;
        }

        /**
         * Retrieve the NTLM challenge status.
         *
         * @return <code>boolean</code> true if the current token is an NTLM challenge.
         */
        boolean isAuthToken() {
            return token == TDS_AUTH_TOKEN;
        }

        /**
         * Retrieve the results pending status.
         *
         * @return <code>boolean</code> true if more results in input.
         */
        boolean resultsPending() {
            return !isEndToken() || ((status & DONE_MORE_RESULTS) != 0);
        }

        /**
         * Retrieve the result set status.
         *
         * @return <code>boolean</code> true if the current token is a result set.
         */
        boolean isResultSet() {
            return token == TDS_COLFMT_TOKEN
                   || token == TDS7_RESULT_TOKEN
                   || token == TDS_RESULT_TOKEN
                   || token == TDS_COLINFO_TOKEN;
        }

        /**
         * Retrieve the row data status.
         *
         * @return <code>boolean</code> true if the current token is a result row.
         */
        public boolean isRowdata() {
            return token == TDS_ROW_TOKEN;
        }

    }

    //
    // Package private constants
    //
    /** Default Microosoft port. */
    public static final int DEFAULT_SQLSERVER_PORT = 1433;
    /** Default Sybase port. */
    public static final int DEFAULT_SYBASE_PORT = 7100;
    /** Minimum network packet size. */
    public static final int MIN_PKT_SIZE = 512;
    /** Default minimum network packet size for TDS 7.0 and newer. */
    public static final int DEFAULT_MIN_PKT_SIZE_TDS70 = 4096;
    /** Maximum network packet size. */
    public static final int MAX_PKT_SIZE = 32768;
    /** The size of the packet header. */
    static final int PKT_HDR_LEN = 8;
    /** TDS 4.2 or 7.0 Query packet. */
    static final byte QUERY_PKT = 1;
    /** TDS 4.2 or 5.0 Login packet. */
    static final byte LOGIN_PKT = 2;
    /** TDS Remote Procedure Call. */
    static final byte RPC_PKT = 3;
    /** TDS Reply packet. */
    static final byte REPLY_PKT = 4;
    /** TDS Cancel packet. */
    static final byte CANCEL_PKT = 6;
    /** TDS 5.0 Query packet. */
    static final byte SYBQUERY_PKT = 15;
    /** TDS 7.0 Login packet. */
    static final byte MSLOGIN_PKT = 16;
    /** TDS 7.0 NTLM Authentication packet. */
    static final byte NTLMAUTH_PKT = 17;

    //
    // Sub packet types
    //
    /** TDS 5.0 Parameter format token. */
    private static final byte TDS5_PARAMFMT2_TOKEN  = (byte) 32;   // 0x20
    /** TDS 5.0 Language token. */
    private static final byte TDS_LANG_TOKEN        = (byte) 33;   // 0x21
    /** TDS 5.0 Close token. */
    private static final byte TDS_CLOSE_TOKEN       = (byte) 113;  // 0x71
    /** TDS Procedure call return status token. */
    private static final byte TDS_RETURNSTATUS_TOKEN= (byte) 121;  // 0x79
    /** TDS Procedure ID token. */
    private static final byte TDS_PROCID            = (byte) 124;  // 0x7C
    /** TDS 7.0 Result set column meta data token. */
    private static final byte TDS7_RESULT_TOKEN     = (byte) 129;  // 0x81
    /** TDS 7.0 Computed Result set column meta data token. */
    private static final byte TDS7_COMP_RESULT_TOKEN= (byte) 136;  // 0x88
    /** TDS 4.2 Column names token. */
    private static final byte TDS_COLNAME_TOKEN     = (byte) 160;  // 0xA0
    /** TDS 4.2 Column meta data token. */
    private static final byte TDS_COLFMT_TOKEN      = (byte) 161;  // 0xA1
    /** TDS Table name token. */
    private static final byte TDS_TABNAME_TOKEN     = (byte) 164;  // 0xA4
    /** TDS Cursor results column infomation token. */
    private static final byte TDS_COLINFO_TOKEN     = (byte) 165;  // 0xA5
    /** TDS Optional command token. */
    private static final byte TDS_OPTIONCMD_TOKEN   = (byte) 166;  // 0xA6
    /** TDS Computed result set names token. */
    private static final byte TDS_COMP_NAMES_TOKEN  = (byte) 167;  // 0xA7
    /** TDS Computed result set token. */
    private static final byte TDS_COMP_RESULT_TOKEN = (byte) 168;  // 0xA8
    /** TDS Order by columns token. */
    private static final byte TDS_ORDER_TOKEN       = (byte) 169;  // 0xA9
    /** TDS error result token. */
    private static final byte TDS_ERROR_TOKEN       = (byte) 170;  // 0xAA
    /** TDS Information message token. */
    private static final byte TDS_INFO_TOKEN        = (byte) 171;  // 0xAB
    /** TDS Output parameter value token. */
    private static final byte TDS_PARAM_TOKEN       = (byte) 172;  // 0xAC
    /** TDS Login acknowledgement token. */
    private static final byte TDS_LOGINACK_TOKEN    = (byte) 173;  // 0xAD
    /** TDS control token. */
    private static final byte TDS_CONTROL_TOKEN     = (byte) 174;  // 0xAE
    /** TDS Result set data row token. */
    private static final byte TDS_ROW_TOKEN         = (byte) 209;  // 0xD1
    /** TDS Computed result set data row token. */
    private static final byte TDS_ALTROW            = (byte) 211;  // 0xD3
    /** TDS 5.0 parameter value token. */
    private static final byte TDS5_PARAMS_TOKEN     = (byte) 215;  // 0xD7
    /** TDS 5.0 capabilities token. */
    private static final byte TDS_CAP_TOKEN         = (byte) 226;  // 0xE2
    /** TDS environment change token. */
    private static final byte TDS_ENVCHANGE_TOKEN   = (byte) 227;  // 0xE3
    /** TDS 5.0 message token. */
    private static final byte TDS_MSG50_TOKEN       = (byte) 229;  // 0xE5
    /** TDS 5.0 RPC token. */
    private static final byte TDS_DBRPC_TOKEN       = (byte) 230;  // 0xE6
    /** TDS 5.0 Dynamic SQL token. */
    private static final byte TDS5_DYNAMIC_TOKEN    = (byte) 231;  // 0xE7
    /** TDS 5.0 parameter descriptor token. */
    private static final byte TDS5_PARAMFMT_TOKEN   = (byte) 236;  // 0xEC
    /** TDS 7.0 NTLM authentication challenge token. */
    private static final byte TDS_AUTH_TOKEN        = (byte) 237;  // 0xED
    /** TDS 5.0 Result set column meta data token. */
    private static final byte TDS_RESULT_TOKEN      = (byte) 238;  // 0xEE
    /** TDS done token. */
    private static final byte TDS_DONE_TOKEN        = (byte) 253;  // 0xFD DONE
    /** TDS done procedure token. */
    private static final byte TDS_DONEPROC_TOKEN    = (byte) 254;  // 0xFE DONEPROC
    /** TDS done in procedure token. */
    private static final byte TDS_DONEINPROC_TOKEN  = (byte) 255;  // 0xFF DONEINPROC

    //
    // Environment change payload codes
    //
    /** Environment change: database changed. */
    private static final byte TDS_ENV_DATABASE      = (byte) 1;
    /** Environment change: language changed. */
    private static final byte TDS_ENV_LANG          = (byte) 2;
    /** Environment change: charset changed. */
    private static final byte TDS_ENV_CHARSET       = (byte) 3;
    /** Environment change: network packet size changed. */
    private static final byte TDS_ENV_PACKSIZE      = (byte) 4;
    /** Environment change: locale changed. */
    private static final byte TDS_ENV_LCID          = (byte) 5;
    /** Environment change: TDS 8 collation changed. */
    private static final byte TDS_ENV_SQLCOLLATION  = (byte) 7; // TDS8 Collation

    //
    // Static variables used only for performance
    //
    /** Used to optimize the {@link #getParameters()} call */
    private static final ParamInfo[] EMPTY_PARAMETER_INFO = new ParamInfo[0];

    //
    // Error status bytes
    //
    /** Done: more results are expected. */
    private static final byte DONE_MORE_RESULTS     = (byte) 0x01;
    /** Done: command caused an error. */
    private static final byte DONE_ERROR            = (byte) 0x02;
    /** Done: There is a valid row count. */
    private static final byte DONE_ROW_COUNT        = (byte) 0x10;
    /** Done: Cancel acknowledgement. */
    private static final byte DONE_CANCEL           = (byte) 0x20;

    //
    // Instance variables
    //
    /** The Connection object that created this object. */
    private ConnectionJDBC2 connection;
    /** The TDS version being supported by this connection. */
    private int tdsVersion;
    /** The make of SQL Server (Sybase/Microsoft). */
    private int serverType;
    /** The Shared network socket object. */
    private SharedSocket socket;
    /** The output server request stream. */
    private RequestStream out;
    /** The input server response stream. */
    private ResponseStream in;
    /** True if the server response is fully read. */
    private boolean endOfResponse = true;
    /** True if the current result set is at end of file. */
    private boolean endOfResults  = true;
    /** The array of column meta data objects for this result set. */
    private ColInfo[] columns;
    /** The array of column data objects in the current row. */
    private ColData[] rowData;
    /** The array of table names associated with this result. */
    private String[] tables;
    /** The descriptor object for the current TDS token. */
    private TdsToken currentToken = new TdsToken();
    /** The stored procedure return status. */
    private Integer returnStatus;
    /** The return parameter meta data object for the current procedure call. */
    private ParamInfo returnParam;
    /** The array of parameter meta data objects for the current procedure call. */
    private ParamInfo[] parameters;
    /** The index of the next output parameter to populate. */
    private int nextParam = -1;
    /** The head of the diagnostic messages chain. */
    private SQLDiagnostic messages;
    /** Indicates that this object is closed. */
    private boolean isClosed = false;
    /** Indicates reading results from READTEXT command. */
    private boolean readTextMode = false;

    /**
     * Construct a TdsCore object.
     *
     * @param connection The connection which owns this object.
     * @param messages The SQLDiagnostic messages chain.
     */
    TdsCore(ConnectionJDBC2 connection, SQLDiagnostic messages) {
        this.connection = connection;
        this.socket = connection.getSocket();
        this.messages = messages;
        tdsVersion = connection.getTdsVersion();
        serverType = connection.getServerType();
        tdsVersion = socket.getTdsVersion();
        out = socket.getRequestStream();
        in = socket.getResponseStream(out);
        out.setBufferSize(connection.getNetPacketSize());
        out.setMaxPrecision(connection.getMaxPrecision());
    }

    /**
     * Check that the connection is still open.
     *
     * @throws SQLException
     */
    private void checkOpen() throws SQLException {
        if (isClosed) {
            throw new SQLException(
                Support.getMessage("error.generic.closed", "Connection"),
                    "HY010");
        }
    }

    /**
     * Retrieve the TDS protocol version.
     *
     * @return The protocol version as an <code>int</code>.
     */
    int getTdsVersion() {
       return tdsVersion;
    }

    /**
     * Retrieve the current result set column descriptors.
     *
     * @return The column descriptors as a <code>ColInfo[]</code>.
     */
    ColInfo[] getColumns() {
        return columns;
    }

    /**
     * Retrieve the parameter meta data from a Sybase prepare.
     *
     * @return The parameter descriptors as a <code>ParamInfo[]</code>.
     */
    ParamInfo[] getParameters() {
        if (currentToken.dynamParamInfo != null) {
            ParamInfo[] params = new ParamInfo[currentToken.dynamParamInfo.length];

            for (int i = 0; i < params.length; i++) {
                ColInfo ci = currentToken.dynamParamInfo[i];
                ParamInfo pi = new ParamInfo();

                pi.tdsType = ci.tdsType;
                pi.scale = ci.scale;
                pi.precision = ci.precision;
                pi.name = ci.name;
                pi.isOutput = false;
                pi.jdbcType = ci.jdbcType;
                pi.sqlType = ci.sqlType;
                params[i] = pi;
            }

            return params;
        }

        return EMPTY_PARAMETER_INFO;
    }

    /**
     * Retrieve the current result set data items.
     *
     * @return The row data as a <code>ColData[]</code>.
     */
    ColData[] getRowData() {
        return rowData;
    }

    /**
     * Login to the SQL Server.
     *
     * @param serverName The server host name.
     * @param database The required database.
     * @param user The user name.
     * @param password The user password.
     * @param domain The Windows NT domain (or null).
     * @param charset The required server character set.
     * @param appName The application name.
     * @param libName The library name.
     * @param language The language to use for server messages.
     * @param macAddress The client network MAC address.
     * @param packetSize The required network packet size.
     * @throws SQLException
     */
    void login(final String serverName,
               final String database,
               final String user,
               final String password,
               final String domain,
               final String charset,
               final String appName,
               final String libName,
               final String language,
               final String macAddress,
               final int packetSize)
        throws SQLException {
        try {
            if (tdsVersion >= Driver.TDS70) {
                sendMSLoginPkt(serverName, database, user, password,
                                domain, appName, libName, language,
                                macAddress, packetSize);
            } else if (tdsVersion == Driver.TDS50) {
                send50LoginPkt(serverName, user, password,
                                charset, appName, libName,
                                language, packetSize);
            } else {
                send42LoginPkt(serverName, user, password,
                                charset, appName, libName,
                                language, packetSize);
            }

            nextToken();

            while (!endOfResponse) {
                if (currentToken.isAuthToken()) {
                    sendNtlmChallengeResponse(currentToken.nonce, user, password, domain);
                }

                nextToken();
            }

            messages.checkErrors();
        } catch (IOException ioe) {
            throw Support.linkException(
                new SQLException(
                       Support.getMessage(
                                "error.generic.ioerror", ioe.getMessage()),
                                    "08S01"), ioe);
        }
    }

    /**
     * Get the next result set or update countf rom the TDS stream.
     *
     * @return <code>boolean</code> if the next item is a result set.
     * @throws SQLException
     */
    boolean getMoreResults() throws SQLException {
        nextToken();
        messages.checkErrors();

        while (!endOfResponse
               && !currentToken.isUpdateCount()
               && !currentToken.isResultSet()) {
            nextToken();
            messages.checkErrors();
        }

        //
        // Cursor opens are followed by TDS_TAB_INFO and TDS_COL_INFO
        // Process these now so that the column descriptors are updated.
        //
        if (currentToken.isResultSet()) {
            try {
                byte x = (byte) in.peek();

                while (x == TDS_TABNAME_TOKEN || x == TDS_COLINFO_TOKEN) {
                    nextToken();
                    x = (byte)in.peek();
                }
            } catch (IOException e) {
                isClosed = true;
                connection.setClosed();

                throw Support.linkException(
                    new SQLException(
                           Support.getMessage(
                                "error.generic.ioerror", e.getMessage()),
                                    "08S01"), e);
            }
        }

        messages.checkErrors();

        return currentToken.isResultSet();
    }

    /**
     * Retrieve the status of the next result item.
     *
     * @return <code>boolean</code> true if the next item is a result set.
     */
    boolean isResultSet() {
        return currentToken.isResultSet();
    }

    /**
     * Retrieve the status of the next result item.
     *
     * @return <code>boolean</code> true if the next item is an update count.
     */
    boolean isUpdateCount() {
        return currentToken.isUpdateCount();
    }

    /**
     * Retrieve the update count from the current TDS token.
     *
     * @return The update count as an <code>int</code>.
     */
    int getUpdateCount() {
        if (currentToken.isEndToken()) {
            return currentToken.updateCount;
        }

        return -1;
    }

    /**
     * Retrieve the status of the response stream.
     *
     * @return <code>boolean</code> true if the response has been entirely consumed.
     */
    boolean isEndOfResponse() {
        return endOfResponse;
    }

    /**
     * Empty the server response queue.
     *
     * @throws SQLException
     */
    void clearResponseQueue() throws SQLException {
        while (!endOfResponse) {
            nextToken();
        }
    }

    /**
     * Retrieve the next data row from the result set.
     *
     * @param readAhead <code>true</code> to force driver to skip to end of
     *        response when the last row has been read. This ensures all SP
     *        output parameters are processed. Only usable in executeQuery().
     * @return <code>boolean</code> - <code>false</code> if at end of results.
     */
    boolean getNextRow(boolean readAhead) throws SQLException {
        if (endOfResponse || endOfResults) {
            return false;
        }

        nextToken();

        // Will either be first or next data row or end.
        while (!currentToken.isRowdata() && !currentToken.isEndToken()) {
            nextToken(); // Could be messages
        }

        boolean isResultSet = currentToken.isRowdata();

        if (readAhead && !endOfResponse) {
            // This will ensure that called procedure return parameters
            // and status are read in executeQuery()
            byte x;

            try {
                x = (byte) in.peek();

                while (x != TDS_ROW_TOKEN) {
                    nextToken();

                    if (endOfResponse) {
                        break;
                    }

                    x = (byte) in.peek();
                }
            } catch (IOException e) {
                isClosed = true;
                connection.setClosed();

                throw Support.linkException(
                    new SQLException(
                           Support.getMessage(
                                    "error.generic.ioerror", e.getMessage()),
                                        "08S01"), e);
            }
        }

        messages.checkErrors();

        return isResultSet;
    }

    /**
     * Retrieve the status of result set.
     * <p>
     * This does a quick read ahead and is needed to support the isLast()
     * method in the ResultSet.
     *
     * @return <code>boolean</code> - <code>true</code> if there is more data
     *          in the result set.
     */
    boolean isDataInResultSet() throws SQLException {
        byte x;

        checkOpen();

        try {
            x = (endOfResponse) ? TDS_DONE_TOKEN : (byte) in.peek();

            while (x != TDS_ROW_TOKEN
                   && x != TDS_DONE_TOKEN
                   && x != TDS_DONEINPROC_TOKEN
                   && x != TDS_DONEPROC_TOKEN) {
                nextToken();
                x = (byte) in.peek();
            }

            messages.checkErrors();
        } catch (IOException e) {
            isClosed = true;
            connection.setClosed();
            throw Support.linkException(
                new SQLException(
                       Support.getMessage(
                                "error.generic.ioerror", e.getMessage()),
                                    "08S01"), e);
        }

        return x == TDS_ROW_TOKEN;
    }

    /**
     * Retrieve the return status for the current stored procedure.
     *
     * @return The return status as an <code>Integer</code>.
     */
    Integer getReturnStatus() {
        return this.returnStatus;
    }

    /**
     * Inform the server that this connection is closing.
     * <p>
     * Used by Sybase a no op for Microsoft.
     *
     * @throws SQLException
     */
    void closeConnection() {
        try {
            if (tdsVersion == Driver.TDS50) {
                out.setPacketType(SYBQUERY_PKT);
                out.write((byte)TDS_CLOSE_TOKEN);
                out.write((byte)0);
                out.flush();
                endOfResponse = false;
                clearResponseQueue();
            }
        } catch (Exception e) {
            // Ignore any exceptions as this connection
            // is closing anyway.
            isClosed = true;
        }
    }

    /**
     * Close the TDSCore connection object and associated streams.
     *
     * @throws IOException
     * @throws SQLException
     */
    void close() throws SQLException {
       if (!isClosed) {
           try {
               clearResponseQueue();
               out.close();
               in.close();
           } catch (IOException ioe) {
               isClosed = true;
               connection.setClosed();
               throw Support.linkException(
                   new SQLException(
                          Support.getMessage(
                                   "error.generic.ioerror", ioe.getMessage()),
                                       "08S01"), ioe);
           } finally {
               isClosed = true;
           }
        }
    }

    /**
     * Send a cancel packet to the server.
     */
    void cancel() {
        socket.cancel(out.getStreamId());
    }

    /**
     * Submit a simple SQL statement to the server and process all output.
     *
     * @param sql The statement to execute.
     * @throws SQLException
     */
    void submitSQL(String sql) throws SQLException {
        checkOpen();

        if (sql.length() == 0) {
            throw new IllegalArgumentException("submitSQL() called with empty SQL String");
        }

        executeSQL(sql, null, null, false, 0, 0);
        clearResponseQueue();
        messages.checkErrors();
    }

    /**
     * Send an SQL statement with optional parameters to the server.
     *
     * @param sql The SQL statement to execute.
     * @param procName Stored procedure to execute or null.
     * @param parameters Parameters for call or null.
     * @param noMetaData Suppress meta data for cursor calls.
     * @param timeOut Optional query timeout or 0.
     * @param maxRows The maximum number of data rows to return.
     * @throws SQLException
     */
    synchronized void executeSQL(String sql,
                                 String procName,
                                 ParamInfo[] parameters,
                                 boolean noMetaData,
                                 int timeOut,
                                 int maxRows)
        throws SQLException {
        checkOpen();
        clearResponseQueue();
        messages.exceptions = null;
        setRowCount(maxRows);
        messages.clearWarnings();
        this.returnStatus = null;
        this.parameters = parameters;

        if (parameters != null && parameters.length > 0 && parameters[0].isRetVal) {
            returnParam = parameters[0];
            returnParam.isSet = false;
            nextParam = 0;
        } else {
            returnParam = null;
            nextParam = -1;
        }

        if (parameters != null) {
            for (int i = 0; i < parameters.length; i++){
                if (!parameters[i].isSet && !parameters[i].isOutput){
                    throw new SQLException(Support.getMessage("error.prepare.paramnotset",
                                                              Integer.toString(i + 1)),
                                           "07000");
                }

                TdsData.getNativeType(connection, parameters[i]);
            }
        }

        try {
            switch (tdsVersion) {
                case Driver.TDS42:
                    executeSQL42(sql, procName, parameters, noMetaData);
                    break;
                case Driver.TDS50:
                    executeSQL50(sql, procName, parameters, noMetaData);
                    break;
                case Driver.TDS70:
                case Driver.TDS80:
                    executeSQL70(sql, procName, parameters, noMetaData);
                    break;
                default:
                    throw new IllegalStateException("Unknown TDS version " + tdsVersion);
            }

            endOfResponse = false;
            endOfResults  = true;
            wait(timeOut);
        } catch (IOException ioe) {
            isClosed = true;
            connection.setClosed();

            throw Support.linkException(
                new SQLException(
                       Support.getMessage(
                                "error.generic.ioerror", ioe.getMessage()),
                                    "08S01"), ioe);
        }
    }

    /**
     * Create a temporary stored procedure on a Microsoft server.
     *
     * @param sql The SQL statement to prepare.
     * @param procName The dynamic ID for the procedure.
     * @param params The actual parameter list
     * @return <code>boolean</code> true if statement sucessfully prepared.
     * @throws SQLException
     */
    boolean microsoftPrepare(String sql, String procName, ParamInfo[] params)
        throws SQLException {
        StringBuffer spSql = new StringBuffer(sql.length()+64);
        spSql.append("create proc ");
        spSql.append(procName);
        spSql.append(' ');

        for (int i = 0; i < params.length; i++) {
            spSql.append("@P");
            spSql.append(i);
            spSql.append(' ');
            spSql.append(params[i].sqlType);

            if (i + 1 < params.length) {
                spSql.append(',');
            }
        }

        // continue building proc
        spSql.append(" as ");
        spSql.append(Support.substituteParamMarkers(sql, params));

        try {
            submitSQL(spSql.toString());
        } catch (SQLException e) {
            if (e.getSQLState() != null && e.getSQLState().equals("08S01")) {
                // Serious error rethrow
                throw e;
            }

            // This exception probably caused by failure to prepare
            // Return false;
            return false;
        }

        return true;
    }

    /**
     * Create a light weight stored procedure on a Sybase server.
     *
     * @param sql The SQL statement to prepare.
     * @param procName The dynamic ID for the procedure.
     * @param params The actual parameter list
     * @return <code>boolean</code> true if statement sucessfully prepared.
     * @throws SQLException
     */
    boolean sybasePrepare(String sql, String procName, ParamInfo[] params)
        throws SQLException {
        if (sql == null || sql.length() == 0) {
            throw new IllegalArgumentException(
                    "sql parameter must be at least 1 character long.");
        }

        if (procName == null || procName.length() != 11) {
            throw new IllegalArgumentException(
                    "procName parameter must be 11 characters long.");
        }

        // Check no text/image parameters
        for (int i = 0; i < params.length; i++) {
            if (params[i].sqlType.equals("text")
                || params[i].sqlType.equals("image")) {
                return false; // Sadly no way
            }
        }

        try {
            out.setPacketType(SYBQUERY_PKT);
            out.write((byte)TDS5_DYNAMIC_TOKEN);

            byte buf[] = Support.encodeString(connection.getCharSet(), sql);

            out.write((short) (buf.length + 41));
            out.write((byte) 1);
            out.write((byte) 0);
            out.write((byte) 10);
            out.writeAscii(procName.substring(1));
            out.write((short) (buf.length + 26));
            out.writeAscii("create proc ");
            out.writeAscii(procName.substring(1));
            out.writeAscii(" as ");
            out.write(buf);
            out.flush();
            endOfResponse = false;
            clearResponseQueue();
            messages.checkErrors();
        } catch (IOException ioe) {
            isClosed = true;
            connection.setClosed();
            throw Support.linkException(
                new SQLException(
                       Support.getMessage(
                                "error.generic.ioerror", ioe.getMessage()),
                                    "08S01"), ioe);
        } catch (SQLException e) {
            if (e.getSQLState() != null && e.getSQLState().equals("08S01")) {
                // Serious error rethrow
                throw e;
            }

            // This exception probably caused by failure to prepare
            // Return false;
            return false;
        }

        return true;
    }

    /**
     * Read text or image data from the server using readtext.
     *
     * @param tabName The parent table for this column.
     * @param colName The name of the text or image column.
     * @param textPtr The text pointer structure.
     * @param offset The starting offset in the text object.
     * @param length The number of bytes or characters to read.
     * @return A returned <code>String</code> or <code>byte[]</code>.
     * @throws SQLException
     */
    Object readText(String tabName, String colName, TextPtr textPtr, int offset, int length)
        throws SQLException {
        if (colName == null || colName.length() == 0
            || tabName == null || tabName.length() == 0) {
            throw new SQLException(Support.getMessage("error.tdscore.badtext"), "HY000");
        }

        if (textPtr == null) {
            throw new SQLException(
                Support.getMessage("error.tdscore.notextptr", tabName + "." + colName),
                                     "HY000");
        }

        Object results = null;
        StringBuffer sql = new StringBuffer(256);

        sql.append("set textsize ");
        sql.append((length + 1) * 2);
        sql.append("\r\nreadtext ");
        sql.append(tabName);
        sql.append('.');
        sql.append(colName);
        sql.append(" 0x");
        sql.append(Support.toHex(textPtr.ptr));
        sql.append(' ');
        sql.append(offset);
        sql.append(' ');
        sql.append(length);
        sql.append("\r\nset textsize 1");

        if (Logger.isActive()) {
            Logger.println(sql.toString());
        }

        executeSQL(sql.toString(), null, null, false, 0, 0);
        readTextMode = true;

        if (getMoreResults()) {
            if (getNextRow(false)) {
            	// FIXME - this will not be valid since a Blob/Clob is returned
            	// instead of byte[]/String
                results = rowData[0].getValue();
            }
        }

        clearResponseQueue();
        messages.checkErrors();

System.out.println("results.getClass().getName()" + results.getClass().getName());
        return results;
    }

    /**
     * Retrieve the length of a text or image column.
     *
     * @param tabName The parent table for this column.
     * @param colName The name of the text or image column.
     * @return The length of the column as a <code>int</code>.
     * @throws SQLException
     */
    int dataLength(String tabName, String colName) throws SQLException {
        if (colName == null || colName.length() == 0
            || tabName == null || tabName.length() == 0) {
            throw new SQLException(Support.getMessage("error.tdscore.badtext"), "HY000");
        }

        Object results = null;
        StringBuffer sql = new StringBuffer(128);

        sql.append("select datalength(");
        sql.append(colName);
        sql.append(") from ");
        sql.append(tabName);
        executeSQL(sql.toString(), null, null, false, 0, 0);

        if (getMoreResults()) {
            if (getNextRow(false)) {
                results = rowData[0].getValue();
            }
        }

        clearResponseQueue();
        messages.checkErrors();

        if (!(results instanceof Number)) {
            throw new SQLException(
                Support.getMessage("error.tdscore.badlen", tabName + "." + colName),
                "HY000");
        }

        return ((Number) results).intValue();
    }

// ---------------------- Private Methods from here ---------------------

    /**
     * Write a TDS login packet string. Text followed by padding followed
     * by a byte sized length.
     */
    private void putLoginString(String txt, int len)
        throws IOException
    {
        byte[] tmp = Support.encodeString(connection.getCharSet(), txt);
        out.write(tmp, 0, len);
        out.write((byte) (tmp.length < len ? tmp.length : len));
    }

    /**
     * TDS 4.2 Login Packet.
     * <P>
     * @param serverName The server host name.
     * @param user The user name.
     * @param password The user password.
     * @param charset The required server character set.
     * @param appName The application name.
     * @param libName The library name.
     * @param language The server language for messages
     * @param packetSize The required network packet size.
     * @throws IOException
     */
    private void send42LoginPkt(final String serverName,
                                final String user,
                                final String password,
                                final String charset,
                                final String appName,
                                final String libName,
                                final String language,
                                final int    packetSize)
        throws IOException
    {
        final byte pad = (byte) 0;
        final byte[] empty = new byte[0];

        out.setPacketType(LOGIN_PKT);
        putLoginString(getHostName(), 30);  // Host name
        putLoginString(user, 30);           // user name
        putLoginString(password, 30);       // password
        putLoginString("00000123", 30);     // hostproc (offset 93 0x5d)

        out.write((byte) 3); // type of int2
        out.write((byte) 1); // type of int4
        out.write((byte) 6); // type of char
        out.write((byte) 10);// type of flt
        out.write((byte) 9); // type of date
        out.write((byte) 1); // notify of use db
        out.write((byte) 1); // disallow dump/load and bulk insert
        out.write((byte) 0); // sql interface type
        out.write((byte) 0); // type of network connection

        out.write(empty, 0, 7);

        putLoginString(appName, 30);  // appname
        putLoginString(serverName, 30); // server name

        out.write((byte)0); // remote passwords
        out.write((byte)password.length());
        byte[] tmp = Support.encodeString(connection.getCharSet(), password);
        out.write(tmp, 0, 253);
        out.write((byte) (tmp.length + 2));

        out.write((byte) 4);  // tds version
        out.write((byte) 2);

        out.write((byte) 0);
        out.write((byte) 0);
        putLoginString(libName, 10); // prog name

        out.write((byte) 6);  // prog version
        out.write((byte) 0);
        out.write((byte) 0);
        out.write((byte) 0);

        out.write((byte) 0);  // auto convert short
        out.write((byte) 0x0D); // type of flt4
        out.write((byte) 0x11); // type of date4

        putLoginString(language, 30);  // language

        out.write((byte) 1);  // notify on lang change
        out.write((short) 0);  // security label hierachy
        out.write((byte) 0);  // security encrypted
        out.write(empty, 0, 8);  // security components
        out.write((short) 0);  // security spare

        putLoginString(charset, 30); // Character set

        out.write((byte) 1);  // notify on charset change
        putLoginString(String.valueOf(packetSize), 6); // length of tds packets

        out.write(empty, 0, 8);  // pad out to a longword

        out.flush(); // Send the packet
        endOfResponse = false;
    }

    /**
     * TDS 5.0 Login Packet.
     * <P>
     * @param serverName The server host name.
     * @param user The user name.
     * @param password The user password.
     * @param charset The required server character set.
     * @param appName The application name.
     * @param libName The library name.
     * @param language The server language for messages
     * @param packetSize The required network packet size.
     * @throws IOException
     */
    private void send50LoginPkt(final String serverName,
                              final String user,
                              final String password,
                              final String charset,
                              final String appName,
                              final String libName,
                              final String language,
                              final int    packetSize)
        throws IOException
    {
        final byte pad = (byte) 0;
        final byte[] empty = new byte[0];

        out.setPacketType(LOGIN_PKT);
        putLoginString(getHostName(), 30);  // Host name
        putLoginString(user, 30);           // user name
        putLoginString(password, 30);       // password
        putLoginString("00000123", 30);     // hostproc (offset 93 0x5d)

        out.write((byte) 3); // type of int2
        out.write((byte) 1); // type of int4
        out.write((byte) 6); // type of char
        out.write((byte) 10);// type of flt
        out.write((byte) 9); // type of date
        out.write((byte) 1); // notify of use db
        out.write((byte) 1); // disallow dump/load and bulk insert
        out.write((byte) 0); // sql interface type
        out.write((byte) 0); // type of network connection

        out.write(empty, 0, 7);

        putLoginString(appName, 30);  // appname
        putLoginString(serverName, 30); // server name
        out.write((byte)0); // remote passwords
        out.write((byte)password.length());
        byte[] tmp = Support.encodeString(connection.getCharSet(), password);
        out.write(tmp, 0, 253);
        out.write((byte) (tmp.length + 2));

        out.write((byte) 5);  // tds version
        out.write((byte) 0);

        out.write((byte) 0);
        out.write((byte) 0);
        putLoginString(libName, 10); // prog name

        out.write((byte) 5);  // prog version
        out.write((byte) 0);
        out.write((byte) 0);
        out.write((byte) 0);

        out.write((byte) 0);  // auto convert short
        out.write((byte) 0x0D); // type of flt4
        out.write((byte) 0x11); // type of date4

        putLoginString(language, 30);  // language

        out.write((byte) 1);  // notify on lang change
        out.write((short) 0);  // security label hierachy
        out.write((byte) 0);  // security encrypted
        out.write(empty, 0, 8);  // security components
        out.write((short) 0);  // security spare

        putLoginString(charset, 30); // Character set

        out.write((byte) 1);  // notify on charset change
        putLoginString(String.valueOf(packetSize), 6); // length of tds packets

        out.write(empty, 0, 4);
        // Send capability request
        byte capString[] = {
            0x01,0x07,0x03,0x6D,0x7F,(byte)0xFF,(byte)0xFF,(byte)0xFF,
            (byte)0xFE,0x02,0x07,0x00,0x00,0x0A,0x68,0x00,0x00,0x00
        };
        out.write(TDS_CAP_TOKEN);
        out.write((short)capString.length);
        out.write(capString);

        out.flush(); // Send the packet
        endOfResponse = false;
    }

    /**
     * Send a TDS 7 login packet.
     * <p>
     * @param serverName The server host name.
     * @param database The required database.
     * @param user The user name.
     * @param password The user password.
     * @param domain The Windows NT domain (or null).
     * @param appName The application name.
     * @param libName The library name.
     * @param language The server language for messages
     * @param macAddress The client network MAC address.
     * @throws IOException
     */
    private void sendMSLoginPkt(final String serverName,
                        final String database,
                        final String user,
                        final String password,
                        final String domain,
                        final String appName,
                        final String libName,
                        final String language,
                        final String macAddress,
                        final int    netPacketSize)
        throws IOException
    {
        final byte pad = (byte) 0;
        final byte[] empty = new byte[0];
        final String clientName = getHostName();

        //mdb
        final boolean ntlmAuth = (domain.length() > 0);

        //mdb:begin-change
        short packSize = (short) (86 + 2 *
                (clientName.length() +
                appName.length() +
                serverName.length() +
                libName.length() +
                database.length() +
                language.length()));
        final short authLen;
        //NOTE(mdb): ntlm includes auth block; sql auth includes uname and pwd.
        if (ntlmAuth) {
            authLen = (short) (32 + domain.length());
            packSize += authLen;
        } else {
            authLen = 0;
            packSize += (2 * (user.length() + password.length()));
        }
        //mdb:end-change

        out.setPacketType(MSLOGIN_PKT);
        out.write((int)packSize);
        // TDS version
        if (tdsVersion == Driver.TDS70) {
            out.write((int)0x70000000);
        } else {
            out.write((int)0x71000001);
        }
        // Network Packet size requested by client
        out.write((int)netPacketSize);
        // Program version?
        out.write((int)7);
        // Process ID
        out.write((int)123);
        // Connection ID
        out.write((int)0);
        byte flags = 0;
        flags |= 0x80; // enable warning messages if SET LANGUAGE issued
        flags |= 0x40; // change to initial database must succeed
        flags |= 0x20; // enable warning messages if USE <database> issued
        out.write((byte) flags);

        //mdb: this byte controls what kind of auth we do.
        flags = 0x03; // ODBC (JDBC) driver
        if (ntlmAuth)
            flags |= 0x80; // Use NT authentication
        out.write((byte)flags);

        out.write((byte)0); // SQL type flag
        out.write((byte)0); // Reserved flag
        // TODO Set Timezone and collation?
        out.write(empty, 0, 4); // Time Zone
        out.write(empty, 0, 4); // Collation

        // Pack up value lengths, positions.
        short curPos = 86;

        // Hostname
        out.write((short)curPos);
        out.write((short) clientName.length());
        curPos += clientName.length() * 2;

        //mdb: NTLM doesn't send username and password...
        if (!ntlmAuth) {
            // Username
            out.write((short)curPos);
            out.write((short) user.length());
            curPos += user.length() * 2;

            // Password
            out.write((short)curPos);
            out.write((short) password.length());
            curPos += password.length() * 2;
        } else {
            out.write((short)curPos);
            out.write((short) 0);

            out.write((short)curPos);
            out.write((short) 0);
        }

        // App name
        out.write((short)curPos);
        out.write((short) appName.length());
        curPos += appName.length() * 2;

        // Server name
        out.write((short)curPos);
        out.write((short) serverName.length());
        curPos += serverName.length() * 2;

        // Unknown
        out.write((short) 0);
        out.write((short) 0);

        // Library name
        out.write((short)curPos);
        out.write((short) libName.length());
        curPos += libName.length() * 2;

        // Server language
        out.write((short)curPos);
        out.write((short) language.length());
        curPos += language.length() * 2;

        // Database
        out.write((short)curPos);
        out.write((short) database.length());
        curPos += database.length() * 2;

        // MAC address
        out.write(getMACAddress(macAddress));

        //mdb: location of ntlm auth block. note that for sql auth, authLen==0.
        out.write((short)curPos);
        out.write((short)authLen);

        //"next position" (same as total packet size)
        out.write((int)packSize);

        out.write(clientName);

        // Pack up the login values.
        //mdb: for ntlm auth, uname and pwd aren't sent up...
        if (!ntlmAuth) {
            final String scrambledPw = tds7CryptPass(password);
            out.write(user);
            out.write(scrambledPw);
        }

        out.write(appName);
        out.write(serverName);
        out.write(libName);
        out.write(language);
        out.write(database);

        //mdb: add the ntlm auth info...
        if (ntlmAuth) {
            // host and domain name are _narrow_ strings.
            final byte[] domainBytes = domain.getBytes("UTF8");
            //byte[] hostBytes   = localhostname.getBytes("UTF8");

            final byte[] header = {0x4e, 0x54, 0x4c, 0x4d, 0x53, 0x53, 0x50, 0x00};
            out.write(header); //header is ascii "NTLMSSP\0"
            out.write((int)1);          //sequence number = 1
            out.write((int)0xb201);     //flags (???)

            //domain info
            out.write((short) domainBytes.length);
            out.write((short) domainBytes.length);
            out.write((int)32); //offset, relative to start of auth block.

            //host info
            //NOTE(mdb): not sending host info; hope this is ok!
            out.write((short) 0);
            out.write((short) 0);
            out.write((int)32); //offset, relative to start of auth block.

            // add the variable length data at the end...
            out.write(domainBytes);
        }
        out.flush(); // Send the packet
        endOfResponse = false;
    }

    /**
     * Send the response to the NTLM authentication challenge.
     * @param nonce The secret to hash with password.
     * @param user The user name.
     * @param password The user password.
     * @param domain The Windows NT Dommain.
     * @throws java.io.IOException
     */
    private void sendNtlmChallengeResponse(final byte[] nonce,
                                           final String user,
                                           final String password,
                                           final String domain)
            throws java.io.IOException {
        out.setPacketType(NTLMAUTH_PKT);
        // host and domain name are _narrow_ strings.
        //byte[] domainBytes = domain.getBytes("UTF8");
        //byte[] user        = user.getBytes("UTF8");

        final byte[] header = {0x4e, 0x54, 0x4c, 0x4d, 0x53, 0x53, 0x50, 0x00};
        out.write(header); //header is ascii "NTLMSSP\0"
        out.write((int)3); //sequence number = 3
        final int domainLenInBytes = domain.length() * 2;
        final int userLenInBytes = user.length() * 2;
        //mdb: not sending hostname; I hope this is ok!
        final int hostLenInBytes = 0; //localhostname.length()*2;
        int pos = 64 + domainLenInBytes + userLenInBytes + hostLenInBytes;
        // lan man response: length and offset
        out.write((short)24);
        out.write((short)24);
        out.write((int)pos);
        pos += 24;
        // nt response: length and offset
        out.write((short)24);
        out.write((short)24);
        out.write((int)pos);
        pos = 64;
        //domain
        out.write((short) domainLenInBytes);
        out.write((short) domainLenInBytes);
        out.write((int)pos);
        pos += domainLenInBytes;

        //user
        out.write((short) userLenInBytes);
        out.write((short) userLenInBytes);
        out.write((int)pos);
        pos += userLenInBytes;
        //local hostname
        out.write((short) hostLenInBytes);
        out.write((short) hostLenInBytes);
        out.write((int)pos);
        pos += hostLenInBytes;
        //unknown
        out.write((short) 0);
        out.write((short) 0);
        out.write((int)pos);
        //flags
        out.write((int)0x8201);     //flags (???)
        //variable length stuff...
        out.write(domain);
        out.write(user);
        //Not sending hostname...I hope this is OK!
        //comm.appendChars(localhostname);

        //the response to the challenge...
        final byte[] lmAnswer = NtlmAuth.answerLmChallenge(password, nonce);
        final byte[] ntAnswer = NtlmAuth.answerNtChallenge(password, nonce);
        out.write(lmAnswer);
        out.write(ntAnswer);
        out.flush();
    }

    /**
     * Read the next TDS token from the response stream.
     * @return The Next TDS token as a <code>TdsToken</code>.
     * @throws SQLException
     */
    private void nextToken()
        throws SQLException
    {
        checkOpen();
        if (endOfResponse) {
            currentToken.token  = TDS_DONE_TOKEN;
            currentToken.status = 0;
            return;
        }
        try {
            currentToken.token = (byte)in.read();
            switch (currentToken.token) {
                case TDS_LANG_TOKEN:
                    tdsInvalidToken();
                    break;
                case TDS_CLOSE_TOKEN:
                    tdsInvalidToken();
                    break;
                case TDS_RETURNSTATUS_TOKEN:
                    tdsReturnStatusToken();
                    break;
                case TDS_PROCID:
                    tdsProcIdToken();
                    break;
                case TDS7_RESULT_TOKEN:
                    tds7ResultToken();
                    break;
                case TDS7_COMP_RESULT_TOKEN:
                    tdsInvalidToken();
                    break;
                case TDS_COLNAME_TOKEN:
                    tds4ColNamesToken();
                    break;
                case TDS_COLFMT_TOKEN:
                    tds4ColFormatToken();
                    break;
                case TDS_TABNAME_TOKEN:
                    tdsTableNameToken();
                    break;
                case TDS_COLINFO_TOKEN:
                    tdsColumnInfoToken();
                    break;
                case TDS_COMP_NAMES_TOKEN:
                    tdsInvalidToken();
                    break;
                case TDS_COMP_RESULT_TOKEN:
                    tdsInvalidToken();
                    break;
                case TDS_ORDER_TOKEN:
                    tdsOrderByToken();
                    break;
                case TDS_ERROR_TOKEN:
                case TDS_INFO_TOKEN:
                    tdsErrorToken();
                    break;
                case TDS_PARAM_TOKEN:
                    tdsOutputParamToken();
                    break;
                case TDS_LOGINACK_TOKEN:
                    tdsLoginAckToken();
                    break;
                case TDS_CONTROL_TOKEN:
                    tdsControlToken();
                    break;
                case TDS_ROW_TOKEN:
                    tdsRowToken();
                    break;
                case TDS_ALTROW:
                    tdsInvalidToken();
                    break;
                case TDS5_PARAMS_TOKEN:
                    tds5ParamsToken();
                    break;
                case TDS_CAP_TOKEN:
                    tdsCapabilityToken();
                    break;
                case TDS_ENVCHANGE_TOKEN:
                    tdsEnvChangeToken();
                    break;
                case TDS_MSG50_TOKEN:
                    tds5ErrorToken();
                    break;
                case TDS5_DYNAMIC_TOKEN:
                    tds5DynamicToken();
                    break;
                case TDS5_PARAMFMT_TOKEN:
                    tds5ParamFmtToken();
                    break;
                case TDS_AUTH_TOKEN:
                    tdsNtlmAuthToken();
                    break;
                case TDS_RESULT_TOKEN:
                    tds5ResultToken();
                    break;
                case TDS_DONE_TOKEN:
                case TDS_DONEPROC_TOKEN:
                case TDS_DONEINPROC_TOKEN:
                    tdsDoneToken();
                    break;
                default:
                    throw new ProtocolException(
                            "Invalid packet type 0x" +
                                Integer.toHexString(currentToken.token));
            }
        } catch (IOException ioe) {
            isClosed = true;
            connection.setClosed();
            throw Support.linkException(
                new SQLException(
                       Support.getMessage(
                                "error.generic.ioerror", ioe.getMessage()),
                                    "08S01"), ioe);
        } catch (ProtocolException pe) {
            isClosed = true;
            connection.setClosed();
            throw Support.linkException(
                new SQLException(
                       Support.getMessage(
                                "error.generic.tdserror", pe.getMessage()),
                                    "08S01"), pe);
        }
    }

    /**
     * Report unsupported TDS token in input stream.
     *
     * @throws IOException
     */
    private void tdsInvalidToken()
        throws IOException, ProtocolException
    {
        in.skip(in.readShort());
        throw new ProtocolException("Unsupported TDS token: 0x" +
                            Integer.toHexString(currentToken.token & 0xFF));
    }

    /**
     * Process stored procedure return status token.
     *
     * @throws IOException
     */
    private void tdsReturnStatusToken()
        throws IOException
    {
        returnStatus = new Integer(in.readInt());
        if (this.returnParam != null) {
            returnParam.jdbcType = java.sql.Types.INTEGER;
            returnParam.value = returnStatus;
            returnParam.isSet = true;
        }
    }

    /**
     * Process procedure ID token (function unknown).
     *
     * @throws IOException
     */
    private void tdsProcIdToken() throws IOException {
        in.skip(8);
    }

    /**
     * Process a TDS 7.0 result set token.
     *
     * @throws IOException
     * @throws ProtocolException
     */
    private void tds7ResultToken() throws IOException, ProtocolException {
        int colCnt = in.readShort();
        this.columns = new ColInfo[colCnt];
        this.rowData = new ColData[colCnt];
        this.tables = null;

        for (int i = 0; i < colCnt; i++) {
            ColInfo col = new ColInfo();

            col.userType = in.readShort();

            int flags = in.readShort();

            col.nullable = ((flags & 0x01) != 0) ?
                                ResultSetMetaData.columnNullable :
                                ResultSetMetaData.columnNoNulls;
            col.isCaseSensitive = (flags & 0X02) != 0;
            col.isIdentity = (flags & 0x10) != 0;
            col.isWriteable = (flags & 0x0C) != 0;
            TdsData.readType(in, col);

            int clen = in.read();

            col.name = in.readString(clen);
            col.label = col.name;

            this.columns[i] = col;
        }

        endOfResults = false;
    }

    /**
     * Process a TDS 4.2 column names token.
     * <p>
     * Note: Will be followed by a COL_FMT token.
     *
     * @throws IOException
     */
    private void tds4ColNamesToken() throws IOException {
        ArrayList colList = new ArrayList();

        final int pktLen = in.readShort();

        int bytesRead = 0;
        int i = 0;

        while (bytesRead < pktLen) {
            ColInfo col = new ColInfo();
            int nameLen = in.read();
            String name = in.readString(nameLen);

            bytesRead = bytesRead + 1 + nameLen;
            i++;
            col.name  = name;
            col.label = name;

            colList.add(col);
        }

        int colCnt  = colList.size();
        this.columns = (ColInfo[]) colList.toArray(new ColInfo[colCnt]);
        this.rowData = new ColData[colCnt];
    }

    /**
     * Process a TDS 4.2 column format token.
     *
     * @throws IOException
     * @throws ProtocolException
     */
    private void tds4ColFormatToken()
        throws IOException, ProtocolException
    {
        int precision;
        int scale;

        final int pktLen = in.readShort();

        int bytesRead = 0;
        int numColumns = 0;
        while (bytesRead < pktLen) {
            if (numColumns > columns.length) {
                throw new ProtocolException("Too many columns in TDS_COL_FMT packet");
            }
            scale = -1;
            precision = -1;
            ColInfo col = columns[numColumns];
            int bufLength;
            int dispSize = -1;
            if (serverType == Driver.SQLSERVER) {
                col.userType = in.readShort();
                int flags    = in.readShort();
                col.nullable = ((flags & 0x01) != 0)?
                                    ResultSetMetaData.columnNullable:
                                       ResultSetMetaData.columnNoNulls;
                col.isCaseSensitive = (flags & 0x02) != 0;
                col.isWriteable     = (flags & 0x0C) != 0;
                col.isIdentity      = (flags & 0x10) != 0;
            } else {
                // Sybase does not send column flags
                col.isCaseSensitive = false;
                col.isWriteable     = true;
                if (col.nullable == ResultSetMetaData.columnNoNulls) {
                    col.nullable = ResultSetMetaData.columnNullableUnknown;
                }
                col.userType = in.readInt();
            }
            bytesRead += 4;
            String tableName = "";

            bytesRead += TdsData.readType(in, col);

            numColumns++;
        }
        if (numColumns != columns.length) {
            throw new ProtocolException("Too few columns in TDS_COL_FMT packet");
        }
        endOfResults = false;
    }

    /**
     * Process a table name token.
     * <p> Sent by select for browse or cursor functions.
     *
     * @throws IOException
     */
    private void tdsTableNameToken()
        throws IOException
    {
        final int pktLen = in.readShort();
        int bytesRead = 0;

        String tabName;
        ArrayList tableList = new ArrayList();

        if (tdsVersion >= Driver.TDS70) {
            while (bytesRead < pktLen) {
                if (tdsVersion == Driver.TDS80) {
                    // Not sure what this is used for
                    int flag = in.read(); bytesRead++;
                }
                int nameLen = in.readShort();
                bytesRead += 2;
                tabName = in.readString(nameLen);
                bytesRead += (nameLen * 2);
                tableList.add(tabName);
            }
        } else {
            while (bytesRead < pktLen) {
                int nameLen = in.read();
                bytesRead++;
                tabName = in.readString(nameLen);
                bytesRead += nameLen;
                tableList.add(tabName);
            }
        }
        this.tables = (String[])tableList.toArray(new String[tableList.size()]);
    }

    /**
     * Process a column infomation token.
     * <p>Sent by select for browse or cursor functions.
     * @throws IOException
     * @throws ProtocolException
     */
    private void tdsColumnInfoToken()
        throws IOException, ProtocolException
    {
        final int pktLen = in.readShort();
        int bytesRead = 0;

        // In some cases (e.g. if the user calls 'CREATE CURSOR', the
        // TDS_TABNAME packet seems to be missing. Weird.
        if (tables == null) {
            in.skip(pktLen);
        } else {
            while (bytesRead < pktLen) {
                int columnIndex = in.read();
                if (columnIndex < 1 || columnIndex > columns.length) {
                    throw new ProtocolException("Column index " + columnIndex +
                                                     " invalid in TDS_COLINFO packet");
                }
                ColInfo col = columns[columnIndex-1];
                int tableIndex = in.read();
                if (tableIndex > tables.length) {
                    throw new ProtocolException("Table index " + tableIndex +
                                                     " invalid in TDS_COLINFO packet");
                }
                byte flags = (byte)in.read(); // flags
                bytesRead += 3;

                if (tableIndex != 0) {
                    String tabName = tables[tableIndex-1];

                    // tabName can be a fully qualified name
                    int dotPos = tabName.lastIndexOf('.');
                    if (dotPos > 0) {
                        col.tableName = tabName.substring(dotPos + 1);

                        int nextPos = tabName.lastIndexOf('.', dotPos-1);
                        if (nextPos + 1 < dotPos) {
                            col.schema = tabName.substring(nextPos + 1, dotPos);
                        }

                        dotPos = nextPos;
                        nextPos = tabName.lastIndexOf('.', dotPos-1);
                        if (nextPos + 1 < dotPos) {
                            col.catalog = tabName.substring(nextPos + 1, dotPos);
                        }
                    } else {
                        col.tableName = tabName;
                    }
                }

                col.isKey           = (flags & 0x08) != 0;
                col.isHidden        = (flags & 0x10) != 0;

                // If bit 5 is set, we have a column name
                if ((flags & 0x20) != 0) {
                    final int nameLen = in.read();
                    bytesRead += 1;
                    final String colName = in.readString(nameLen);
                    bytesRead += (tdsVersion >= Driver.TDS70)? nameLen * 2: nameLen;
                    col.name = colName;
                }
            }
        }
    }

    /**
     * Process an order by token.
     * <p>Sent to describe columns in an order by clause.
     * @throws IOException
     */
    private void tdsOrderByToken()
        throws IOException
    {
        // Skip this packet type
        int pktLen = in.readShort();
        in.skip(pktLen);
    }

    /**
     * Process a TD4/TDS7 error or informational message.
     *
     * @throws IOException
     */
    private void tdsErrorToken()
    throws IOException
    {
        int pktLen = in.readShort(); // Packet length
        int sizeSoFar = 6;
        int number = in.readInt();
        int state = in.read();
        int severity = in.read();
        int msgLen = in.readShort();
        String message = in.readString(msgLen);
        sizeSoFar += 2 + ((tdsVersion >= Driver.TDS70)? msgLen * 2: msgLen);
        final int srvNameLen = in.read();
        String server = in.readString(srvNameLen);
        sizeSoFar += 1 + ((tdsVersion >= Driver.TDS70)? srvNameLen * 2: srvNameLen);

        final int procNameLen = in.read();
        String procName = in.readString(procNameLen);
        sizeSoFar += 1 + ((tdsVersion >= Driver.TDS70)? procNameLen * 2: procNameLen);

        int line = in.readShort();
        sizeSoFar += 2;
        // Skip any EED information to read rest of packet
        if (pktLen - sizeSoFar > 0)
            in.skip(pktLen - sizeSoFar);

        if (currentToken.token == TDS_ERROR_TOKEN)
        {
            if (severity < 10) {
                severity = 11; // Ensure treated as error
            }
            messages.addDiagnostic(number, state, severity,
                                    message, server, procName, line);
        } else {
            if (severity > 9) {
                severity = 9; // Ensure treated as warning
            }
            messages.addDiagnostic(number, state, severity,
                                     message, server, procName, line);
        }
    }

    /**
     * Process output parameters.
     *
     * @throws IOException
     * @throws ProtocolException
     */
    private void tdsOutputParamToken()
        throws IOException, ProtocolException
    {
        int pktLen = in.readShort(); // Packet length
        String colName = in.readString(in.read());
        in.skip(5);

        ColInfo col = new ColInfo();
        TdsData.readType(in, col);
        Object value = TdsData.readData(connection, in, col, false);

        if (tdsVersion >= Driver.TDS80 &&
            returnParam != null &&
            !returnParam.isSet )
        {
            // TDS 8 Allows function return values of types other than int
                parameters[nextParam].jdbcType  = col.jdbcType;
                parameters[nextParam].value     = value;
                parameters[nextParam].collation = col.collation;
        } else {
            // Look for next output parameter in list
            if (parameters != null) {
                while (++nextParam < parameters.length) {
                    if (parameters[nextParam].isOutput) {
                        parameters[nextParam].jdbcType  = col.jdbcType;
                        parameters[nextParam].value     = value;
                        parameters[nextParam].collation = col.collation;
                        break;
                    }
                }
            }
        }
     }

    /**
     * Process a login acknowledgement packet.
     *
     * @throws IOException
     */
    private void tdsLoginAckToken()
        throws IOException
    {
        String product;
        int major, minor, build = 0;
        int pktLen = in.readShort(); // Packet length
        int ack = 1;
        if (tdsVersion >= Driver.TDS70) {
            in.skip(5);
            final int nameLen = in.read();
            product = in.readString(nameLen);
            major = in.read();
            minor = in.read();
            build = in.read() << 8;
            build += in.read();
        } else {
            ack = in.read(); // Ack TDS 5 = 5 for OK 6 for fail
            in.skip(4);
            final int nameLen = in.read();
            product = in.readString(nameLen);
            if (product.toLowerCase().startsWith("microsoft")) {
                in.skip(1);
                major = in.read();
                minor = in.read();
            } else {
                major = in.read();
                minor = in.read() * 10;
                minor += in.read();
            }
            in.skip(1);
        }
        if (product.length() > 1 && -1 != product.indexOf('\0')) {
            product = product.substring(0, product.indexOf('\0'));
        }

        connection.setDBServerInfo(product, major, minor, build);

        if (tdsVersion == Driver.TDS50 && ack != 5) {
            // Login ejected by server create SQLException
            messages.addDiagnostic(4002, 0, 14,
                                    "Login failed", "", "", 0);
            currentToken.token = TDS_ERROR_TOKEN;
        }
    }

    /**
     * Process a control token (function unknown).
     *
     * @throws IOException
     */
    private void tdsControlToken() throws IOException {
        int pktLen = in.readShort();

        in.skip(pktLen);
    }

    /**
     * Process a row data token.
     *
     * @throws IOException
     * @throws ProtocolException
     */
    private void tdsRowToken() throws IOException, ProtocolException {
        for (int i = 0; i < columns.length; i++) {
            rowData[i] =  new ColData(TdsData.readData(connection, in, columns[i], readTextMode), tdsVersion);
        }

        readTextMode = false;
    }

    /**
     * Process TDS 5.0 Params Token.
     * This seems to be data returned in parameter format after a TDS Dynamic
     * packet or as extended error information.
     *
     * @throws IOException
     */
    private void tds5ParamsToken() throws IOException, ProtocolException {
        if (currentToken.dynamParamInfo == null) {
            throw new ProtocolException(
              "TDS 5 Param results token (0xD7) not preceded by param format (0xEC).");
        }

        for (int i = 0; i < currentToken.dynamParamData.length; i++) {
            currentToken.dynamParamData[i] =
                new ColData(TdsData.readData(connection, in, currentToken.dynamParamInfo[i], false), tdsVersion);
        }
    }

    /**
     * Process a TDS 5.0 capability token.
     * <p>Sent after login to describe the servers capabilities.
     *
     * @throws IOException
     */
    private void tdsCapabilityToken()
        throws IOException
    {
        int pktLen = in.readShort(); // Packet length
        // Sybase < 12 can return wrong size here
        // TODO use capability info
        if (pktLen != 18)
            pktLen = 18; // May be suspect
        in.skip(pktLen);
    }

    /**
     * Process an environment change packet.
     *
     * @throws IOException
     * @throws SQLException
     */
    private void tdsEnvChangeToken()
        throws IOException, SQLException
    {
        int len = in.readShort();
        int type = in.read();

        switch (type) {
            case TDS_ENV_DATABASE:
                {
                    int clen = in.read();
                    final String newDb = in.readString(clen);
                    clen = in.read();
                    final String oldDb = in.readString(clen);
                    connection.setDatabase(newDb, oldDb);
                    break;
                }

            case TDS_ENV_LANG:
                {
                    int clen = in.read();
                    String language = in.readString(clen);
                    clen = in.read();
                    String oldLang = in.readString(clen);
                    if (Logger.isActive()) {
                        Logger.println("Language changed from " + oldLang + " to " + language);
                    }
                    break;
                }

            case TDS_ENV_CHARSET:
                {
                    final int clen = in.read();
                    final String charset = in.readString(clen);
                    if (tdsVersion >= Driver.TDS70) {
                        in.skip(len - 2 - clen * 2);
                    } else {
                        in.skip(len - 2 - clen);
                    }
                    connection.setCharset(charset);
                    break;
                }

            case TDS_ENV_PACKSIZE:
                    {
                        final int blocksize;
                        final int clen = in.read();
                        blocksize = Integer.parseInt(in.readString(clen));
                        if (tdsVersion >= Driver.TDS70) {
                            in.skip(len - 2 - clen * 2);
                        } else {
                            in.skip(len - 2 - clen);
                        }
                        this.connection.setNetPacketSize(blocksize);
                        out.setBufferSize(blocksize);
                        if (Logger.isActive()) {
                            Logger.println("Changed blocksize to " + blocksize);
                        }
                    }
                    break;

            case TDS_ENV_LCID:
                    // Only sent by TDS 7
                    // In TDS 8 replaced by column specific collation info.
                    // TODO Make use of this for character set conversions?
                    in.skip(len - 1);
                    break;

            case TDS_ENV_SQLCOLLATION:
                {
                    int clen = in.read();
                    byte collation[] = new byte[5];
                    if (clen == 5) {
                        in.read(collation);
                        connection.setCollation(collation);
                        if (Logger.isActive()) {
                            Logger.println("Collation changed to 0x" + Support.toHex(collation));
                        }
                    } else {
                        in.skip(clen);
                    }
                    clen = in.read();
                    in.skip(clen);
                    break;
                }

            default:
                {
                    if (Logger.isActive()) {
                        Logger.println("Unknown environment change type 0x" +
                                            Integer.toHexString(type));
                    }
                    in.skip(len - 1);
                    break;
                }
        }
    }

    /**
     * Process a TDS 5 error or informational message.
     *
     * @throws IOException
     */
    private void tds5ErrorToken()
    throws IOException
    {
        boolean hasEED = false;
        int pktLen = in.readShort(); // Packet length
        int sizeSoFar = 6;
        int number = in.readInt();
        int state = in.read();
        int severity = in.read();
        // Discard text state
        int stateLen = in.read();
        in.readString(stateLen);
        // True if extended error data follows
        hasEED = in.read() == 1;
        // Discard status and transaction state
        in.readShort();
        sizeSoFar += 4 + stateLen;

        int msgLen = in.readShort();
        String message = in.readString(msgLen);
        sizeSoFar += 2 + msgLen;
        final int srvNameLen = in.read();
        String server = in.readString(srvNameLen);
        sizeSoFar += 1 + srvNameLen;

        final int procNameLen = in.read();
        String procName = in.readString(procNameLen);
        sizeSoFar += 1 + procNameLen;

        int line = in.readShort();
        sizeSoFar += 2;
        // Skip any EED information to read rest of packet
        if (pktLen - sizeSoFar > 0)
            in.skip(pktLen - sizeSoFar);

        if (severity > 10)
        {
            messages.addDiagnostic(number, state, severity,
                                    message, server, procName, line);
        } else {
            messages.addDiagnostic(number, state, severity,
                                     message, server, procName, line);
        }
    }

    /**
     * Process TDS5 dynamic SQL aknowledgements.
     *
     * @throws IOException
     */
    private void tds5DynamicToken()
        throws IOException
    {
        int pktLen = in.readShort();
        byte type = (byte)in.read();
        byte status = (byte)in.read();
        pktLen -= 2;
        if (type == (byte)0x20) {
            // Only handle aknowledgements for now
            int len = in.read();
            currentToken.dynamicId = in.readString(len);
            pktLen -= len+1;
        }
        in.skip(pktLen);
    }

    /**
     * Process TDS 5 Dynamic results paramater descriptors.
     * @throws IOException
     * @throws ProtocolException
     */
    private void tds5ParamFmtToken()
        throws IOException, ProtocolException
    {
        int pktLen = in.readShort();
        int paramCnt = in.readShort();
        ColInfo[] params = new ColInfo[paramCnt];
        for (int i = 0; i < paramCnt; i++) {
            //
            // Get the parameter details using the
            // ColInfo class as the server format is the same.
            //
            ColInfo col = new ColInfo();
            int colNameLen = in.read();
            col.name = in.readString(colNameLen);
            int column_flags = in.read();   /*  Flags */
            col.isCaseSensitive = false;
            col.nullable    = ((column_flags & 0x20) != 0)?
                                        ResultSetMetaData.columnNullable:
                                        ResultSetMetaData.columnNoNulls;
            col.isWriteable = (column_flags & 0x10) != 0;
            col.isIdentity  = (column_flags & 0x40) != 0;
            col.isKey       = (column_flags & 0x02) != 0;
            col.isHidden    = (column_flags & 0x01) != 0;

            col.userType    = in.readInt();
            if ((byte)in.peek() == TDS_DONE_TOKEN) {
                // Sybase 11.92 bug data type missing!
                currentToken.dynamParamInfo = null;
                currentToken.dynamParamData = null;
                // error trapped in sybasePrepare();
                messages.addDiagnostic(9999, 0, 16,
                                        "Prepare failed", "", "", 0);

                return; // Give up
            }
            TdsData.readType(in, col);
            // Skip locale information
            in.skip(1);
            params[i] = col;
        }
        currentToken.dynamParamInfo = params;
        currentToken.dynamParamData = new ColData[paramCnt];
    }

    /**
     * Process a NTLM Authentication challenge.
     *
     * @throws IOException
     * @throws ProtocolException
     */
    private void tdsNtlmAuthToken()
        throws IOException, ProtocolException
    {
        int pktLen = in.readShort(); // Packet length

        int hdrLen = 40;

        if (pktLen < hdrLen)
            throw new ProtocolException("NTLM challenge: packet is too small:" + pktLen);

        in.skip(8);  //header "NTLMSSP\0"
        int seq = in.readInt(); //sequence number (2)
        if (seq != 2)
            throw new ProtocolException("NTLM challenge: got unexpected sequence number:" + seq);
        in.skip(4); //domain length (repeated 2x)
        in.skip(4); //domain offset
        in.skip(4); //flags
        currentToken.nonce = new byte[8];
        in.read(currentToken.nonce);
        in.skip(8); //?? unknown

        //skip the end, which may include the domain name, among other things...
        in.skip(pktLen - hdrLen);
    }

    /**
     * Process a TDS 5.0 result set packet.
     *
     * @throws IOException
     * @throws ProtocolException
     */
    private void tds5ResultToken()
        throws IOException, ProtocolException
    {
        int pktLen = in.readShort(); // Packet length
        int colCnt = in.readShort();
        this.columns = new ColInfo[colCnt];
        this.rowData = new ColData[colCnt];
        this.tables = null;

        for (int colNum = 0; colNum < colCnt; ++colNum) {
            //
            // Get the column name
            //
            ColInfo col = new ColInfo();
            int colNameLen = in.read();
            col.name  = in.readString(colNameLen);
            col.label = col.name;
            int column_flags = in.read();   /*  Flags */
            col.isCaseSensitive = false;
            col.nullable    = ((column_flags & 0x20) != 0)?
                                   ResultSetMetaData.columnNullable:
                                        ResultSetMetaData.columnNoNulls;
            col.isWriteable = (column_flags & 0x10) != 0;
            col.isIdentity  = (column_flags & 0x40) != 0;
            col.isKey       = (column_flags & 0x02) != 0;
            col.isHidden    = (column_flags & 0x01) != 0;

            col.userType    = in.readInt();
            TdsData.readType(in, col);
            // Skip locale information
            in.skip(1);
            columns[colNum] = col;
        }
        endOfResults = false;
    }

    /**
     * Process a DONE, DONEINPROC or DONEPROC token.
     *
     * @throws IOException
     */
    private void tdsDoneToken()
        throws IOException
    {
        currentToken.status = (byte)in.read();
        in.skip(1);
        currentToken.operation = (byte)in.read();
        in.skip(1);
        currentToken.updateCount = in.readInt();
        if (!endOfResults) {
            // This will eliminate the select row count for sybase
            currentToken.status &= ~DONE_ROW_COUNT;
            endOfResults = true;
        }

        if ((currentToken.status & DONE_MORE_RESULTS) == 0) {
            endOfResponse = true;
        }

        if (serverType == Driver.SQLSERVER) {
            //
            // MS SQL Server provides additional information we
            // can use to return special row counts for DDL etc.
            //
            switch (currentToken.operation) {
                //
                // These next four entries provide backwards
                // compatibility with previous versions of jTDS
                //
                case (byte)0xC6: // CREATE
                case (byte)0xC7: // DROP TABLE
                case (byte)0xD8: // ALTER TABLE
                case (byte)0xDF: // DROP PROC
                    currentToken.status |= DONE_ROW_COUNT;
                    break;
                //
                // Ignore row counts returned by SELECTs
                //
                case (byte)0XC1: // SELECT
                    currentToken.status &= ~DONE_ROW_COUNT;
                    break;
            }
        }

        if ((currentToken.status & 0X20) != 0) {
            // Indicates cancel packet
            messages.addDiagnostic(9999, 0, 14,
                                    "Request cancelled", "", "", 0);
        }
    }

    /**
     * Execute SQL using TDS 4.2 protocol.
     * 
     * @param sql The SQL statement to execute.
     * @param procName Stored procedure to execute or null.
     * @param parameters Parameters for call or null.
     * @param noMetaData Suppress meta data for cursor calls.
     * @throws SQLException
     */
    private void executeSQL42(String sql,
                              String procName,
                              ParamInfo[] parameters,
                              boolean noMetaData)
        throws IOException, SQLException {
        if (procName != null && procName.length() > 0) {
            // RPC call
            out.setPacketType(RPC_PKT);
            byte[] buf = Support.encodeString(connection.getCharSet(), procName);

            out.write((byte) buf.length);
            out.write(buf);
            out.write((short) (noMetaData ? 512 : 0));

            for (int i = nextParam + 1; i < parameters.length; i++) {
                if (parameters[i].name != null) {
                   buf = Support.encodeString(connection.getCharSet(),
                           parameters[i].name);
                   out.write((byte) buf.length);
                   out.write(buf);
                } else {
                   out.write((byte) 0);
                }

                out.write((byte) (parameters[i].isOutput ? 1 : 0));
                TdsData.writeParam(out,
                                   connection.getCharSet(),
                                   connection.isWideChar(),
                                   null,
                                   parameters[i]);
            }

            out.flush();
        } else if (sql.length() > 0) {
            if (parameters != null && parameters.length > 0) {
                sql = Support.substituteParameters(sql, parameters);
            }

            out.setPacketType(QUERY_PKT);
            out.write(sql);
            out.flush();
        }
    }

    /**
     * Execute SQL using TDS 5.0 protocol.
     *
     * @param sql The SQL statement to execute.
     * @param procName Stored procedure to execute or null.
     * @param parameters Parameters for call or null.
     * @param noMetaData Suppress meta data for cursor calls.
     * @throws SQLException
     */
    private void executeSQL50(String sql,
                              String procName,
                              ParamInfo[] parameters,
                              boolean noMetaData)
        throws IOException, SQLException {
        boolean haveParams = parameters != null && parameters.length > 0;
        boolean useParamNames = false;

        // Sybase does not allow text or image parameters
        for (int i = 0; haveParams && i < parameters.length; i++) {
            if (parameters[i].sqlType.equals("text")
                || parameters[i].sqlType.equals("image")) {
                if (procName != null && procName.length() > 0) {
                    // Call to store proc nothing we can do
                    if (parameters[i].sqlType.equals("text")) {
                        throw new SQLException(
                                        Support.getMessage("error.chartoolong"), "HY000");
                    }

                    throw new SQLException(
                                     Support.getMessage("error.bintoolong"), "HY000");
                }

                // prepared statement substitute parameters into SQL
                sql = Support.substituteParameters(sql, parameters);
                haveParams = false;
                procName = null;

                break;
            }
        }

        out.setPacketType(SYBQUERY_PKT);

        if (procName == null || procName.length() == 0) {
            // Use TDS_LANGUAGE TOKEN with optional parameters
            out.write((byte)TDS_LANG_TOKEN);

            if (haveParams) {
                sql = Support.substituteParamMarkers(sql, parameters);
            }

            if (socket.isWideChars()) {
                // Need to preconvert string to get correct length
                byte[] buf = Support.encodeString(connection.getCharSet(), sql);

                out.write((int) buf.length + 1);
                out.write((byte)(haveParams ? 1 : 0));
                out.write(buf);
            } else {
                out.write((int) sql.length() + 1);
                out.write((byte) (haveParams ? 1 : 0));
                out.write(sql);
            }
        } else if (procName.startsWith("#jtds")) {
            // Dynamic light weight procedure call
            out.write((byte) TDS5_DYNAMIC_TOKEN);
            out.write((short) (procName.length() + 4));
            out.write((byte) 2);
            out.write((byte) (haveParams ? 1 : 0));
            out.write((byte) (procName.length() - 1));
            out.write(procName.substring(1));
            out.write((short) 0);
        } else {
            byte buf[] = Support.encodeString(socket.getCharset(), procName);

            // RPC call
            out.write((byte) TDS_DBRPC_TOKEN);
            out.write((short) (buf.length + 3));
            out.write((byte) buf.length);
            out.write(buf);
            out.write((short) (haveParams ? 2 : 0));
            useParamNames = true;
        }

        //
        // Output any parameters
        //
        if (haveParams) {
            // First write parameter descriptors
            out.write((byte) TDS5_PARAMFMT_TOKEN);

            int len = 2;

            for (int i = nextParam + 1; i < parameters.length; i++) {
                len += TdsData.getTds5ParamSize(connection.getCharSet(),
                                                connection.isWideChar(),
                                                parameters[i],
                                                useParamNames);
            }

            out.write((short) len);
            out.write((short) ((nextParam < 0) ? parameters.length : parameters.length - 1));

            for (int i = nextParam + 1; i < parameters.length; i++) {
                TdsData.writeTds5ParamFmt(out,
                                          connection.getCharSet(),
                                          connection.isWideChar(),
                                          parameters[i],
                                          useParamNames);
            }

            // Now write the actual data
            out.write((byte) TDS5_PARAMS_TOKEN);

            for (int i = nextParam + 1; i < parameters.length; i++) {
                TdsData.writeTds5Param(out,
                                       connection.getCharSet(),
                                       connection.isWideChar(),
                                       parameters[i]);
            }
        }

        out.flush();
    }

    /**
     * Map of system stored procedures that have shortcuts in TDS8.
     */
    private static HashMap tds8SpNames = new HashMap();
    static {
        tds8SpNames.put("sp_cursor",            new Integer(1));
        tds8SpNames.put("sp_cursoropen",        new Integer(2));
        tds8SpNames.put("sp_cursorprepare",     new Integer(3));
        tds8SpNames.put("sp_cursorexecute",     new Integer(4));
        tds8SpNames.put("sp_cursorprepexec",    new Integer(5));
        tds8SpNames.put("sp_cursorunprepare",   new Integer(6));
        tds8SpNames.put("sp_cursorfetch",       new Integer(7));
        tds8SpNames.put("sp_cursoroption",      new Integer(8));
        tds8SpNames.put("sp_cursorclose",       new Integer(9));
        tds8SpNames.put("sp_executesql",        new Integer(10));
        tds8SpNames.put("sp_prepare",           new Integer(11));
//      tds8SpNames.put("sp_execute",           new Integer(12)); broken!
        tds8SpNames.put("sp_prepexec",          new Integer(13));
        tds8SpNames.put("sp_prepexecrpc",       new Integer(14));
        tds8SpNames.put("sp_unprepare",         new Integer(15));
    }

    /**
     * Execute SQL using TDS 7.0 protocol.
     *
     * @param sql The SQL statement to execute.
     * @param procName Stored procedure to execute or null.
     * @param parameters Parameters for call or null.
     * @param noMetaData Suppress meta data for cursor calls.
     * @throws SQLException
     */
    private void executeSQL70(String sql,
                              String procName,
                              ParamInfo[] parameters,
                              boolean noMetaData)
        throws IOException, SQLException {
        if (procName == null && parameters != null && parameters.length > 0) {
            ParamInfo params[] = new ParamInfo[parameters.length + 2];

            // Use sp_executesql approach
            procName = "sp_executesql";

            System.arraycopy(parameters, 0, params, 2, parameters.length);

            params[0] = new ParamInfo();
            params[0].jdbcType = java.sql.Types.VARCHAR;
            params[0].bufferSize = 4000;
            params[0].isSet = true;
            params[0].isUnicode = true;
            params[0].value = Support.substituteParamMarkers(sql, parameters);

            TdsData.getNativeType(connection, params[0]);
            StringBuffer paramDef = new StringBuffer(80);

            for (int i = 0; i < parameters.length; i++) {
                paramDef.append("@P");
                paramDef.append(i);
                paramDef.append(' ');
                paramDef.append(parameters[i].sqlType);

                if (i + 1 < parameters.length) {
                    paramDef.append(',');
                }
            }

            params[1] = new ParamInfo();
            params[1].jdbcType = java.sql.Types.VARCHAR;
            params[1].bufferSize = 4000;
            params[1].isSet = true;
            params[1].isUnicode = true;
            params[1].value = paramDef.toString();

            TdsData.getNativeType(connection, params[1]);

            parameters = params;
        }

        if (procName != null && procName.length() > 0) {
            // RPC call
            out.setPacketType(RPC_PKT);
            Integer shortcut;

            if (tdsVersion == Driver.TDS80
            	&& (shortcut = (Integer) tds8SpNames.get(procName)) != null) {
                // Use the shortcut form of procedure name for TDS8
                out.write((short) -1);
                out.write((short) shortcut.shortValue());
            } else {
                out.write((short) procName.length());
                out.write(procName);
            }

            out.write((short) (noMetaData ? 512 : 0));

            for (int i = nextParam + 1; i < parameters.length; i++) {
                if (parameters[i].name != null) {
                   out.write((byte) parameters[i].name.length());
                   out.write(parameters[i].name);
                } else {
                   out.write((byte) 0);
                }

                out.write((byte) (parameters[i].isOutput ? 1 : 0));

                TdsData.writeParam(out,
                                   connection.getCharSet(),
                                   connection.isWideChar(),
                                   connection.getCollation(),
                                   parameters[i]);
            }

            out.flush();
        } else if (sql.length() > 0) {
            // Simple query
            out.setPacketType(QUERY_PKT);
            out.write(sql);
            out.flush();
        }
    }

    /**
     * Set the server row count used to limit the number of rows in a result set.
     *
     * @param rowCount The number of rows to return or 0 for no limit.
     * @throws SQLException
     */
    private void setRowCount(int rowCount) throws SQLException {
        if (rowCount >= 0 && rowCount != connection.getRowCount()) {
            try {
                out.setPacketType(QUERY_PKT);
                out.write("SET ROWCOUNT " + rowCount);
                out.flush();
                endOfResponse = false;
                endOfResults  = true;
                wait(0);
                clearResponseQueue();
                messages.checkErrors();
                connection.setRowCount(rowCount);
            } catch (IOException ioe) {
                throw new SQLException(
                            Support.getMessage("error.generic.ioerror",
                                                    ioe.getMessage()), "08S01");
            }
        }
    }

    /**
     * Wait for the first byte of the server response.
     *
     * @param timeOut The time out period in seconds or 0.
     * @throws IOException
     */
    private void wait(int timeOut) throws IOException, SQLException {
        try {
            in.setTimeout(timeOut * 1000);
            in.peek();
        } catch (java.io.InterruptedIOException e) {
            // Query timed out
            endOfResponse = true;
            throw new SQLException(
                    Support.getMessage("error.generic.timeout"), "HYT00");
        } finally {
            in.setTimeout(0);
        }
    }

    /**
     * Convert a user supplied MAC address into a byte array.
     *
     * @param macString The MAC address as a hex string.
     * @return The MAC address as a <code>byte[]</code>.
     */
    private static byte[] getMACAddress(String macString) {
        byte[] mac = new byte[6];
        boolean ok = false;

        if (macString != null && macString.length() == 12) {
            try {
                for (int i = 0, j = 0; i < 6; i++, j += 2) {
                    mac[i] = (byte) Integer.parseInt(
                            macString.substring(j, j + 2), 16);
                }

                ok = true;
            } catch (Exception ex) {
                // Ignore it. ok will be false.
            }
        }

        if (!ok) {
            Arrays.fill(mac, (byte) 0);
        }

        return mac;
    }

    /**
     * Try to figure out what client name we should identify ourselves as. Get
     * the hostname of this machine,
     *
     * @return    name we will use as the client.
     */
    private static String getHostName() {
        String name;

        try {
            name = java.net.InetAddress.getLocalHost().getHostName().toUpperCase();
        } catch (java.net.UnknownHostException e) {
            return "UNKNOWN";
        }

        int pos = name.indexOf('.');

        if (pos >= 0) {
            name = name.substring(0, pos);
        }

        if (name.length() == 0) {
            return "UNKNOWN";
        }

        try {
            Integer.parseInt(name);
            // All numbers probably an IP address
            return "UNKNOWN";
        } catch (NumberFormatException e) {
            // Bit tacky but simple check for all numbers
        }

        return name;
    }

    /**
     * This is a <B>very</B> poor man's "encryption."
     *
     * @param  pw  password to encrypt
     * @return     encrypted password
     */
    private static String tds7CryptPass(final String pw) {
        final int xormask = 0x5A5A;
        final int len = pw.length();
        final char[] chars = new char[len];

        for (int i = 0; i < len; ++i) {
            final int c = (int) (pw.charAt(i)) ^ xormask;
            final int m1 = (c >> 4) & 0x0F0F;
            final int m2 = (c << 4) & 0xF0F0;

            chars[i] = (char) (m1 | m2);
        }

        return new String(chars);
    }
}
