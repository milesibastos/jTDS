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

import java.io.*;
import java.sql.*;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;

import net.sourceforge.jtds.ssl.*;
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
 * @version $Id: TdsCore.java,v 1.68 2005-02-09 09:57:36 alin_sinpalean Exp $
 */
public class TdsCore {
    /**
     * Inner static class used to hold information about TDS tokens read.
     */
    private static class TdsToken {
        /** The current TDS token byte. */
        byte token;
        /** The status field from a DONE packet. */
        byte status;
        /** The operation field from a DONE packet. */
        byte operation;
        /** The update count from a DONE packet. */
        int updateCount;
        /** The nonce from an NTLM challenge packet. */
        byte[] nonce;
        /** NTLM authentication message. */
        byte[] ntlmMessage;
        /** The dynamicID from the last TDS_DYNAMIC token. */
        String dynamicId;
        /** The TDS token that preceded the dynamic parameter data. */
        int previousToken;
        /** The dynamic parameters from the last TDS_DYNAMIC token. */
        ColInfo[] dynamParamInfo;
        /** The dynamic parameter data from the last TDS_DYNAMIC token. */
        Object[] dynamParamData;

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
                   || token == TDS5_WIDE_RESULT
                   || token == TDS_COLINFO_TOKEN
                   || token == TDS_ROW_TOKEN;
        }

        /**
         * Retrieve the row data status.
         *
         * @return <code>boolean</code> true if the current token is a result row.
         */
        public boolean isRowData() {
            return token == TDS_ROW_TOKEN;
        }

    }
    /**
     * Inner static class used to hold table meta data.
     */
    private static class TableMetaData {
        /** Table catalog (database) name. */
        String catalog;
        /** Table schema (user) name. */
        String schema;
        /** Table name. */
        String name;
    }

    //
    // Package private constants
    //
    /** Minimum network packet size. */
    public static final int MIN_PKT_SIZE = 512;
    /** Default minimum network packet size for TDS 7.0 and newer. */
    public static final int DEFAULT_MIN_PKT_SIZE_TDS70 = 4096;
    /** Maximum network packet size. */
    public static final int MAX_PKT_SIZE = 32768;
    /** The size of the packet header. */
    public static final int PKT_HDR_LEN = 8;
    /** TDS 4.2 or 7.0 Query packet. */
    public static final byte QUERY_PKT = 1;
    /** TDS 4.2 or 5.0 Login packet. */
    public static final byte LOGIN_PKT = 2;
    /** TDS Remote Procedure Call. */
    public static final byte RPC_PKT = 3;
    /** TDS Reply packet. */
    public static final byte REPLY_PKT = 4;
    /** TDS Cancel packet. */
    public static final byte CANCEL_PKT = 6;
    /** TDS MSDTC packet. */
    public static final byte MSDTC_PKT = 14;
    /** TDS 5.0 Query packet. */
    public static final byte SYBQUERY_PKT = 15;
    /** TDS 7.0 Login packet. */
    public static final byte MSLOGIN_PKT = 16;
    /** TDS 7.0 NTLM Authentication packet. */
    public static final byte NTLMAUTH_PKT = 17;
    /** SQL 2000 prelogin negotiation packet. */
    public static final byte PRELOGIN_PKT = 18;
    /** SSL Mode - Login packet must be encrypted. */
    public static final int SSL_ENCRYPT_LOGIN = 0;
    /** SSL Mode - Client requested force encryption. */
    public static final int SSL_CLIENT_FORCE_ENCRYPT = 1;
    /** SSL Mode - No server certificate installed. */
    public static final int SSL_NO_ENCRYPT = 2;
    /** SSL Mode - Server requested force encryption. */
    public static final int SSL_SERVER_FORCE_ENCRYPT = 3;

    //
    // Sub packet types
    //
    /** TDS 5.0 Parameter format token. */
    private static final byte TDS5_PARAMFMT2_TOKEN  = (byte) 32;   // 0x20
    /** TDS 5.0 Language token. */
    private static final byte TDS_LANG_TOKEN        = (byte) 33;   // 0x21
    /** TSD 5.0 Wide result set token. */
    private static final byte TDS5_WIDE_RESULT      = (byte) 97;   // 0x61
    /** TDS 5.0 Close token. */
    private static final byte TDS_CLOSE_TOKEN       = (byte) 113;  // 0x71
    /** TDS DBLIB Offsets token. */
    private static final byte TDS_OFFSETS_TOKEN     = (byte) 120;  // 0x78
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
    // End token status bytes
    //
    /** Done: more results are expected. */
    private static final byte DONE_MORE_RESULTS     = (byte) 0x01;
    /** Done: command caused an error. */
    private static final byte DONE_ERROR            = (byte) 0x02;
    /** Done: There is a valid row count. */
    private static final byte DONE_ROW_COUNT        = (byte) 0x10;
    /** Done: Cancel acknowledgement. */
    static final byte DONE_CANCEL                   = (byte) 0x20;
    /**
     * Done: Response terminator (if more than one request packet is sent, each
     * response is terminated by a DONE packet with this flag set).
     */
    private static final byte DONE_END_OF_RESPONSE  = (byte) 0x80;

    //
    // Prepared SQL types
    //
    /** Do not prepare SQL */
    public static final int UNPREPARED = 0;
    /** Prepare SQL using temporary stored procedures */
    public static final int TEMPORARY_STORED_PROCEDURES = 1;
    /** Prepare SQL using sp_executesql */
    public static final int EXECUTE_SQL = 2;
    /** Prepare SQL using sp_prepare and sp_execute */
    public static final int PREPARE = 3;
    /** Prepare SQL using sp_prepexec and sp_execute */
    public static final int PREPEXEC = 4;

    //
    // Sybase capability flags
    //
    /** Sybase char and binary > 255.*/
    static final int SYB_LONGDATA    = 1;
    /** Sybase date and time data types.*/
    static final int SYB_DATETIME    = 2;
    /** Sybase nullable bit type.*/
    static final int SYB_BITNULL     = 4;
    /** Sybase extended column meta data.*/
    static final int SYB_EXTCOLINFO  = 8;
    /** Sybase univarchar etc. */
    static final int SYB_UNICODE     = 16;

    //
    // Class variables
    //
    /** Name of the client host (it can take quite a while to find it out if DNS is configured incorrectly). */
    private static String hostName = null;

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
    private Object[] rowData;
    /** The array of table names associated with this result. */
    private TableMetaData[] tables;
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
    private boolean isClosed;
    /** Indicates reading results from READTEXT command. */
    private boolean readTextMode;
    /** A reference to ntlm.SSPIJNIClient. */
    private SSPIJNIClient sspiJNIClient;
    /** Flag that indicates if logon() should try to use Windows Single Sign On using SSPI. */
    private boolean ntlmAuthSSO;
    /** Indicates that a fatal error has occured and the connection will close. */
    private boolean fatalError;
    /** Mutual exclusion lock on connection. */
    private Semaphore connectionLock;
    /** Indicates processing a batch. */
    private boolean inBatch;
    /** Indicates type of SSL connection. */
    private int sslMode = SSL_NO_ENCRYPT;

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
        if (connection.isClosed()) {
            throw new SQLException(
                Messages.get("error.generic.closed", "Connection"),
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
                params[i] = new ParamInfo(ci, ci.realName, null, 0);
            }

            return params;
        }

        return EMPTY_PARAMETER_INFO;
    }

    /**
     * Retrieve the current result set data items.
     *
     * @return the row data as an <code>Object</code> array
     */
    Object[] getRowData() {
        return rowData;
    }

    /**
     * Negotiate SSL settings with SQL 2000+ server.
     * <p/>
     * Server returns the following values for SSL mode:
     * <ol>
     * <ll>0 = Certificate installed encrypt login packet only.
     * <li>1 = Certificate installed client requests force encryption.
     * <li>2 = No certificate no encryption possible.
     * <li>3 = Server requests force encryption.
     * </ol>
     * @param instance The server instance name.
     * @param ssl The SSL URL property value.
     * @throws IOException
     */
    void negotiateSSL(String instance, String ssl)
            throws IOException, SQLException {
        if (!ssl.equalsIgnoreCase(Ssl.SSL_OFF)) {
            if (ssl.equalsIgnoreCase(Ssl.SSL_REQUIRE) ||
                    ssl.equalsIgnoreCase(Ssl.SSL_AUTHENTICATE)) {
                sendPreLoginPacket(instance, true);
                sslMode = readPreLoginPacket();
                if (sslMode != SSL_CLIENT_FORCE_ENCRYPT &&
                    sslMode != SSL_SERVER_FORCE_ENCRYPT) {
                    throw new SQLException(
                            Messages.get("error.ssl.encryptionoff"),
                            "08S01");
                }
            } else {
                sendPreLoginPacket(instance, false);
                sslMode = readPreLoginPacket();
            }
            if (sslMode != SSL_NO_ENCRYPT) {
                socket.enableEncryption(ssl);
            }
        }
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
     * @param progName The program name.
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
               final String progName,
               final String language,
               final String macAddress,
               final int packetSize)
        throws SQLException {
        try {
            if (tdsVersion >= Driver.TDS70) {
                sendMSLoginPkt(serverName, database, user, password,
                                domain, appName, progName, language,
                                macAddress, packetSize);
            } else if (tdsVersion == Driver.TDS50) {
                send50LoginPkt(serverName, user, password,
                                charset, appName, progName,
                                language, packetSize);
            } else {
                send42LoginPkt(serverName, user, password,
                                charset, appName, progName,
                                language, packetSize);
            }
            if (sslMode == SSL_ENCRYPT_LOGIN) {
                socket.disableEncryption();
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
                       Messages.get(
                                "error.generic.ioerror", ioe.getMessage()),
                                    "08S01"), ioe);
        }
    }

    /**
     * Get the next result set or update count from the TDS stream.
     *
     * @return <code>boolean</code> if the next item is a result set.
     * @throws SQLException
     */
    boolean getMoreResults() throws SQLException {
        checkOpen();
        nextToken();

        while (!endOfResponse
               && !currentToken.isUpdateCount()
               && !currentToken.isResultSet()) {
            nextToken();
        }
        messages.checkErrors();

        //
        // Cursor opens are followed by TDS_TAB_INFO and TDS_COL_INFO
        // Process these now so that the column descriptors are updated.
        // Sybase wide result set headers are followed by a TDS_CONTROL_TOKEN
        // skip that as well.
        //
        if (currentToken.isResultSet()) {
            byte saveToken = currentToken.token;
            try {
                byte x = (byte) in.peek();

                while (   x == TDS_TABNAME_TOKEN
                       || x == TDS_COLINFO_TOKEN
                       || x == TDS_CONTROL_TOKEN) {
                    nextToken();
                    x = (byte)in.peek();
                }
            } catch (IOException e) {
                connection.setClosed();

                throw Support.linkException(
                    new SQLException(
                           Messages.get(
                                "error.generic.ioerror", e.getMessage()),
                                    "08S01"), e);
            }
            currentToken.token = saveToken;
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
     * @return <code>boolean</code> true if the next item is row data.
     */
    boolean isRowData() {
        return currentToken.isRowData();
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
     * @return <code>boolean</code> true if the response has been entirely consumed
     */
    boolean isEndOfResponse() {
        return endOfResponse;
    }

    /**
     * Empty the server response queue.
     *
     * @throws SQLException if an error occurs
     */
    void clearResponseQueue() throws SQLException {
        checkOpen();
        while (!endOfResponse) {
            nextToken();
        }
    }

    /**
     * Consume packets from the server response queue up to (and including) the
     * first response terminator.
     *
     * @throws SQLException if an error occurs
     */
    void consumeOneResponse() throws SQLException {
        checkOpen();
        while (!endOfResponse) {
            nextToken();
            // If it's a response terminator, return
            if (currentToken.isEndToken()
                    && (currentToken.status & DONE_END_OF_RESPONSE) != 0) {
                return;
            }
        }
        messages.checkErrors();
    }

    /**
     * Retrieve the next data row from the result set.
     *
     * @return <code>boolean</code> - <code>false</code> if at end of results.
     */
    boolean getNextRow() throws SQLException {
        if (endOfResponse || endOfResults) {
            return false;
        }
        checkOpen();
        nextToken();

        // Will either be first or next data row or end.
        while (!currentToken.isRowData() && !currentToken.isEndToken()) {
            nextToken(); // Could be messages
        }

        messages.checkErrors();

        return currentToken.isRowData();
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
            connection.setClosed();
            throw Support.linkException(
                new SQLException(
                       Messages.get(
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
     * Used by Sybase a no-op for Microsoft.
     */
    synchronized void closeConnection() {
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
        }
    }

    /**
     * Close the TDSCore connection object and associated streams.
     *
     * @throws SQLException
     */
    void close() throws SQLException {
       if (!isClosed) {
           try {
               clearResponseQueue();
               out.close();
               in.close();
           } finally {
               isClosed = true;
           }
        }
    }

    /**
     * Send a cancel packet to the server.
     */
    void cancel() {
        Semaphore mutex = null;
        try {
            mutex = connection.getMutex();
            mutex.acquire();
            socket.cancel(out.getStreamId());
        } catch (InterruptedException e) {
            mutex = null;
        } finally {
            if (mutex != null) {
                mutex.release();
            }
        }
    }

    /**
     * Submit a simple SQL statement to the server and process all output.
     *
     * @param sql the statement to execute
     * @throws SQLException if an error is returned by the server
     */
    void submitSQL(String sql) throws SQLException {
        checkOpen();

        if (sql.length() == 0) {
            throw new IllegalArgumentException("submitSQL() called with empty SQL String");
        }

        executeSQL(sql, null, null, false, 0, -1, -1, true);
        clearResponseQueue();
        messages.checkErrors();
    }

    /**
     * Notifies the <code>TdsCore</code> that a batch is starting. This is so
     * that it knows to use <code>sp_executesql</code> for parameterized
     * queries (because there's no way to prepare a statement in the middle of
     * a batch).
     * <p>
     * Sets the {@link #inBatch} flag.
     */
    void startBatch() {
        inBatch = true;
    }

    /**
     * Send an SQL statement with optional parameters to the server.
     *
     * @param sql          SQL statement to execute
     * @param procName     stored procedure to execute or <code>null</code>
     * @param parameters   parameters for call or null
     * @param noMetaData   suppress meta data for cursor calls
     * @param timeOut      optional query timeout or 0
     * @param maxRows      the maximum number of data rows to return (-1 to
     *                     leave unaltered)
     * @param maxFieldSize the maximum number of bytes in a column to return
     *                     (-1 to leave unaltered)
     * @param sendNow      whether to send the request now or not
     * @throws SQLException if an error occurs
     */
    synchronized void executeSQL(String sql,
                                 String procName,
                                 ParamInfo[] parameters,
                                 boolean noMetaData,
                                 int timeOut,
                                 int maxRows,
                                 int maxFieldSize,
                                 boolean sendNow)
            throws SQLException {
        boolean sendFailed = true; // Used to ensure mutex is released.

        try {
            //
            // Obtain a lock on the connection giving exclusive access
            // to the network connection for this thread
            //
            if (connectionLock == null) {
                connectionLock = connection.getMutex();
                try {
                    connectionLock.acquire();
                } catch (InterruptedException e) {
                    connectionLock = null;
                    throw new IllegalStateException("Connection synchronization failed");
                }
            }
            checkOpen();
            clearResponseQueue();
            messages.exceptions = null;

            //
            // Set the connection row count and text size if required.
            // Once set these will not be changed within a
            // batch so execution of the set rows query will
            // only occur once a the start of a batch.
            // No other thread can send until this one has finished.
            //
            setRowCountAndTextSize(maxRows, maxFieldSize);

            messages.clearWarnings();
            this.returnStatus = null;
            //
            // Normalize the parameters argument to simplify later checks
            //
            if (parameters != null && parameters.length == 0) {
                parameters = null;
            }
            this.parameters = parameters;
            //
            // Normalise the procName argument as well
            //
            if (procName != null && procName.length() == 0) {
                procName = null;
            }

            if (parameters != null && parameters[0].isRetVal) {
                returnParam = parameters[0];
                nextParam = 0;
            } else {
                returnParam = null;
                nextParam = -1;
            }

            if (parameters != null) {
                if (procName == null && sql.startsWith("EXECUTE ")) {
                    //
                    // If this is a callable statement that could not be fully parsed
                    // into an RPC call convert to straight SQL now.
                    // An example of non RPC capable SQL is {?=call sp_example('literal', ?)}
                    //
                    for (int i = 0; i < parameters.length; i++){
                        // Output parameters not allowed.
                        if (!parameters[i].isRetVal && parameters[i].isOutput){
                            throw new SQLException(Messages.get("error.prepare.nooutparam",
                                    Integer.toString(i + 1)), "07000");
                        }
                    }
                    sql = Support.substituteParameters(sql, parameters, getTdsVersion());
                    parameters = null;
                } else {
                    //
                    // Check all parameters are either output or have values set
                    //
                    for (int i = 0; i < parameters.length; i++){
                        if (!parameters[i].isSet && !parameters[i].isOutput){
                            throw new SQLException(Messages.get("error.prepare.paramnotset",
                                    Integer.toString(i + 1)), "07000");
                        }
                        parameters[i].clearOutValue();
                        TdsData.getNativeType(connection, parameters[i]);
                    }
                }
            }

            try {
                switch (tdsVersion) {
                    case Driver.TDS42:
                        executeSQL42(sql, procName, parameters, noMetaData, sendNow);
                        break;
                    case Driver.TDS50:
                        executeSQL50(sql, procName, parameters);
                        break;
                    case Driver.TDS70:
                    case Driver.TDS80:
                    case Driver.TDS81:
                        executeSQL70(sql, procName, parameters, noMetaData, sendNow);
                        break;
                    default:
                        throw new IllegalStateException("Unknown TDS version " + tdsVersion);
                }

                if (sendNow) {
                    out.flush();
                    connectionLock.release();
                    connectionLock = null;
                    sendFailed = false;
                    endOfResponse = false;
                    endOfResults  = true;
                    wait(timeOut);
                } else {
                    sendFailed = false;
                }
            } catch (IOException ioe) {
                connection.setClosed();

                throw Support.linkException(
                    new SQLException(
                           Messages.get(
                                    "error.generic.ioerror", ioe.getMessage()),
                                        "08S01"), ioe);
            }
        } finally {
            if ((sendNow || sendFailed) && connectionLock != null) {
                connectionLock.release();
                connectionLock = null;
            }
            // Clear the in batch flag
            if (sendNow) {
                inBatch = false;
            }
        }
    }

    /**
     * Prepares the SQL for use with Microsoft server.
     *
     * @param sql                  the SQL statement to prepare.
     * @param params               the actual parameter list
     * @param resultSetType        value of the resultSetType parameter when
     *                             the Statement was created
     * @param resultSetConcurrency value of the resultSetConcurrency parameter
     *                             whenthe Statement was created
     * @return name of the procedure or prepared statement handle.
     * @exception SQLException
     */
    String microsoftPrepare(String sql, ParamInfo[] params,
                            int resultSetType, int resultSetConcurrency)
            throws SQLException {
        int prepareSql = connection.getPrepareSql();

        if (prepareSql == TEMPORARY_STORED_PROCEDURES) {
            StringBuffer spSql = new StringBuffer(sql.length() + 32 + params.length * 15);
            String procName = connection.getProcName();

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

            submitSQL(spSql.toString());

            return procName;
        } else if (prepareSql == PREPARE) {
            boolean needCursor = resultSetType != ResultSet.TYPE_FORWARD_ONLY
                    || resultSetConcurrency != ResultSet.CONCUR_READ_ONLY;
            int scrollOpt, ccOpt;

            ParamInfo prepParam[] = new ParamInfo[needCursor ? 6 : 4];

            // Setup prepare handle param
            prepParam[0] = new ParamInfo(Types.INTEGER, null, ParamInfo.OUTPUT);

            // Setup parameter descriptor param
            prepParam[1] = new ParamInfo(Types.LONGVARCHAR,
                    Support.getParameterDefinitions(params),
                    ParamInfo.UNICODE);

            // Setup sql statemement param
            prepParam[2] = new ParamInfo(Types.LONGVARCHAR,
                    Support.substituteParamMarkers(sql, params),
                    ParamInfo.UNICODE);

            // Setup options param
            prepParam[3] = new ParamInfo(Types.INTEGER, new Integer(1), ParamInfo.INPUT);

            if (needCursor) {
                // Select the correct type of Server side cursor to
                // match the scroll and concurrency options.
                scrollOpt = MSCursorResultSet.getCursorScrollOpt(resultSetType, true);
                ccOpt = MSCursorResultSet.getCursorConcurrencyOpt(resultSetConcurrency);

                // Setup scroll options parameter
                prepParam[4] = new ParamInfo(Types.INTEGER,
                        new Integer(scrollOpt),
                        ParamInfo.OUTPUT);

                // Setup concurrency options parameter
                prepParam[5] = new ParamInfo(Types.INTEGER,
                        new Integer(ccOpt),
                        ParamInfo.OUTPUT);

                // TODO Implement support for preparing cursors (including SP support)
                // Don't forget to include scrollability and concurrency into
                // the prepared statement key and to remove the
                // sp_cursorunprepare call from MSCursorResultSet (in the
                // current implementation this would unprepare the statement
                // when the first result set is closed, but leave the statement
                // in the cache, causing problems on reuse).
                return null;
            }

            columns = null; // Will be populated if preparing a select

            executeSQL(null, needCursor ? "sp_cursorprepare" : "sp_prepare",
                    prepParam, false, 0, -1, -1, true);

            clearResponseQueue();

            // columns will now hold meta data for select statements
            Integer prepareHandle = (Integer) prepParam[0].getOutValue();

            if (prepareHandle != null) {
                return prepareHandle.toString();
            }
        }

        return null;
    }

    /**
     * Create a light weight stored procedure on a Sybase server.
     *
     * @param sql The SQL statement to prepare.
     * @param params The actual parameter list
     * @return name of the procedure.
     * @throws SQLException
     */
    synchronized String sybasePrepare(String sql, ParamInfo[] params)
        throws SQLException {
        checkOpen();
        if (sql == null || sql.length() == 0) {
            throw new IllegalArgumentException(
                    "sql parameter must be at least 1 character long.");
        }

        String procName = connection.getProcName();

        if (procName == null || procName.length() != 11) {
            throw new IllegalArgumentException(
                    "procName parameter must be 11 characters long.");
        }

        // TODO Check if output parameters are handled ok
        // Check no text/image parameters
        for (int i = 0; i < params.length; i++) {
            if (params[i].sqlType.equals("text")
                || params[i].sqlType.equals("image")) {
                return null; // Sadly no way
            }
        }

        try {
            out.setPacketType(SYBQUERY_PKT);
            out.write((byte)TDS5_DYNAMIC_TOKEN);

            byte buf[] = Support.encodeString(connection.getCharset(), sql);

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
            connection.setClosed();
            throw Support.linkException(
                new SQLException(
                       Messages.get(
                                "error.generic.ioerror", ioe.getMessage()),
                                    "08S01"), ioe);
        } catch (SQLException e) {
            if (e.getSQLState() != null && e.getSQLState().equals("08S01")) {
                // Serious error rethrow
                throw e;
            }

            // This exception probably caused by failure to prepare
            // Return null;
            return null;
        }

        return procName;
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
        checkOpen();
        if (colName == null || colName.length() == 0
            || tabName == null || tabName.length() == 0) {
            throw new SQLException(Messages.get("error.tdscore.badtext"), "HY000");
        }

        if (textPtr == null) {
            throw new SQLException(
                Messages.get("error.tdscore.notextptr", tabName + "." + colName),
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

        executeSQL(sql.toString(), null, null, false, 0, -1, -1, true);
        readTextMode = true;

        if (getMoreResults()) {
            if (getNextRow()) {
                // FIXME - this will not be valid since a Blob/Clob is returned instead of byte[]/String
                results = rowData[0];
            }
        }

        clearResponseQueue();
        messages.checkErrors();

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
        checkOpen();
        if (colName == null || colName.length() == 0
            || tabName == null || tabName.length() == 0) {
            throw new SQLException(Messages.get("error.tdscore.badtext"), "HY000");
        }

        Object results = null;
        StringBuffer sql = new StringBuffer(128);

        sql.append("select datalength(");
        sql.append(colName);
        sql.append(") from ");
        sql.append(tabName);
        executeSQL(sql.toString(), null, null, false, 0, -1, -1, true);

        if (getMoreResults()) {
            if (getNextRow()) {
                results = rowData[0];
            }
        }

        clearResponseQueue();
        messages.checkErrors();

        if (!(results instanceof Number)) {
            throw new SQLException(
                Messages.get("error.tdscore.badlen", tabName + "." + colName),
                "HY000");
        }

        return ((Number) results).intValue();
    }

    /**
     * Enlist the current connection in a distributed transaction or request the location of the
     * MSDTC instance controlling the server we are connected to.
     *
     * @param type      set to 0 to request TM address or 1 to enlist connection
     * @param oleTranID the 40 OLE transaction ID
     * @return a <code>byte[]</code> array containing the TM address data
     * @throws SQLException
     */
    synchronized byte[] enlistConnection(int type, byte[] oleTranID) throws SQLException {
        Semaphore mutex = null;
        try {
            try {
                mutex = connection.getMutex();
                mutex.acquire();
            } catch (InterruptedException e) {
                mutex = null;
                throw new IllegalStateException("Connection synchronization failed");
            }
            out.setPacketType(MSDTC_PKT);
            out.write((short)type);
            switch (type) {
                case 0: // Get result set with location of MSTDC
                    out.write((short)0);
                    break;
                case 1: // Set OLE transaction ID
                    if (oleTranID != null) {
                        out.write((short)oleTranID.length);
                        out.write(oleTranID);
                    } else {
                        // Delist the connection from all transactions.
                        out.write((short)0);
                    }
                    break;
            }
            out.flush();
            endOfResponse = false;
            endOfResults  = true;
        } catch (IOException ioe) {
            connection.setClosed();
            throw Support.linkException(
                    new SQLException(
                            Messages.get(
                                    "error.generic.ioerror", ioe.getMessage()),
                            "08S01"),
                    ioe);
        } finally {
            if (mutex != null) {
                mutex.release();
            }
        }

        byte[] tmAddress = null;
        if (getMoreResults() && getNextRow()) {
            if (rowData.length == 1) {
                Object x = rowData[0];
                if (x instanceof byte[]) {
                    tmAddress = (byte[])x;
                }
            }
        }

        clearResponseQueue();
        messages.checkErrors();
        return tmAddress;
    }

    /**
     * Obtain the counts from a batch of SQL updates.
     * <p>
     * If an error occurs Sybase will continue processing a batch whilst SQL
     * Server will stop. This method returns the JDBC3
     * <code>EXECUTE_FAILED</code> constant in the counts array when a
     * statement fails with an error.
     *
     * @return the update counts as an <code>int[]</code>
     */
    int[] getBatchCounts() throws BatchUpdateException {
        ArrayList counts = new ArrayList();
        Integer lastCount = JtdsStatement.SUCCESS_NO_INFO;
        BatchUpdateException batchEx = null;

        try {
            checkOpen();
            while (!endOfResponse) {
                nextToken();
                if (currentToken.isResultSet()) {
                    throw new SQLException(
                            Messages.get(
                                    "error.statement.batchnocount"),"07000");
                }
                //
                // Analyse type of end token and try to extract correct
                // update count when calling stored procs.
                //
                switch (currentToken.token) {
                    case TDS_DONE_TOKEN:
                        if ((currentToken.status & DONE_ERROR) != 0) {
                            counts.add(JtdsStatement.EXECUTE_FAILED);
                        } else {
                            if (currentToken.isUpdateCount()) {
                                counts.add(new Integer(currentToken.updateCount));
                            } else {
                                counts.add(lastCount);
                            }
                        }
                        lastCount = JtdsStatement.SUCCESS_NO_INFO;
                        break;
                    case TDS_DONEINPROC_TOKEN:
                        if ((currentToken.status & DONE_ERROR) != 0) {
                            lastCount = JtdsStatement.EXECUTE_FAILED;
                        } else
                        if (currentToken.isUpdateCount()) {
                            lastCount = new Integer(currentToken.updateCount);
                        }
                        break;
                    case TDS_DONEPROC_TOKEN:
                        if ((currentToken.status & DONE_ERROR) != 0) {
                            counts.add(JtdsStatement.EXECUTE_FAILED);
                        } else {
                            counts.add(lastCount);
                        }
                        lastCount = JtdsStatement.SUCCESS_NO_INFO;
                        break;
                }
            }
            messages.checkErrors();
        } catch (SQLException e) {
            // A server error has been reported
            // return a BatchUpdateException
            // Create partial response count array
            int results[] = new int[counts.size()];
            for (int i = 0; i < results.length; i++) {
                results[i] = ((Integer)counts.get(i)).intValue();
            }
            batchEx = new BatchUpdateException(e.getMessage(), e.getSQLState(),
                    e.getErrorCode(), results);
            throw batchEx;
        } finally {
            while (!endOfResponse) {
                // Flush rest of response
                try {
                    nextToken();
                } catch (SQLException ex) {
                    // Chain any exceptions to the BatchUpdateException
                    if (batchEx != null) {
                        batchEx.setNextException(ex);
                    }
                }
            }
        }
        //
        // Batch completed OK return full count array
        //
        int results[] = new int[counts.size()];
        for (int i = 0; i < results.length; i++) {
            results[i] = ((Integer)counts.get(i)).intValue();
        }
        return results;
    }

// ---------------------- Private Methods from here ---------------------

    /**
     * Write a TDS login packet string. Text followed by padding followed
     * by a byte sized length.
     */
    private void putLoginString(String txt, int len)
        throws IOException {
        byte[] tmp = Support.encodeString(connection.getCharset(), txt);
        out.write(tmp, 0, len);
        out.write((byte) (tmp.length < len ? tmp.length : len));
    }

    /**
     * Send the SQL Server 2000 pre login packet.
     * <p>Packet contains; netlib version, ssl mode, instance
     * and process ID.
     * @param instance
     * @param forceEncryption
     * @throws IOException
     */
    private void sendPreLoginPacket(String instance, boolean forceEncryption)
            throws IOException {
        out.setPacketType(PRELOGIN_PKT);
        // Write Netlib pointer
        out.write((short)0);
        out.write((short)21);
        out.write((byte)6);
        // Write Encrypt flag pointer
        out.write((short)1);
        out.write((short)27);
        out.write((byte)1);
        // Write Instance name pointer
        out.write((short)2);
        out.write((short)28);
        out.write((byte)(instance.length()+1));
        // Write process ID pointer
        out.write((short)3);
        out.write((short)(28+instance.length()+1));
        out.write((byte)4);
        // Write terminator
        out.write((byte)0xFF);
        // Write fake net lib ID 8.341.0
        out.write(new byte[]{0x08, 0x00, 0x01, 0x55, 0x00, 0x00});
        // Write force encryption flag
        out.write((byte)(forceEncryption? 1: 0));
        // Write instance name
        out.writeAscii(instance);
        out.write((byte)0);
        // Write dummy process ID
        out.write(new byte[]{0x01, 0x02, 0x00, 0x00});
        //
        out.flush();
    }

    /**
     * Process the pre login acknowledgement from the server.
     * <p>Packet contains; server version no, SSL mode, instance name
     * and process id.
     * <p>Server returns the following values for SSL mode:
     * <ol>
     * <ll>0 = Certificate installed encrypt login packet only.
     * <li>1 = Certificate installed client requests force encryption.
     * <li>2 = No certificate no encryption possible.
     * <li>3 = Server requests force encryption.
     * </ol>
     * @return The server side SSL mode.
     * @throws IOException
     */
    private int readPreLoginPacket() throws IOException {
        byte list[][] = new byte[8][];
        byte data[][] = new byte[8][];
        int recordCount = 0;

        byte record[] = new byte[5];
        // Read entry pointers
        record[0] = (byte)in.read();
        while ((record[0] & 0xFF) != 0xFF) {
            if (recordCount == list.length) {
                throw new IOException("Pre Login packet has more than 8 entries");
            }
            // Read record
            in.read(record, 1, 4);
            list[recordCount++] = record;
            record = new byte[5];
            record[0] = (byte)in.read();
        }
        // Read entry data
        for (int i = 0; i < recordCount; i++) {
            byte value[] = new byte[(byte)list[i][4]];
            in.read(value);
            data[i] = value;
        }
        if (Logger.isActive()) {
            // Diagnostic dump
            Logger.println("PreLogin server response");
            for (int i = 0; i < recordCount; i++) {
                Logger.println("Record " + i+ " = " +
                        Support.toHex(data[i]));
            }
        }
        if (recordCount > 1) {
            return data[1][0]; // This is the server side SSL mode
        } else {
            // Response too short to include SSL mode!
            return SSL_NO_ENCRYPT;
        }
    }

    /**
     * TDS 4.2 Login Packet.
     * <P>
     * @param serverName The server host name.
     * @param user The user name.
     * @param password The user password.
     * @param charset The required server character set.
     * @param appName The application name.
     * @param progName The program name.
     * @param language The server language for messages
     * @param packetSize The required network packet size.
     * @throws IOException
     */
    private void send42LoginPkt(final String serverName,
                                final String user,
                                final String password,
                                final String charset,
                                final String appName,
                                final String progName,
                                final String language,
                                final int packetSize)
        throws IOException {
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
        byte[] tmp = Support.encodeString(connection.getCharset(), password);
        out.write(tmp, 0, 253);
        out.write((byte) (tmp.length + 2));

        out.write((byte) 4);  // tds version
        out.write((byte) 2);

        out.write((byte) 0);
        out.write((byte) 0);
        putLoginString(progName, 10); // prog name

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
     * @param progName The program name.
     * @param language The server language for messages
     * @param packetSize The required network packet size.
     * @throws IOException
     */
    private void send50LoginPkt(final String serverName,
                                final String user,
                                final String password,
                                final String charset,
                                final String appName,
                                final String progName,
                                final String language,
                                final int packetSize)
        throws IOException {
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
        byte[] tmp = Support.encodeString(connection.getCharset(), password);
        out.write(tmp, 0, 253);
        out.write((byte) (tmp.length + 2));

        out.write((byte) 5);  // tds version
        out.write((byte) 0);

        out.write((byte) 0);
        out.write((byte) 0);
        putLoginString(progName, 10); // prog name

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
        // Request capabilities
            (byte)0x01,(byte)0x0A,(byte)0x03,(byte)0x84,(byte)0x0E,(byte)0xEF,
            (byte)0x65,(byte)0x7F,(byte)0xFF,(byte)0xFF,(byte)0xFF,(byte)0xD6,
        // Response capabilities
            (byte)0x02,(byte)0x0A,(byte)0x00,(byte)0x02,(byte)0x00,(byte)0x07,
            (byte)0x9E,(byte)0x06,(byte)0x48,(byte)0x00,(byte)0x00,(byte)0x00
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
     * This method incorporates the Windows single sign on code contributed by
     * Magendran Sathaiah. To invoke single sign on just leave the user name
     * blank or null. NB. This can only work if the driver is being executed on
     * a Windows PC and <code>ntlmauth.dll</code> is on the path.
     *
     * @param serverName    server host name
     * @param database      required database
     * @param user          user name
     * @param password      user password
     * @param domain        Windows NT domain (or <code>null</code>)
     * @param appName       application name
     * @param progName      program name
     * @param language      server language for messages
     * @param macAddress    client network MAC address
     * @param netPacketSize TDS packet size to use
     * @throws IOException if an I/O error occurs
     */
    private void sendMSLoginPkt(final String serverName,
                                final String database,
                                final String user,
                                final String password,
                                final String domain,
                                final String appName,
                                final String progName,
                                final String language,
                                final String macAddress,
                                final int netPacketSize)
            throws IOException, SQLException {
        final byte[] empty = new byte[0];
        final String clientName = getHostName();
        boolean ntlmAuth = false;
        byte[] ntlmMessage = null;

        if (user == null || user.length() == 0) {
            // See if executing on a Windows platform and if so try and
            // use the single sign on native library.
            if (System.getProperty("os.name").toLowerCase().startsWith("windows")) {
                ntlmAuthSSO = true;
                ntlmAuth = true;
            } else {
                throw new SQLException(Messages.get("error.connection.sso"),
                        "08001");
            }
        } else if (domain != null && domain.length() > 0) {
            // Assume we want to use Windows authentication with
            // supplied user and password.
            ntlmAuth = true;
        }

        if (ntlmAuthSSO) {
            try {
                // Create the NTLM request block using the native library
                sspiJNIClient = SSPIJNIClient.getInstance();
                ntlmMessage = sspiJNIClient.invokePrepareSSORequest();
            } catch (Exception e) {
                throw new IOException("SSO Failed: " + e.getMessage());
            }
        }

        //mdb:begin-change
        short packSize = (short) (86 + 2 *
                (clientName.length() +
                appName.length() +
                serverName.length() +
                progName.length() +
                database.length() +
                language.length()));
        final short authLen;
        //NOTE(mdb): ntlm includes auth block; sql auth includes uname and pwd.
        if (ntlmAuth) {
            if (ntlmAuthSSO && ntlmMessage != null) {
                authLen = (short) ntlmMessage.length;
            } else {
                authLen = (short) (32 + domain.length());
            }
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
        out.write(flags);

        //mdb: this byte controls what kind of auth we do.
        flags = 0x03; // ODBC (JDBC) driver
        if (ntlmAuth)
            flags |= 0x80; // Use NT authentication
        out.write(flags);

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

        // Program name
        out.write((short)curPos);
        out.write((short) progName.length());
        curPos += progName.length() * 2;

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
        out.write(progName);
        out.write(language);
        out.write(database);

        //mdb: add the ntlm auth info...
        if (ntlmAuth) {
            if (ntlmAuthSSO) {
                // Use the NTLM message generated by the native library
                out.write(ntlmMessage);
            } else {
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

        // Prepare and Set NTLM Type 2 message appropriately
        // Author: mahi@aztec.soft.net
        if (ntlmAuthSSO) {
            byte[] ntlmMessage = currentToken.ntlmMessage;
            try {
                // Create the challenge response using the native library
                ntlmMessage = sspiJNIClient.invokePrepareSSOSubmit(ntlmMessage);
            } catch (Exception e) {
                throw new IOException("SSO Failed: " + e.getMessage());
            }
            out.write(ntlmMessage);
        } else {
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
        }
        out.flush();
    }

    /**
     * Read the next TDS token from the response stream.
     *
     * @throws SQLException if an I/O or protocol error occurs
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
                case TDS5_PARAMFMT2_TOKEN:
                    tds5ParamFmt2Token();
                    break;
                case TDS_LANG_TOKEN:
                    tdsInvalidToken();
                    break;
                case TDS5_WIDE_RESULT:
                    tds5WideResultToken();
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
                case TDS_OFFSETS_TOKEN:
                    tdsOffsetsToken();
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
                                Integer.toHexString((int) currentToken.token & 0xFF));
            }
        } catch (IOException ioe) {
            connection.setClosed();
            throw Support.linkException(
                new SQLException(
                       Messages.get(
                                "error.generic.ioerror", ioe.getMessage()),
                                    "08S01"), ioe);
        } catch (ProtocolException pe) {
            connection.setClosed();
            throw Support.linkException(
                new SQLException(
                       Messages.get(
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
                            Integer.toHexString((int) currentToken.token & 0xFF));
    }

    /**
     * Process TDS 5 Sybase 12+ Dynamic results parameter descriptor.
     * <p>When returning output parameters this token will be followed
     * by a TDS5_PARAMS_TOKEN with the actual data.
     * @throws IOException
     * @throws ProtocolException
     */
    private void tds5ParamFmt2Token() throws IOException, ProtocolException {
        in.readInt(); // Packet length
        int paramCnt = in.readShort();
        ColInfo[] params = new ColInfo[paramCnt];
        for (int i = 0; i < paramCnt; i++) {
            //
            // Get the parameter details using the
            // ColInfo class as the server format is the same.
            //
            ColInfo col = new ColInfo();
            int colNameLen = in.read();
            col.realName = in.readNonUnicodeString(colNameLen);
            int column_flags = in.readInt();   /*  Flags */
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
            params[i] = col;
        }
        currentToken.previousToken  = TDS5_PARAMFMT2_TOKEN;
        currentToken.dynamParamInfo = params;
        currentToken.dynamParamData = new Object[paramCnt];
    }

    /**
     * Process Sybase 12+ wide result token which provides enhanced
     * column meta data.
     *
     * @throws IOException
     */
     private void tds5WideResultToken()
         throws IOException, ProtocolException
     {
         in.readInt(); // Packet length
         int colCnt   = in.readShort();
         this.columns = new ColInfo[colCnt];
         this.rowData = new Object[colCnt];
         this.tables  = null;

         for (int colNum = 0; colNum < colCnt; ++colNum) {
             ColInfo col = new ColInfo();
             //
             // Get the alias name
             //
             int nameLen = in.read();
             col.name  = in.readNonUnicodeString(nameLen);
             //
             // Get the catalog name
             //
             nameLen = in.read();
             col.catalog = in.readNonUnicodeString(nameLen);
             //
             // Get the schema name
             //
             nameLen = in.read();
             col.schema = in.readNonUnicodeString(nameLen);
             //
             // Get the table name
             //
             nameLen = in.read();
             col.tableName = in.readNonUnicodeString(nameLen);
             //
             // Get the column name
             //
             nameLen = in.read();
             col.realName  = in.readNonUnicodeString(nameLen);
             if (col.name == null || col.name.length() == 0) {
                 col.name = col.realName;
             }
             int column_flags = in.readInt();   /*  Flags */
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
     * Process stored procedure return status token.
     *
     * @throws IOException
     */
    private void tdsReturnStatusToken() throws IOException, SQLException {
        returnStatus = new Integer(in.readInt());
        if (this.returnParam != null) {
            returnParam.setOutValue(Support.convert(this.connection,
                    returnStatus,
                    returnParam.jdbcType,
                    connection.getCharset()));
        }
    }

    /**
     * Process procedure ID token.
     * <p>
     * Used by DBLIB to obtain the object id of a stored procedure.
     */
    private void tdsProcIdToken() throws IOException {
        in.skip(8);
    }

    /**
     * Process offsets token.
     * <p>
     * Used by DBLIB to return the offset of various keywords in a statement.
     * This saves the client from having to parse a SQL statement. Enabled with
     * <code>&quot;set offsets from on&quot;</code>.
     */
    private void tdsOffsetsToken() throws IOException {
        /*int keyword =*/ in.read();
        /*int unknown =*/ in.read();
        /*int offset  =*/ in.readShort();
    }

    /**
     * Process a TDS 7.0 result set token.
     *
     * @throws IOException
     * @throws ProtocolException
     */
    private void tds7ResultToken()
            throws IOException, ProtocolException, SQLException {
        endOfResults = false;

        int colCnt = in.readShort();

        if (colCnt < 0) {
            // Short packet returned by TDS8 when the column meta data is
            // supressed on cursor fetch etc.
            // NB. With TDS7 no result set packet is returned at all.
            return;
        }

        this.columns = new ColInfo[colCnt];
        this.rowData = new Object[colCnt];
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
            // Set the charsetInfo field of col
            if (tdsVersion >= Driver.TDS80 && col.collation != null) {
                TdsData.setColumnCharset(col, connection.getCollation(),
                        connection.getCharsetInfo());
            }

            int clen = in.read();

            col.realName = in.readUnicodeString(clen);
            col.name = col.realName;

            this.columns[i] = col;
        }
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
        this.tables = null;
        int bytesRead = 0;

        while (bytesRead < pktLen) {
            ColInfo col = new ColInfo();
            int nameLen = in.read();
            String name = in.readNonUnicodeString(nameLen);

            // FIXME This will not work for multi-byte charsets
            bytesRead = bytesRead + 1 + nameLen;
            col.realName  = name;
            col.name = name;

            colList.add(col);
        }

        int colCnt  = colList.size();
        this.columns = (ColInfo[]) colList.toArray(new ColInfo[colCnt]);
        this.rowData = new Object[colCnt];
    }

    /**
     * Process a TDS 4.2 column format token.
     *
     * @throws IOException
     * @throws ProtocolException
     */
    private void tds4ColFormatToken()
        throws IOException, ProtocolException {

        final int pktLen = in.readShort();

        int bytesRead = 0;
        int numColumns = 0;
        while (bytesRead < pktLen) {
            if (numColumns > columns.length) {
                throw new ProtocolException("Too many columns in TDS_COL_FMT packet");
            }
            ColInfo col = columns[numColumns];

            if (serverType == Driver.SQLSERVER) {
                col.userType = in.readShort();

                int flags = in.readShort();

                col.nullable = ((flags & 0x01) != 0)?
                                    ResultSetMetaData.columnNullable:
                                       ResultSetMetaData.columnNoNulls;
                col.isCaseSensitive = (flags & 0x02) != 0;
                col.isWriteable = (flags & 0x0C) != 0;
                col.isIdentity = (flags & 0x10) != 0;
            } else {
                // Sybase does not send column flags
                col.isCaseSensitive = false;
                col.isWriteable = true;

                if (col.nullable == ResultSetMetaData.columnNoNulls) {
                    col.nullable = ResultSetMetaData.columnNullableUnknown;
                }

                col.userType = in.readInt();
            }
            bytesRead += 4;

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
    private void tdsTableNameToken() throws IOException, ProtocolException {
        final int pktLen = in.readShort();
        int bytesRead = 0;
        ArrayList tableList = new ArrayList();

        while (bytesRead < pktLen) {
            int    nameLen;
            String tabName;
            TableMetaData table;
            if (tdsVersion >= Driver.TDS81) {
                // TDS8.1 supplies the database.owner.table as three separate
                // components which allows us to have names with embedded
                // periods.
                // Can't think why anyone would want that!
                table = new TableMetaData();
                bytesRead++;
                switch (in.read()) {
                    case 3: nameLen = in.readShort();
                            bytesRead += nameLen * 2 + 2;
                            table.catalog = in.readUnicodeString(nameLen);
                    case 2: nameLen = in.readShort();
                            bytesRead += nameLen * 2 + 2;
                            table.schema = in.readUnicodeString(nameLen);
                    case 1: nameLen = in.readShort();
                            bytesRead += nameLen * 2 + 2;
                            table.name = in.readUnicodeString(nameLen);
                    case 0: break;
                    default:
                        throw new ProtocolException("Invalid table TAB_NAME_TOKEN");
                }
            } else {
                if (tdsVersion >= Driver.TDS70) {
                    nameLen = in.readShort();
                    bytesRead += nameLen * 2 + 2;
                    tabName  = in.readUnicodeString(nameLen);
                } else {
                    // TDS 4.2 or TDS 5.0
                    nameLen = in.read();
                    bytesRead++;
                    if (nameLen == 0) {
                        break; // Sybase/SQL 6.5 use a zero length name to terminate list
                    }
                    tabName = in.readString(nameLen);
                    // FIXME This will not work for multi-byte charsets
                    bytesRead += nameLen;
                }
                table = new TableMetaData();
                // tabName can be a fully qualified name
                int dotPos = tabName.lastIndexOf('.');
                if (dotPos > 0) {
                    table.name = tabName.substring(dotPos + 1);

                    int nextPos = tabName.lastIndexOf('.', dotPos-1);
                    if (nextPos + 1 < dotPos) {
                        table.schema = tabName.substring(nextPos + 1, dotPos);
                    }
                    dotPos = nextPos;
                    nextPos = tabName.lastIndexOf('.', dotPos-1);
                    if (nextPos + 1 < dotPos) {
                        table.catalog = tabName.substring(nextPos + 1, dotPos);
                    }
                } else {
                    table.name = tabName;
                }
            }
            tableList.add(table);
        }
        if (tableList.size() > 0) {
            this.tables = (TableMetaData[]) tableList.toArray(new TableMetaData[tableList.size()]);
        }
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
        int columnIndex = 0;

        // In some cases (e.g. if the user calls 'CREATE CURSOR', the
        // TDS_TABNAME packet seems to be missing. Weird.
        if (tables == null) {
            in.skip(pktLen);
        } else {
            while (bytesRead < pktLen) {
                // Seems like all columns are always returned in the COL_INFO
                // packet and there might be more than 255 columns, so we'll
                // just increment a counter instead.
                // Ignore the column index.
                in.read();
                if (columnIndex >= columns.length) {
                    throw new ProtocolException("Column index " + (columnIndex + 1) +
                                                     " invalid in TDS_COLINFO packet");
                }
                ColInfo col = columns[columnIndex++];
                int tableIndex = in.read();
                if (tableIndex > tables.length) {
                    throw new ProtocolException("Table index " + tableIndex +
                                                     " invalid in TDS_COLINFO packet");
                }
                byte flags = (byte)in.read(); // flags
                bytesRead += 3;

                if (tableIndex != 0) {
                    TableMetaData table = tables[tableIndex-1];
                    col.catalog   = table.catalog;
                    col.schema    = table.schema;
                    col.tableName = table.name;
                }

                col.isKey           = (flags & 0x08) != 0;
                col.isHidden        = (flags & 0x10) != 0;

                // If bit 5 is set, we have a column name
                if ((flags & 0x20) != 0) {
                    final int nameLen = in.read();
                    bytesRead += 1;
                    final String colName = in.readString(nameLen);
                    // FIXME This won't work with multi-byte charsets
                    bytesRead += (tdsVersion >= Driver.TDS70)? nameLen * 2: nameLen;
                    col.realName = colName;
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
        // FIXME This won't work with multi-byte charsets
        // Skip any EED information to read rest of packet
        if (pktLen - sizeSoFar > 0)
            in.skip(pktLen - sizeSoFar);

        if (currentToken.token == TDS_ERROR_TOKEN)
        {
            if (severity < 10) {
                severity = 11; // Ensure treated as error
            }
            if (severity >= 20) {
                // A fatal error has occured, the connection will be closed by
                // the server immediately after the last TDS_DONE packet
                fatalError = true;
            }
        } else {
            if (severity > 9) {
                severity = 9; // Ensure treated as warning
            }
        }
        messages.addDiagnostic(number, state, severity,
                message, server, procName, line);
    }

    /**
     * Process output parameters.
     *
     * @throws IOException
     * @throws ProtocolException
     */
    private void tdsOutputParamToken()
        throws IOException, ProtocolException, SQLException {
        in.readShort(); // Packet length
        in.skipString(in.read()); // Column Name
        in.skip(5);

        ColInfo col = new ColInfo();
        TdsData.readType(in, col);
        // Set the charsetInfo field of col
        if (tdsVersion >= Driver.TDS80 && col.collation != null) {
            TdsData.setColumnCharset(col, connection.getCollation(),
                    connection.getCharsetInfo());
        }
        Object value = TdsData.readData(connection, in, col, false);

        if (parameters != null) {
            if (tdsVersion >= Driver.TDS80
                && returnParam != null
                && !returnParam.isSetOut) {
                // TDS 8 Allows function return values of types other than int
                if (value != null) {
                    parameters[nextParam].setOutValue(
                        Support.convert(connection, value,
                                parameters[nextParam].jdbcType,
                                connection.getCharset()));
                    parameters[nextParam].collation = col.collation;
                    parameters[nextParam].charsetInfo = col.charsetInfo;
                } else {
                    parameters[nextParam].setOutValue(null);
                }
            } else {
                // Look for next output parameter in list
                while (++nextParam < parameters.length) {
                    if (parameters[nextParam].isOutput) {
                        if (value != null) {
                            parameters[nextParam].setOutValue(
                                Support.convert(connection, value,
                                        parameters[nextParam].jdbcType,
                                        connection.getCharset()));
                            parameters[nextParam].collation = col.collation;
                            parameters[nextParam].charsetInfo = col.charsetInfo;
                        } else {
                            parameters[nextParam].setOutValue(null);
                        }
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
    private void tdsLoginAckToken() throws IOException {
        String product;
        int major, minor, build = 0;
        in.readShort(); // Packet length

        int ack = in.read(); // Ack TDS 5 = 5 for OK 6 for fail, 1/0 for the others

        // Update the TDS protocol version in this TdsCore and in the Socket.
        // The Connection will update itself immediately after this call.
        // As for other objects containing a TDS version value, there are none
        // at this point (we're just constructing the Connection).
        tdsVersion = TdsData.getTdsVersion(((int) in.read() << 24) | ((int) in.read() << 16)
                | ((int) in.read() << 8) | (int) in.read());
        socket.setTdsVersion(tdsVersion);

        product = in.readString(in.read());

        if (tdsVersion >= Driver.TDS70) {
            major = in.read();
            minor = in.read();
            build = in.read() << 8;
            build += in.read();
        } else {
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
            // Login rejected by server create SQLException
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
            rowData[i] =  TdsData.readData(connection, in, columns[i], readTextMode);
        }

        endOfResults = false;
        readTextMode = false;
    }

    /**
     * Process TDS 5.0 Params Token.
     * Stored procedure output parameters or data returned in parameter format
     * after a TDS Dynamic packet or as extended error information.
     * <p>The type of the preceding token is inspected to determine if this packet
     * contains output parameter result data. A TDS5_PARAMFMT2_TOKEN is sent before
     * this one in Sybase 12 to introduce output parameter results.
     * A TDS5_PARAMFMT_TOKEN is sent before this one to introduce extended error
     * information.
     *
     * @throws IOException
     */
    private void tds5ParamsToken() throws IOException, ProtocolException, SQLException {
        if (currentToken.dynamParamInfo == null) {
            throw new ProtocolException(
              "TDS 5 Param results token (0xD7) not preceded by param format (0xEC or 0X20).");
        }

        for (int i = 0; i < currentToken.dynamParamData.length; i++) {
            currentToken.dynamParamData[i] =
                TdsData.readData(connection, in, currentToken.dynamParamInfo[i], false);
            if (parameters != null) {
                // Sybase 12+ this token used to set output parameter results
                while (++nextParam < parameters.length) {
                    if (parameters[nextParam].isOutput) {
                        Object value = currentToken.dynamParamData[i];
                        if (value != null) {
                            parameters[nextParam].setOutValue(
                                Support.convert(connection, value,
                                        parameters[nextParam].jdbcType,
                                        connection.getCharset()));
                        } else {
                            parameters[nextParam].setOutValue(null);
                        }
                        break;
                    }
                }
            }
        }
    }

    /**
     * Process a TDS 5.0 capability token.
     * <p>
     * Sent after login to describe the server's capabilities.
     *
     * @throws IOException
     */
    private void tdsCapabilityToken()
        throws IOException, ProtocolException
    {
        in.readShort(); // Packet length
        if (in.read() != 1) {
            throw new ProtocolException("TDS_CAPABILITY: expected request string");
        }
        int capLen = in.read();
        if (capLen != 10) {
            throw new ProtocolException("TDS_CAPABILITY: byte count not 10");
        }
        byte capRequest[] = new byte[10];
        in.read(capRequest);
        if (in.read() != 2) {
            throw new ProtocolException("TDS_CAPABILITY: expected response string");
        }
        capLen = in.read();
        if (capLen != 10) {
            throw new ProtocolException("TDS_CAPABILITY: byte count not 10");
        }
        byte capResponse[] = new byte[10];
        in.read(capResponse);
        //
        // jTDS sends   01 0A 03 84 0E EF 65 7F FF FF FF D6
        // Sybase 11.92 01 0A 00 00 00 23 61 41 CF FF FF C6
        // Sybase 12.52 01 0A 03 84 0A E3 61 41 FF FF FF C6
        //
        // JTDS sends   02 0A 00 02 00 07 9E 06 48 00 00 00
        // Sybase 11.92 02 0A 00 00 00 00 00 06 00 00 00 00
        // Sybase 12.52 02 0A 00 00 00 00 00 06 00 00 00 00
        //
        //
        // Now set the correct attributes for this connection.
        // See the CT_LIB documentation for details on the bit
        // positions.
        //
        int capMask = 0;
        if ((capRequest[6] & 0x30) == 0x30) {
            capMask |= SYB_LONGDATA;
        }
        if ((capRequest[0] & 0x03) == 0x03) {
            capMask |= SYB_DATETIME;
        }
        if ((capRequest[2] & 0x02) == 0x02) {
            capMask |= SYB_EXTCOLINFO;
        }
        if ((capRequest[3] & 0x04) == 0x04) {
            capMask |= SYB_BITNULL;
        }
        if ((capRequest[1] & 0x80) == 0x80) {
            capMask |= SYB_UNICODE;
        }
        connection.setSybaseInfo(capMask);
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
                    connection.setServerCharset(charset);
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
    private void tds5ErrorToken() throws IOException {
        int pktLen = in.readShort(); // Packet length
        int sizeSoFar = 6;
        int number = in.readInt();
        int state = in.read();
        int severity = in.read();
        // Discard text state
        int stateLen = in.read();
        in.readNonUnicodeString(stateLen);
        in.read(); // == 1 if extended error data follows
        // Discard status and transaction state
        in.readShort();
        sizeSoFar += 4 + stateLen;

        int msgLen = in.readShort();
        String message = in.readNonUnicodeString(msgLen);
        sizeSoFar += 2 + msgLen;
        final int srvNameLen = in.read();
        String server = in.readNonUnicodeString(srvNameLen);
        sizeSoFar += 1 + srvNameLen;

        final int procNameLen = in.read();
        String procName = in.readNonUnicodeString(procNameLen);
        sizeSoFar += 1 + procNameLen;

        int line = in.readShort();
        sizeSoFar += 2;
        // FIXME This won't work with multi-byte charsets
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
        /*byte status = (byte)*/in.read();
        pktLen -= 2;
        if (type == (byte)0x20) {
            // Only handle aknowledgements for now
            int len = in.read();
            currentToken.dynamicId = in.readNonUnicodeString(len);
            pktLen -= len+1;
        }
        // FIXME This won't work with multi-byte charsets
        in.skip(pktLen);
    }

    /**
     * Process TDS 5 Dynamic results parameter descriptors.
     * <p>
     * With Sybase 12+ this has been superseded by the TDS5_PARAMFMT2_TOKEN
     * except when used to return extended error information.
     *
     * @throws IOException
     * @throws ProtocolException
     */
    private void tds5ParamFmtToken() throws IOException, ProtocolException {
        in.readShort(); // Packet length
        int paramCnt = in.readShort();
        ColInfo[] params = new ColInfo[paramCnt];
        for (int i = 0; i < paramCnt; i++) {
            //
            // Get the parameter details using the
            // ColInfo class as the server format is the same.
            //
            ColInfo col = new ColInfo();
            int colNameLen = in.read();
            col.realName = in.readNonUnicodeString(colNameLen);
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
        currentToken.previousToken  = TDS5_PARAMFMT_TOKEN;
        currentToken.dynamParamInfo = params;
        currentToken.dynamParamData = new Object[paramCnt];
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

        byte[] ntlmMessage = new byte[pktLen];
        in.read(ntlmMessage);
        int b1 = ((int) ntlmMessage[8] & 0xff);
        int b2 = ((int) ntlmMessage[9] & 0xff) << 8;
        int b3 = ((int) ntlmMessage[10] & 0xff) << 16;
        int b4 = ((int) ntlmMessage[11] & 0xff) << 24;
        final int seq =b4 | b3 | b2 | b1;

        if (seq != 2)
            throw new ProtocolException("NTLM challenge: got unexpected sequence number:" + seq);

        currentToken.nonce = new byte[8];
        currentToken.ntlmMessage = ntlmMessage;
        System.arraycopy(ntlmMessage, 24, currentToken.nonce, 0, 8);
    }

    /**
     * Process a TDS 5.0 result set packet.
     *
     * @throws IOException
     * @throws ProtocolException
     */
    private void tds5ResultToken() throws IOException, ProtocolException {
        in.readShort(); // Packet length
        int colCnt = in.readShort();
        this.columns = new ColInfo[colCnt];
        this.rowData = new Object[colCnt];
        this.tables = null;

        for (int colNum = 0; colNum < colCnt; ++colNum) {
            //
            // Get the column name
            //
            ColInfo col = new ColInfo();
            int colNameLen = in.read();
            col.realName  = in.readNonUnicodeString(colNameLen);
            col.name = col.realName;
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
    private void tdsDoneToken() throws IOException {
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
            //
            // There are no more results or pending cancel packets
            // to process.
            //
            endOfResponse = true;

            if (fatalError) {
                // A fatal error has occured, the server has closed the
                // connection
                connection.setClosed();
            }
        }

        if (serverType == Driver.SQLSERVER) {
            //
            // MS SQL Server provides additional information we
            // can use to return special row counts for DDL etc.
            //
            if (currentToken.operation == (byte) 0xC1) {
                currentToken.status &= ~DONE_ROW_COUNT;
            }
        }

        if ((currentToken.status & DONE_CANCEL) != 0) {
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
                              boolean noMetaData,
                              boolean sendNow)
            throws IOException, SQLException {
        if (procName != null) {
            // RPC call
            out.setPacketType(RPC_PKT);
            byte[] buf = Support.encodeString(connection.getCharset(), procName);

            out.write((byte) buf.length);
            out.write(buf);
            out.write((short) (noMetaData ? 2 : 0));

            if (parameters != null) {
                for (int i = nextParam + 1; i < parameters.length; i++) {
                    if (parameters[i].name != null) {
                       buf = Support.encodeString(connection.getCharset(),
                               parameters[i].name);
                       out.write((byte) buf.length);
                       out.write(buf);
                    } else {
                       out.write((byte) 0);
                    }

                    out.write((byte) (parameters[i].isOutput ? 1 : 0));
                    TdsData.writeParam(out,
                                       connection.getCharsetInfo(),
                                       null,
                                       parameters[i]);
                }
            }
            if (!sendNow) {
                // Send end of packet byte to batch RPC
                out.write((byte) DONE_END_OF_RESPONSE);
            }
        } else if (sql.length() > 0) {
            if (parameters != null) {
                sql = Support.substituteParameters(sql, parameters, tdsVersion);
            }

            out.setPacketType(QUERY_PKT);
            out.write(sql);
            if (!sendNow) {
                // Batch SQL statements
                out.write("\r\n");
            }
        }
    }

    /**
     * Execute SQL using TDS 5.0 protocol.
     *
     * @param sql The SQL statement to execute.
     * @param procName Stored procedure to execute or null.
     * @param parameters Parameters for call or null.
     * @throws SQLException
     */
    private void executeSQL50(String sql,
                              String procName,
                              ParamInfo[] parameters)
        throws IOException, SQLException {
        boolean haveParams    = parameters != null;
        boolean useParamNames = false;
        currentToken.previousToken  = 0;
        currentToken.dynamParamInfo = null;
        currentToken.dynamParamData = null;

        // Sybase does not allow text or image parameters
        for (int i = 0; haveParams && i < parameters.length; i++) {
            if (parameters[i].sqlType.equals("text")
                || parameters[i].sqlType.equals("image")) {
                if (procName != null && procName.length() > 0) {
                    // Call to store proc nothing we can do
                    if (parameters[i].sqlType.equals("text")) {
                        throw new SQLException(
                                        Messages.get("error.chartoolong"), "HY000");
                    }

                    throw new SQLException(
                                     Messages.get("error.bintoolong"), "HY000");
                }

                // prepared statement substitute parameters into SQL
                sql = Support.substituteParameters(sql, parameters, tdsVersion);
                haveParams = false;
                procName = null;

                break;
            }
        }

        out.setPacketType(SYBQUERY_PKT);

        if (procName == null) {
            // Use TDS_LANGUAGE TOKEN with optional parameters
            out.write((byte)TDS_LANG_TOKEN);

            if (haveParams) {
                sql = Support.substituteParamMarkers(sql, parameters);
            }

            if (connection.isWideChar()) {
                // Need to preconvert string to get correct length
                byte[] buf = Support.encodeString(connection.getCharset(), sql);

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
            byte buf[] = Support.encodeString(connection.getCharset(), procName);

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
                len += TdsData.getTds5ParamSize(connection.getCharset(),
                        connection.isWideChar(),
                        parameters[i],
                        useParamNames);
            }

            out.write((short) len);
            out.write((short) ((nextParam < 0) ? parameters.length : parameters.length - 1));

            for (int i = nextParam + 1; i < parameters.length; i++) {
                TdsData.writeTds5ParamFmt(out,
                        connection.getCharset(),
                        connection.isWideChar(),
                        parameters[i],
                        useParamNames);
            }

            // Now write the actual data
            out.write((byte) TDS5_PARAMS_TOKEN);

            for (int i = nextParam + 1; i < parameters.length; i++) {
                TdsData.writeTds5Param(out,
                        connection.getCharset(),
                        parameters[i]);
            }
        }
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
        tds8SpNames.put("sp_execute",           new Integer(12));
        tds8SpNames.put("sp_prepexec",          new Integer(13));
        tds8SpNames.put("sp_prepexecrpc",       new Integer(14));
        tds8SpNames.put("sp_unprepare",         new Integer(15));
    }

    /**
     * Returns <code>true</code> if the specified <code>procName</code>
     * is a sp_prepare or sp_prepexec handle; returns <code>false</code>
     * otherwise.
     *
     * @param procName Stored procedure to execute or <code>null</code>.
     * @return <code>true</code> if the specified <code>procName</code>
     *   is a sp_prepare or sp_prepexec handle; <code>false</code>
     *   otherwise.
     */
    public static boolean isPreparedProcedureName(final String procName) {
        if (procName != null && procName.length() > 0
                && Character.isDigit(procName.charAt(0))) {
            return true;
        }

        return false;
    }

    /**
     * Execute SQL using TDS 7.0 protocol.
     *
     * @param sql The SQL statement to execute.
     * @param procName Stored procedure to execute or <code>null</code>.
     * @param parameters Parameters for call or <code>null</code>.
     * @param noMetaData Suppress meta data for cursor calls.
     * @throws SQLException
     */
    private void executeSQL70(String sql,
                              String procName,
                              ParamInfo[] parameters,
                              boolean noMetaData,
                              boolean sendNow)
        throws IOException, SQLException {
        int prepareSql = connection.getPrepareSql();

        if (parameters == null
                && (prepareSql == EXECUTE_SQL || prepareSql == PREPARE || prepareSql == PREPEXEC)) {
            // Downgrade EXECUTE_SQL, PREPARE and PREPEXEC to UNPREPARED
            // if there are no parameters.
            //
            // Should we downgrade TEMPORARY_STORED_PROCEDURES as well?
            // No it may be a complex select with no parameters but costly to
            // evaluate for each execution.
            prepareSql = UNPREPARED;
        }

        if (inBatch) {
            // For batch execution with parameters
            // we need to be consistant and use
            // execute SQL
            prepareSql = EXECUTE_SQL;
        }

        if (isPreparedProcedureName(procName)) {
            // If the procedure is a prepared handle then redefine the
            // procedure name as sp_execute with the handle as a parameter.
            ParamInfo params[];

            if (parameters != null) {
                params = new ParamInfo[1 + parameters.length];
                System.arraycopy(parameters, 0, params, 1, parameters.length);
            } else {
                params = new ParamInfo[1];
            }

            params[0] = new ParamInfo(Types.INTEGER, new Integer(procName), ParamInfo.INPUT);

            TdsData.getNativeType(connection, params[0]);

            parameters = params;

            // Use sp_execute approach
            procName = "sp_execute";
        } else if (procName == null) {
            if (prepareSql == PREPEXEC) {
                // TODO: Make this option work with batch execution!
                ParamInfo params[] = new ParamInfo[3 + parameters.length];

                System.arraycopy(parameters, 0, params, 3, parameters.length);

                // Setup prepare handle param
                params[0] = new ParamInfo(Types.INTEGER, null, ParamInfo.OUTPUT);
                TdsData.getNativeType(connection, params[0]);

                // Setup parameter descriptor param
                params[1] = new ParamInfo(Types.LONGVARCHAR,
                        Support.getParameterDefinitions(parameters),
                        ParamInfo.UNICODE);
                TdsData.getNativeType(connection, params[1]);

                // Setup sql statemement param
                params[2] = new ParamInfo(Types.LONGVARCHAR,
                        Support.substituteParamMarkers(sql, parameters),
                        ParamInfo.UNICODE);
                TdsData.getNativeType(connection, params[2]);

                parameters = params;

                // Use sp_prepexec approach
                procName = "sp_prepexec";
            } else if (prepareSql != TdsCore.UNPREPARED && parameters != null) {
                ParamInfo[] params;

                params = new ParamInfo[2 + parameters.length];
                System.arraycopy(parameters, 0, params, 2, parameters.length);

                params[0] = new ParamInfo(Types.LONGVARCHAR,
                        Support.substituteParamMarkers(sql, parameters),
                        ParamInfo.UNICODE);
                TdsData.getNativeType(connection, params[0]);

                params[1] = new ParamInfo(Types.LONGVARCHAR,
                        Support.getParameterDefinitions(parameters),
                        ParamInfo.UNICODE);
                TdsData.getNativeType(connection, params[1]);

                parameters = params;

                // Use sp_executesql approach
                procName = "sp_executesql";
            }
        }

        if (procName != null) {
            // RPC call
            out.setPacketType(RPC_PKT);
            Integer shortcut;

            if (tdsVersion >= Driver.TDS80
                    && (shortcut = (Integer) tds8SpNames.get(procName)) != null) {
                // Use the shortcut form of procedure name for TDS8
                out.write((short) -1);
                out.write((short) shortcut.shortValue());
            } else {
                out.write((short) procName.length());
                out.write(procName);
            }

            out.write((short) (noMetaData ? 2 : 0));

            if (parameters != null) {
                for (int i = nextParam + 1; i < parameters.length; i++) {
                    if (parameters[i].name != null) {
                       out.write((byte) parameters[i].name.length());
                       out.write(parameters[i].name);
                    } else {
                       out.write((byte) 0);
                    }

                    out.write((byte) (parameters[i].isOutput ? 1 : 0));

                    TdsData.writeParam(out,
                            connection.getCharsetInfo(),
                            connection.getCollation(),
                            parameters[i]);
                }
            }
            if (!sendNow) {
                // Append RPC packets
                out.write((byte) DONE_END_OF_RESPONSE);
            }
        } else if (sql.length() > 0) {
            if (parameters != null) {
                sql = Support.substituteParameters(sql, parameters, tdsVersion);
            }

            // Simple query
            out.setPacketType(QUERY_PKT);
            out.write(sql);
            if (!sendNow) {
                // Append SQL packets
                out.write("\r\n");
            }
        }
    }

    /**
     * Set the server row count (to limit the number of rows in a result set)
     * and text size (to limit the size of returned TEXT/NTEXT fields).
     *
     * @param rowCount the number of rows to return or 0 for no limit or -1 to
     *                 leave as is
     * @param textSize the maximum number of bytes in a TEXT column to return
     *                 or -1 to leave as is
     * @throws SQLException if an error is returned by the server
     */
    private void setRowCountAndTextSize(int rowCount, int textSize)
            throws SQLException {
        boolean newRowCount =
                rowCount >= 0 && rowCount != connection.getRowCount();
        boolean newTextSize =
                textSize >= 0 && textSize != connection.getTextSize();
        if (newRowCount || newTextSize) {
            try {
                StringBuffer query = new StringBuffer(64);
                if (newRowCount) {
                    query.append("SET ROWCOUNT ").append(rowCount);
                }
                if (newTextSize) {
                    query.append(" SET TEXTSIZE ")
                            .append(textSize == 0 ? 2147483647 : textSize);
                }
                out.setPacketType(QUERY_PKT);
                out.write(query.toString());
                out.flush();
                endOfResponse = false;
                endOfResults  = true;
                wait(0);
                clearResponseQueue();
                messages.checkErrors();
                // Update the values stored in the Connection
                connection.setRowCount(rowCount);
                connection.setTextSize(textSize);
            } catch (IOException ioe) {
                throw new SQLException(
                            Messages.get("error.generic.ioerror",
                                                    ioe.getMessage()), "08S01");
            }
        }
    }

    /**
     * Wait for the first byte of the server response.
     *
     * @param timeOut The time out period in seconds or 0.
     */
    private void wait(int timeOut) throws IOException, SQLException {
        Object timer = null;
        try {
            if (timeOut > 0) {
                // Start a query timeout timer
                timer = TimerThread.getInstance().setTimer(timeOut * 1000,
                        new TimerThread.TimerListener() {
                            public void timerExpired() {
                                TdsCore.this.cancel();
                            }
                        });
            }
            in.peek();
        } finally {
            if (timer != null) {
                if (!TimerThread.getInstance().cancelTimer(timer)) {
                    throw new SQLException(
                          Messages.get("error.generic.timeout"), "HYT00");
                }
            }
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
        if (hostName != null) {
            return hostName;
        }

        String name;

        try {
            name = java.net.InetAddress.getLocalHost().getHostName().toUpperCase();
        } catch (java.net.UnknownHostException e) {
            hostName = "UNKNOWN";
            return hostName;
        }

        int pos = name.indexOf('.');

        if (pos >= 0) {
            name = name.substring(0, pos);
        }

        if (name.length() == 0) {
            hostName = "UNKNOWN";
            return hostName;
        }

        try {
            Integer.parseInt(name);
            // All numbers probably an IP address
            hostName = "UNKNOWN";
            return hostName;
        } catch (NumberFormatException e) {
            // Bit tacky but simple check for all numbers
        }

        hostName = name;
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
