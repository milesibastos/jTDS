//
// Copyright 1998, 1999 CDS Networks, Inc., Medford Oregon
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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.*;
import java.io.InterruptedIOException;

import net.sourceforge.jtds.util.Logger;

/**
 * Implement the TDS protocol.
 *
 * @author     Craig Spannring
 * @author     Igor Petrovski
 * @author     The FreeTDS project
 * @created    March 17, 2001
 * @version    $Id: Tds.java,v 1.59 2004-05-30 22:06:53 bheineman Exp $
 */
public class Tds implements TdsDefinitions {
    private static GregorianCalendar cal = new GregorianCalendar();

    private TdsSocket sock = null;
    private TdsComm comm = null;

    private final TdsConnection conn;

    /** Tds version we're using. */
    private int tdsVer = Tds.TDS70;

    private int serverType = -1; // either Tds.SYBASE or Tds.SQLSERVER

    private boolean inUse; // MJH: Indicates that this Tds is active somewhere

    /**
     * If set, character parameters are sent as Unicode (NTEXT, NVARCHAR),
     * otherwise they are sent using the default encoding.
     */
    private final boolean useUnicode;

    /** True as long as there is incoming data available. */
    private boolean moreResults = false;

    /** Time zone offset on client (disregarding DST). */
    private final int zoneOffset = Calendar.getInstance().get(Calendar.ZONE_OFFSET);

    private int maxRows = 0;

    public final static String cvsVersion = "$Id: Tds.java,v 1.59 2004-05-30 22:06:53 bheineman Exp $";

    /**
     * The context of the result set currently being parsed.
     */
    private Context currentContext;

    public Tds(final TdsConnection connection,
               final TdsSocket tdsSocket,
               final int tdsVer,
               final int serverType,
               final boolean useUnicode) {
        this.conn = connection;
        this.sock = tdsSocket;
        this.tdsVer = tdsVer;
        this.serverType = serverType;
        this.useUnicode = useUnicode;

        comm = new TdsComm(sock, tdsVer, connection.getNetworkPacketSize());
    }

    /**
     * The type of server that we connect to.
     *
     * @return    TdsDefinitions.SYBASE or TdsDefinitions.SQLSERVER
     */
    public int getServerType() {
        return serverType;
    }

    /**
     * Get in use status of this Tds object.
     * @return The in use value as a <code>boolean</code>.
     */
    public boolean isInUse()
    {
        return this.inUse;
    }

    public void setInUse(boolean value)
    {
        this.inUse = value;
    }

    /**
     * Determine if the next subpacket is a result set.
     * <p>
     * This does not eat any input.
     *
     * @return     true if the next piece of data to read is a result set.
     * @exception  TdsException
     * @exception  java.io.IOException
     */
    public synchronized boolean isResultSet()
            throws TdsException, java.io.IOException {
        final byte type = comm.peek();

        /*
         * XXX to support 5.0 we need to expand our view of what a result
         * set is.
         */
        return type == TDS_COL_NAME_TOKEN || type == TDS_COLMETADATA;
    }

    /**
     * Determine if the next subpacket is a result row or a computed result row.
     * <p>
     * This does not eat any input.
     *
     * @return     true if the next piece of data to read is a result row.
     * @exception  TdsException
     * @exception  java.io.IOException
     */
    private synchronized boolean isResultRow()
            throws TdsException, java.io.IOException {
        final byte type = comm.peek();

        return type == TDS_ROW /*|| type == TDS_CMP_ROW_TOKEN*/;
    }

    /**
     * Determine if the next subpacket is an end of result set marker.
     * <p>
     * This does not eat any input.
     *
     * @return     true if the next piece of data to read is end of result set marker.
     * @exception  TdsException
     * @exception  java.io.IOException
     */
    private synchronized boolean isEndOfResults()
            throws TdsException, java.io.IOException {
        final byte type = comm.peek();
        return type == TDS_DONE || type == TDS_DONEPROC || type == TDS_DONEINPROC;
    }

    public void close() {
        comm.close();
        try {
            sock.close();
        } catch (java.io.IOException e) {
            // XXX should do something here
        }
    }

    public String toString() {
        return "Tds " + comm.getId() + ": "
                + sock.getLocalAddress() + ":" + sock.getLocalPort()
                + " -> " + sock.getInetAddress() + ":" + sock.getPort();
    }

    /**
     * Change the connection transaction isolation level and the database.
     *
     * @param   database  the database name to change to
     */
    protected synchronized void initSettings(
            final String database, final SQLWarningChain wChain) {
        try {
            if (database.length() > 0) {
                changeDB(database, wChain);
            }

            changeSettings(sqlStatementToInitialize());
        } catch (SQLException e) {
            wChain.addException(e);
        }
    }

    public void cancel() throws java.io.IOException, TdsException {
        sock.cancel(comm);
    }

    public boolean moreResults() {
        return moreResults;
    }

    /**
     * @param  sql               SQL statement to execute
     * @param  wChain            warning chain to place exceptions and warnings
     *                           into
     * @exception  SQLException  Description of Exception
     */
    public synchronized void submitProcedure(final String sql, final SQLWarningChain wChain)
            throws SQLException {

        try {
            executeQuery(sql, null, wChain, 0);

            // Skip to end
            do {
                final PacketResult res = processSubPacket();
                if (res instanceof PacketMsgResult) {
                    wChain.addOrReturn((PacketMsgResult) res);
                } else if (res instanceof PacketEndTokenResult) {
                    handleEndToken(res, wChain);
                }
            } while (moreResults);
        } catch (java.io.IOException e) {
            wChain.addException(TdsUtil.getSQLException("Network problem", "08S01", e));
        } catch (TdsUnknownPacketSubType e) {
            wChain.addException(TdsUtil.getSQLException("Unknown response", "08S01", e));
        } catch (TdsException e) {
            wChain.addException(TdsUtil.getSQLException(null, "HY000", e));
        }
    }

    /**
     * Execute a stored procedure on the SQLServer.<p>
     *
     * @param  procedureName
     * @param  formalParameterList
     * @param  actualParameterList
     * @param  stmt
     * @param  timeout
     * @exception  java.sql.SQLException
     */
    public void executeProcedure(
            final String procedureName,
            final ParameterListItem[] formalParameterList,
            final ParameterListItem[] actualParameterList,
            final TdsStatement stmt,
            final SQLWarningChain wChain,
            final int timeout,
            final boolean noMetaData)
            throws java.sql.SQLException
    {
        try {
            executeProcedureInternal(procedureName, formalParameterList,
                    actualParameterList, stmt, wChain, timeout, noMetaData);
        }
//        catch (java.io.IOException e) {
//            wChain.addException(TdsUtil.getSQLException("Network problem", "08S01", e));
//        }
        catch (TdsUnknownPacketSubType e) {
            wChain.addException(TdsUtil.getSQLException("Unknown response", "08S01", e));
        }
        catch (TdsException e) {
            wChain.addException(TdsUtil.getSQLException(null, "HY000", e));
        }

        wChain.checkForExceptions();
    }

    /**
     * Execute a stored procedure on the SQLServer.<p>
     *
     * @param  procedureName
     * @param  formalParameterList
     * @param  actualParameterList
     * @param  stmt
     * @param  timeout
     * @exception  java.sql.SQLException
     * @exception  TdsException
     */
    private synchronized void executeProcedureInternal(
            final String procedureName,
            //  MJH 14/03/04 The next parameter is redundant and should
            //  be removed. Will impact a number of calls so left for
            //  now until everyone is happy.
            final ParameterListItem[] formalParameterList,
            final ParameterListItem[] actualParameterList,
            final TdsStatement stmt,
            final SQLWarningChain wChain,
            final int timeout,
            final boolean noMetaData)
            throws java.sql.SQLException, TdsException {
        // SAfe We need to check if all cancel requests were processed and wait
        //      for any outstanding cancel. It could happen that we sent the
        //      CANCEL after the server sent us all the data, so the answer is
        //      going to come in a packet of its own.
//        while (cancelActive.booleanValue()) {
//            waitForDataOrTimeout(wChain, timeout);
//            // @todo Make sure the statement/resultset are not left in an inconsistent state.
//            processSubPacket();
//        }

        // A stored procedure has a packet type of 0x03 in the header packet.
        // for non-image date the packets consists of
        // offset         length        desc.
        // 0                1           The length of the name of the stored proc
        // 1                N1          The name of the stored proc
        // N1 + 1           2           unknown (filled with zeros?)
        // N1 + 3           2           unknown prefix for param 1 (zero filled?)
        // N1 + 5           1           datatype for param 1
        // N1 + 6           1           max length of param 1
        // N1 + 7           N2          parameter 1 data
        // ...
        //
        // For image data (datatype 0x22) the packet consists of
        // 0                1           The length of the name of the stored proc
        // 1                N1          The name of the stored proc
        // N1 + 1           2           unknown (filled with zeros?)
        // N1 + 3           2           unknown prefix for param 1 (zero filled?)
        // N1 + 5           1           datatype for param 1
        // N1 + 6           4           length of param 1
        // N1 + 10          4           length of param 1 (duplicated?)
        // N1 + 7           N2          parameter 1 data
        // ...
        checkMaxRows(stmt, wChain);

        int i;

        try {
            // SAfe We must not do this here, if an exception is thrown before
            //      the packet is actually sent, we will deadlock in skipToEnd
            //      with nothing to read.
            // moreResults = true;

            // Start sending the procedure execute packet.
            comm.startPacket(TdsComm.PROC);
            if (tdsVer == Tds.TDS70) {
                comm.appendTdsShort((short) (procedureName.length()));
                comm.appendChars(procedureName);
            }
            else {
                final byte[] nameBytes = conn.getEncoder().getBytes(procedureName);
                comm.appendByte((byte) nameBytes.length);
                comm.appendBytes(nameBytes);
            }
            // SAfe If this is set to 512 it means "don't return column
            //      information" (useful for scrollable statements, to reduce
            //      the amount of data passed around).
            comm.appendShort((short) (noMetaData ? 512 : 0));

            // Now handle the parameters
            for (i = 0; i < actualParameterList.length; i++) {
                // SAfe We could try using actualParameterList here, instead
                //      of formalParameterList. If it works (although it
                //      shouldn't) it will solve a lot of problems.
                byte nativeType = cvtJdbcTypeToNativeType(actualParameterList[i].type);

                if (actualParameterList[i].formalName == null) {
                    comm.appendByte((byte) 0);
                } else {
                    // Send parameter name
                    final String name = actualParameterList[i].formalName;

                    comm.appendByte((byte) name.length());
                    if (tdsVer == Tds.TDS70) {
                        comm.appendChars(name);
                    }
                    else {
                        final byte[] nameBytes = conn.getEncoder().getBytes(name);
                        comm.appendBytes(nameBytes);
                    }
                }

                if (actualParameterList[i].isOutput) {
                    comm.appendByte((byte) 1);

                    if (nativeType == SYBBIT && actualParameterList[i].value == null) {
                        actualParameterList[i].value = Boolean.FALSE;
                    }
                } else {
                    comm.appendByte((byte) 0);
                }

                if (actualParameterList[i].value == null
                        && (nativeType == SYBINT1 || nativeType == SYBINT2
                        || nativeType == SYBINT4)) {
                    nativeType = SYBINTN;
                }

                switch (nativeType) {
                case SYBCHAR: {
                    if (actualParameterList[i].value == null) {
                        sendSybChar(null, 255);
                        break;
                    }

                    final int max = actualParameterList[i].maxLength;
                    boolean isText, isUnicode;

                    // MJH - 14/03/04 OK to use actual parameter list as this is now
                    // always created in PreparedStatement_base prior to the call.
                    if (actualParameterList[i].formalType != null
                        && actualParameterList[i].formalType.charAt(0) == 'n') {
                        // This is a Unicode column, safe to assume TDS 7.0
                        isText = max > 4000;
                        isUnicode = true;
                    } else {
                        isText = (tdsVer < TDS70 && max > 255) || max > 8000;
                        isUnicode = false;
                    }

                    if (!isText) {
                        String val;
                        try {
                            val = (String) actualParameterList[i].value;
                        } catch (ClassCastException e) {
                            if (actualParameterList[i].value instanceof Boolean) {
                                val = ((Boolean) actualParameterList[i].value).booleanValue() ? "1" : "0";
                            } else {
                                val = actualParameterList[i].value.toString();
                            }
                        }

                        if (tdsVer < TDS70 && val.length() == 0) {
                            val = " ";
                        }

                        final int len = val.length();

                        // Test that the data isn't actually larger than the
                        // maximum size. Otherwise SQL Server will consider this
                        // an invalid packet and close the connection.
                        if (len > max) {
                            throw new SQLException(
                                    "String or binary data would be truncated.",
                                    "22001", 8152);
                        }

                        // MJH - 14/03/04 OK to use actual parameter list as this is now
                        // always created in PreparedStatement_base prior to the call.
                        if (isUnicode) {
                            comm.appendByte((byte) (SYBNVARCHAR | 0x80));
                            comm.appendTdsShort((short) (max * 2));
                            comm.appendTdsShort((short) (len * 2));
                            comm.appendChars(val);
                        } else {
                            // VARCHAR
                            sendSybChar(val, actualParameterList[i].maxLength);
                        }
                        break;
                    }
                }
                // Possible fall-through (if length is greater than the maximum
                // for CHAR/VARCHAR/NCHAR/NVARCHAR)
                case SYBTEXT: {
                    if (actualParameterList[i].isOutput) {
                        throw new SQLException(
                                "TEXT output parameters not supported", "HY000");
                    }

                    boolean isUnicode =
                            actualParameterList[i].formalType != null &&
                            actualParameterList[i].formalType.charAt(0) == 'n';

                    if (actualParameterList[i].value instanceof String) {
                        String tmp = (String) actualParameterList[i].value;
                        sendSybText(new java.io.StringReader(tmp),
                                    tmp.length(), isUnicode);
                    } else {
                        sendSybText((java.io.Reader) actualParameterList[i].value,
                                    actualParameterList[i].scale, isUnicode);
                    }

                    break;
                }
                case SYBINTN:
                    comm.appendByte(SYBINTN);
                    comm.appendByte((byte) 4); // maximum length of the field,

                    if (actualParameterList[i].value == null) {
                        comm.appendByte((byte) 0); // actual length
                    } else {
                        comm.appendByte((byte) 4); // actual length
                        comm.appendTdsInt(((Number) (actualParameterList[i].value)).intValue());
                    }
                    break;

                case SYBINT4:
                    comm.appendByte(SYBINT4);
                    comm.appendTdsInt(((Number) (actualParameterList[i].value)).intValue());
                    break;

                case SYBINT2:
                    comm.appendByte(SYBINT2);
                    comm.appendTdsShort(((Number) (actualParameterList[i].value)).shortValue());
                    break;

                case SYBINT1:
                    comm.appendByte(SYBINT1);
                    comm.appendByte(((Number) (actualParameterList[i].value)).byteValue());
                    break;

                case SYBFLT8:
                case SYBFLTN: {
                    if (actualParameterList[i].value == null) {
                        comm.appendByte(SYBFLTN);
                        comm.appendByte((byte) 8);
                        comm.appendByte((byte) 0);
                    } else {
                        Double d = null;
                        if (actualParameterList[i].value instanceof Double) {
                            d = (Double) (actualParameterList[i].value);
                        } else {
                            final Number n = (Number) (actualParameterList[i].value);
                            d = new Double(n.doubleValue());
                        }

                        comm.appendByte(SYBFLT8);
                        comm.appendFlt8(d);
                    }
                    break;
                }
                case SYBDATETIMN: {
                    writeDatetimeValue(actualParameterList[i].value);
                    break;
                }
                case SYBBIT:
                case SYBBITN: {
                    if (actualParameterList[i].value == null) {
                        comm.appendByte(SYBBITN);
                        comm.appendByte((byte) 1);
                        comm.appendByte((byte) 0);
                    } else {
                        comm.appendByte(SYBBIT);
                        if (actualParameterList[i].value.equals(Boolean.TRUE)) {
                            comm.appendByte((byte) 1);
                        } else {
                            comm.appendByte((byte) 0);
                        }
                    }
                    break;
                }
                case SYBNUMERIC:
                case SYBDECIMAL: {
                    writeDecimalValue(actualParameterList[i].value,
                                      actualParameterList[i].type,
                                      actualParameterList[i].scale);
                    break;
                }
                case SYBBINARY:
                case SYBVARBINARY: {
                    final int maxLength = actualParameterList[i].maxLength;

                    if (maxLength <= 255 || (maxLength <= 8000 && tdsVer >= TDS70)) {
                        final byte[] value = (byte[]) actualParameterList[i].value;

                        if (value == null) {
                            comm.appendByte(SYBVARBINARY);
                            comm.appendByte((byte) 255);
                            comm.appendByte((byte) 0);
                        } else {
                            // Test that the data isn't actually larger than the
                            // maximum size. Otherwise SQL Server will consider
                            // this an invalid packet and close the connection.
                            if (value.length > maxLength) {
                                throw new SQLException(
                                        "String or binary data would be truncated.",
                                        "22001", 8152);
                            }

                            if (value.length > 255) {
                                comm.appendByte(SYBBIGVARBINARY);
                                comm.appendTdsShort((short) 8000);
                                comm.appendTdsShort((short) value.length);
                            } else {
                                comm.appendByte(SYBVARBINARY);
                                comm.appendByte((byte) 255);
                                comm.appendByte((byte) value.length);
                            }
                            comm.appendBytes(value);
                        }
                        break;
                    }
                }
                // Possible fall-through (if length is greater than the maximum
                // for BINARY/VARBINARY)
                case SYBIMAGE: {
                    if (actualParameterList[i].isOutput) {
                        throw new SQLException(
                                "IMAGE output parameters not supported", "HY000");
                    }

                    if (actualParameterList[i].value instanceof byte[]) {
                        byte[] tmp = (byte[]) actualParameterList[i].value;
                        sendSybImage(new java.io.ByteArrayInputStream(tmp),
                                     tmp.length);
                    } else {
                        sendSybImage((java.io.InputStream) actualParameterList[i].value,
                                     actualParameterList[i].scale);
                    }

                    break;
                }
                case SYBVOID:
                case SYBVARCHAR:
                case SYBDATETIME4:
                case SYBREAL:
                case SYBMONEY:
                case SYBDATETIME:
                case SYBMONEYN:
                case SYBMONEY4:
                default:
                    throw new SQLException("Not implemented for nativeType 0x"
                                           + Integer.toHexString(nativeType), "HY000");
                }
            }

            moreResults = true;
            comm.sendPacket();
            waitForDataOrTimeout(wChain, timeout);

        } catch (java.io.IOException e) {
            throw TdsUtil.getSQLException("Network error", "08S01", e);
        } finally {
            comm.packetType = 0;
        }
    }

    private void checkMaxRows(final java.sql.Statement stmt, final SQLWarningChain wChain)
             throws java.sql.SQLException
    {
        if (stmt == null) { // internal usage
            return;
        }
        final int maxRows = stmt.getMaxRows();
        if ( maxRows != this.maxRows) {
            submitProcedure("set rowcount " + maxRows, wChain);
            this.maxRows = maxRows;
        }
    }

    /**
     * Send a query to the SQLServer for execution.
     *
     * @param  sql    sql statement to execute.
     * @param  stmt
     * @param  timeout
     * @exception  java.sql.SQLException
     */
    public void executeQuery(final String sql, final TdsStatement stmt,
                             final SQLWarningChain wChain, final int timeout)
            throws SQLException {
        try {
            executeQueryInternal(sql, stmt, wChain, timeout);
        }
        catch (java.io.IOException e) {
            wChain.addException(TdsUtil.getSQLException("Network problem", "08S01", e));
        }
        catch (TdsUnknownPacketSubType e) {
            wChain.addException(TdsUtil.getSQLException("Unknown response", "HY000", e));
        }
        catch (TdsException e) {
            wChain.addException(TdsUtil.getSQLException(null, "HY000", e));
        }

        wChain.checkForExceptions();
    }

    /**
     * Send a query to the SQLServer for execution.
     *
     * @param  sql    sql statement to execute.
     * @param  stmt
     * @param  timeout
     * @exception  java.io.IOException
     * @exception  java.sql.SQLException
     * @exception  TdsException
     */
    private synchronized void executeQueryInternal(final String sql, final TdsStatement stmt, final SQLWarningChain wChain, final int timeout)
            throws java.io.IOException, java.sql.SQLException, TdsException {
        // SAfe We need to check if all cancel requests were processed and wait
        //      for any outstanding cancel. It could happen that we sent the
        //      CANCEL after the server sent us all the data, so the answer is
        //      going to come in a packet of its own.
//        while (cancelActive.booleanValue()) {
//            waitForDataOrTimeout(wChain, timeout);
//            // @todo Make sure the statement/resultset are not left in an inconsistent state.
//            processSubPacket();
//        }

        checkMaxRows(stmt, wChain);

        // If the query has length 0, the server doesn't answer, so we'll hang
        if (sql.length() > 0) {
            try {
                comm.startPacket(TdsComm.QUERY);
                if (tdsVer == Tds.TDS70) {
                    comm.appendChars(sql);
                } else {
                    final byte[] sqlBytes = conn.getEncoder().getBytes(sql);
                    comm.appendBytes(sqlBytes, sqlBytes.length, (byte) 0);
                }
                moreResults = true;

                //JJ 1999-01-10
                comm.sendPacket();

                waitForDataOrTimeout(wChain, timeout);
            } finally {
                comm.packetType = 0;
            }
        }
    }

    /**
     * Eats up all tokens until the next relevant token (TDS_COLMETADATA or
     * TDS_DONE for regular Statements or TDS_COLMETADATA, TDS_DONE or
     * TDS_DONEINPROC for PreparedStatements).
     */
    protected synchronized void goToNextResult(final SQLWarningChain warningChain, final TdsStatement stmt)
            throws TdsException, java.io.IOException, SQLException {
        // SAfe Need to process messages, output parameters and return values
        //      here, or even better, in the processXXX methods.
        while (moreResults) {
            final byte next = comm.peek();

            // SAfe If the next packet is a rowcount or ResultSet, break
            if (next == TDS_DONE || next == TDS_DONEINPROC
                    || next == TDS_COLMETADATA || next == TDS_COL_NAME_TOKEN) {
                break;
            }

            final PacketResult res = processSubPacket();

            if (res instanceof PacketMsgResult) {
                warningChain.addOrReturn((PacketMsgResult) res);
            } else if (res instanceof PacketOutputParamResult) {
                stmt.handleParamResult((PacketOutputParamResult) res);
            } else if (res instanceof PacketRetStatResult) {
                stmt.handleRetStat((PacketRetStatResult) res);
            } else if (res instanceof PacketEndTokenResult) {
                handleEndToken(res, warningChain);
            }
        }
    }

    EncodingHelper getEncoder() {
        return conn.getEncoder();
    }

    /**
     * Accessor method to determine the TDS level used.
     *
     * @return    TDS42, TDS50, or TDS70.
     */
    int getTdsVer() {
        return tdsVer;
    }

    /**
     * Return the connection this Tds belongs ro.
     */
    TdsConnection getConnection() {
        return conn;
    }

    /**
     * Process an output parameter subpacket.
     * <p>
     * This routine assumes that the TDS_PARAM byte has already
     * been read.
     *
     * @return a <code>PacketOutputParamResult</code> wrapping an output
     *         parameter
     *
     * @exception TdsException
     * @exception java.io.IOException
     * Thrown if some sort of error occured reading bytes from the network.
     */
    private PacketOutputParamResult processOutputParam()
            throws TdsException, java.io.IOException {
        getSubPacketLength(); // Packet length
        comm.getString(comm.getByte() & 0xff, conn.getEncoder()); // Column name
        comm.skip(5);

        final byte colType = comm.getByte();

        /** @todo Refactor to combine this code with that in getRow() */
        final Object element;
        switch (colType) {
        case SYBINTN:
            comm.getByte(); // Column size
            element = getIntValue(colType);
            break;

        case SYBINT1:
        case SYBINT2:
        case SYBINT4:
            element = getIntValue(colType);
            break;

        case SYBIMAGE:
            comm.getByte(); // Column size
            element = getImageValue();
            break;

        case SYBTEXT:
            comm.getByte(); // Column size
            element = getTextValue(false);
            break;

        case SYBNTEXT:
            comm.getByte(); // Column size
            element = getTextValue(true);
            break;

        case SYBCHAR:
        case SYBVARCHAR:
            comm.getByte(); // Column size
            element = getCharValue(false, true);
            break;

        case SYBBIGVARBINARY:
            comm.getTdsShort(); // Column size
            int len = comm.getTdsShort();
            // if (tdsVer == Tds.TDS70 && len == 0xffff)
            element = comm.getBytes(len, true);
            break;

        case SYBBIGVARCHAR:
            comm.getTdsShort(); // Column size
            element = getCharValue(false, false);
            break;

        case SYBBIGNVARCHAR:
            comm.getTdsShort(); // Column size
            element = getCharValue(true, false);
            break;

        case SYBNCHAR:
        case SYBNVARCHAR:
            comm.getByte(); // Column size
            element = getCharValue(true, true);
            break;

        case SYBREAL:
            element = readFloatN(4);
            break;

        case SYBFLT8:
            element = readFloatN(8);
            break;

        case SYBFLTN:
            comm.getByte(); // Column size
            final int actual_size = comm.getByte(); // No need to & with 0xff
            element = readFloatN(actual_size);
            break;

        case SYBSMALLMONEY:
        case SYBMONEY:
        case SYBMONEYN:
            comm.getByte(); // Column size
            element = getMoneyValue(colType);
            break;

        case SYBNUMERIC:
        case SYBDECIMAL:
            comm.getByte(); // Column size
            comm.getByte(); // Precision
            final int scale = comm.getByte(); // No need to & with 0xff
            element = getDecimalValue(scale);
            break;

        case SYBDATETIMN:
            comm.getByte(); // Column size
            element = getDatetimeValue(colType);
            break;

        case SYBDATETIME4:
        case SYBDATETIME:
            element = getDatetimeValue(colType);
            break;

        case SYBVARBINARY:
        case SYBBINARY:
            comm.getByte(); // Column size
            len = (comm.getByte() & 0xff);
            element = comm.getBytes(len, true);
            break;

        case SYBBITN:
            comm.getByte(); // Column size
            if (comm.getByte() == 0) {
                element = null;
            } else {
                element = (comm.getByte() != 0) ? Boolean.TRUE : Boolean.FALSE;
            }
            break;

        case SYBBIT:
            element = (comm.getByte() != 0) ? Boolean.TRUE : Boolean.FALSE;
            break;

        case SYBUNIQUEID:
            len = comm.getByte() & 0xff;
            element = len == 0 ? null : TdsUtil.uniqueIdToString(comm.getBytes(len, false));
            break;

        default:
            throw new TdsNotImplemented("Don't now how to handle " +
                                        "column type 0x" +
                                        Integer.toHexString(colType));
        }

        return new PacketOutputParamResult(element);
    }

    /**
     * Process a subpacket reply.<p>
     *
     * <b>Note-</b> All subpackets must be processed through here. This is the
     * only routine has the proper locking to support the cancel method in the
     * Statement class. <br>
     *
     * @return packet that was processed.
     * @exception  TdsUnknownPacketSubType
     * @exception  java.io.IOException
     * @exception  TdsException
     */
    PacketResult processSubPacket()
            throws TdsUnknownPacketSubType, java.io.IOException,
            TdsException, SQLException {
        return processSubPacket(currentContext);
    }

    /**
     * Process a subpacket reply.<p>
     *
     * <b>Note-</b> All subpackets must be processed through here. Only this
     * routine has the proper locking to support the cancel method in the
     * Statement class. <br>
     *
     * @param  context
     * @return packet that was processed.
     * @exception  TdsUnknownPacketSubType
     * @exception  java.io.IOException
     * @exception  TdsException
     */
    private synchronized PacketResult processSubPacket(final Context context)
            throws TdsUnknownPacketSubType, SQLException,
            java.io.IOException, TdsException {
        // NOTE!!! Before adding anything to this list you must
        // consider the ramifications to the the handling of cancels
        // as implemented by the CancelController class.
        //
        // The CancelController class might implicitly assume it can call
        // processSubPacket() whenever it is looking for a cancel
        // acknowledgment.  It assumes that any results of the call
        // can be discarded.

        final byte packetSubType = comm.getByte();

        if (Logger.isActive()) {
            Logger.println("processSubPacket: " +
                           Integer.toHexString(packetSubType & 0xFF) + " " +
                           "moreResults: " + moreResults());
        }

        switch (packetSubType) {
        case TDS_ENVCHANGE:
            return processEnvChange();

        case TDS_ERROR:
        case TDS_INFO:
        case TDS_MSG50_TOKEN:
            return processMsg(packetSubType);

        case TDS_PARAM:
            return processOutputParam();

        case TDS_LOGINACK:
            return processLoginAck();

        case TDS_RETURNSTATUS:
            return processRetStat();

        case TDS_PROCID:
            return processProcId();

        case TDS_DONE:
        case TDS_DONEPROC:
        case TDS_DONEINPROC:
            final PacketEndTokenResult result = processEndToken(packetSubType);
            moreResults = result.moreResults();
            return result;

        case TDS_COL_NAME_TOKEN:
            return processColumnNames();

        case TDS_COL_INFO_TOKEN:
            return processColumnInfo();

        case TDS_UNKNOWN_0xA7:
        case TDS_UNKNOWN_0xA8:
            // XXX Need to figure out what this packet is
            comm.skip(comm.getTdsShort());
            return new PacketUnknown(packetSubType);

        case TDS_TABNAME:
            return processTabName();

        case TDS_COLINFO:
            return processColInfo();

        case TDS_ORDER:
            int len = comm.getTdsShort();
            comm.skip(len);
            return new PacketColumnOrderResult();

        case TDS_CONTROL:
            len = comm.getTdsShort();
            comm.skip(len);
            // FIXME - I'm just ignoring this
            return new PacketControlResult();

        case TDS_ROW:
            return getRow(context);

        case TDS_COLMETADATA:
            return processTds7Result();

            //mdb
        case TDS_AUTH_TOKEN:
            return processNtlmChallenge();

        default:
            throw new TdsUnknownPacketSubType(packetSubType);
        }
    }

    /**
     * Try to figure out what client name we should identify ourselves as. Get
     * the hostname of this machine,
     *
     * @return    name we will use as the client.
     */
    private static String getClientName() {
        // This method is thread safe.
        String tmp;
        try {
            tmp = java.net.InetAddress.getLocalHost().getHostName();
        } catch (java.net.UnknownHostException e) {
            tmp = "";
        }

        final StringTokenizer st = new StringTokenizer(tmp, ".");

        if (!st.hasMoreTokens()) {
            // This means hostname wasn't found for this machine.
            return "JOHNDOE";
        }

        // Look at the first (and possibly only) word in the name.
        tmp = st.nextToken();
        if (tmp.length() == 0) {
            // This means the hostname had no leading component.
            // (This case would be strange.)
            return "JANEDOE";
        } else if (Character.isDigit(tmp.charAt(0))) {
            // This probably means that the name was a quad-decimal
            // number.  We don't want to send that as our name,
            // so make one up.
            return "BABYDOE";
        } else {
            // Ah, Life is good.  We have a name.  All other
            // applications I've seen have upper case client names,
            // and so shall we.
            return tmp.toUpperCase();
        }
    }

    /**
     * Get the length of the current subpacket. <p>
     *
     * This will eat two bytes from the input socket.
     *
     * @return    length of the current subpacket.
     * @exception java.io.IOException
     * @exception TdsException
     */
    private int getSubPacketLength()
             throws java.io.IOException, TdsException {
        return comm.getTdsShort();
    }

    /**
     * Determine if a given datatype is a fixed size.
     *
     * @param  nativeColumnType the SQL Server datatype to check
     * @return                  <code>true</code> if the datatype is a fixed
     *                          size, <code>false</code> if the datatype is
     *                          a variable size
     * @exception  TdsException if the <code>nativeColumnType</code> is not a
     *                          known datatype.
     */
    private static boolean isFixedSizeColumn(final byte nativeColumnType)
            throws TdsException {
        switch (nativeColumnType) {
        case SYBINT1:
        case SYBINT2:
        case SYBINT4:
        case SYBFLT8:
        case SYBDATETIME:
        case SYBBIT:
        case SYBMONEY:
        case SYBMONEY4:
        case SYBSMALLMONEY:
        case SYBREAL:
        case SYBDATETIME4:
            return true;

        case SYBINTN:
        case SYBMONEYN:
        case SYBVARCHAR:
        case SYBNVARCHAR:
        case SYBDATETIMN:
        case SYBFLTN:
        case SYBCHAR:
        case SYBNCHAR:
        case SYBNTEXT:
        case SYBIMAGE:
        case SYBVARBINARY:
        case SYBBINARY:
        case SYBDECIMAL:
        case SYBNUMERIC:
        case SYBBITN:
        case SYBUNIQUEID:
            return false;

        default:
            throw new TdsException("Unrecognized column type 0x"
                                   + Integer.toHexString(nativeColumnType));
        }
    }

    private Object getMoneyValue(final int type)
            throws java.io.IOException, TdsException {
        final int len;

        switch (type) {
        case SYBSMALLMONEY:
        case SYBMONEY4:
            len = 4;
            break;

        case SYBMONEY:
            len = 8;
            break;

        case SYBMONEYN:
            len = comm.getByte(); // No need to & with 0xff
            break;

        default:
            throw new TdsException("Not a money value.");
        }

        if (len == 0) {
            return null;
        } else {
            final BigInteger x;

            if (len == 4) {
                x = BigInteger.valueOf(comm.getTdsInt());
            } else if (len == 8) {
                final byte b4 = comm.getByte();
                final byte b5 = comm.getByte();
                final byte b6 = comm.getByte();
                final byte b7 = comm.getByte();
                final byte b0 = comm.getByte();
                final byte b1 = comm.getByte();
                final byte b2 = comm.getByte();
                final byte b3 = comm.getByte();
                final long l = (long) (b0 & 0xff) + ((long) (b1 & 0xff) << 8)
                        + ((long) (b2 & 0xff) << 16) + ((long) (b3 & 0xff) << 24)
                        + ((long) (b4 & 0xff) << 32) + ((long) (b5 & 0xff) << 40)
                        + ((long) (b6 & 0xff) << 48) + ((long) (b7 & 0xff) << 56);
                x = BigInteger.valueOf(l);
            } else {
                throw new TdsConfused("Don't know what to do with len of "
                                      + len);
            }
            return new BigDecimal(x, 4);
        }
    }

    /**
     * Extracts decimal value from the server's results packet. Takes advantage
     * of Java's superb handling of large numbers, which does all the heavy
     * lifting for us. Format is:
     * <UL>
     *   <LI> Length byte <code>len</code> ; count includes sign byte.</LI>
     *
     *   <LI> Sign byte (0=negative; 1=positive).</LI>
     *   <LI> Magnitude bytes (array of <code>len</code> - 1 bytes, in
     *   little-endian order.</LI>
     * </UL>
     *
     * @param scale number of decimal digits after the decimal point.
     * @return      <code>BigDecimal</code> for extracted value (or
     *              <code>null</code> if appropriate).
     * @exception  TdsException
     * @exception  java.io.IOException
     * @exception  NumberFormatException
     */
    private Object getDecimalValue(final int scale)
            throws TdsException, java.io.IOException, NumberFormatException {
        int len = comm.getByte() & 0xff;
        if (--len < 1) {
            return null;
        }

        // RMK 2000-06-10.  Deduced from some testing/packet sniffing.
        final byte[] bytes = new byte[len];
        final int signum = comm.getByte() == 0 ? -1 : 1;
        while (len > 0) {
            bytes[--len] = comm.getByte();
        }
        final BigInteger bigInt = new BigInteger(signum, bytes);
        return new BigDecimal(bigInt, scale);
    }

    /**
     * Convert a Julian date from the Sybase epoch of 1900-01-01
     * to a Calendar object.
     * @param julianDate The Sybase days from 1900 value.
     * @param cal The Calendar object to populate.
     *
     * Algorithm  from Fliegel, H F and van Flandern, T C (1968). 
     * Communications of the ACM, Vol 11, No 10 (October, 1968). 
     * <pre>
     *           SUBROUTINE GDATE (JD, YEAR,MONTH,DAY)
     *     C
     *     C---COMPUTES THE GREGORIAN CALENDAR DATE (YEAR,MONTH,DAY)
     *     C   GIVEN THE JULIAN DATE (JD).
     *     C
     *           INTEGER JD,YEAR,MONTH,DAY,I,J,K
     *     C
     *           L= JD+68569
     *           N= 4*L/146097
     *           L= L-(146097*N+3)/4
     *           I= 4000*(L+1)/1461001
     *           L= L-1461*I/4+31
     *           J= 80*L/2447
     *           K= L-2447*J/80
     *           L= J/11
     *           J= J+2-12*L
     *           I= 100*(N-49)+I+L
     *     C
     *           YEAR= I
     *           MONTH= J
     *           DAY= K
     *     C
     *           RETURN
     *           END
     * </pre>
     */
    private static void sybaseToCalendar(int julianDate, GregorianCalendar cal)
    {
        int l = julianDate + 68569 + 2415021;
        int n = 4 * l / 146097;
        l = l - (146097 * n + 3) / 4;
        int i = 4000 * (l + 1) / 1461001;
        l = l - 1461 * i / 4 + 31;
        int j = 80 * l / 2447;
        int k = l - 2447 * j / 80;
        l = j / 11;
        j = j + 2 - 12 * l;
        i = 100 * (n - 49) + i + l;
        cal.set(Calendar.YEAR, i);
        cal.set(Calendar.MONTH, j-1);
        cal.set(Calendar.DAY_OF_MONTH, k);
    }
    
    /**
     * Get a DATETIME value from the server response stream.
     * @param type The TDS data type.
     * @return The java.sql.Timestamp value or null.
     * @throws java.io.IOException
     */
    private Object getDatetimeValue(final int type)
            throws java.io.IOException, TdsException {
        int len;
        int daysSince1900;
        int time;
        int hours;
        int minutes;
        int seconds;
        synchronized (cal) {
            if (type == SYBDATETIMN) {
                len = comm.getByte(); // No need to & with 0xff
            } else if (type == SYBDATETIME4) {
                len = 4;
            } else {
                len = 8;
            }

            switch (len) {
            case 0:
                return null;

            case 8:
                // A datetime is made of of two 32 bit integers
                // The first one is the number of days since 1900
                // The second integer is the number of seconds*300
                // Negative days indicate dates earlier than 1900.
                // The full range is 1753-01-01 to 9999-12-31.
                daysSince1900 = comm.getTdsInt();
                sybaseToCalendar(daysSince1900, cal);
                time = comm.getTdsInt();
                hours = time / 1080000;
                cal.set(Calendar.HOUR_OF_DAY, hours);
                time = time - hours * 1080000;
                minutes = time / 18000;
                cal.set(Calendar.MINUTE, minutes);
                time = time - (minutes * 18000);
                seconds = time / 300;
                cal.set(Calendar.SECOND, seconds);
                time = time - seconds * 300;
                time = time * 1000 / 300;
                cal.set(Calendar.MILLISECOND, (int)(time));
//
//              getTimeInMillis() is protected in java vm 1.3 :-(
//              return new Timestamp(cal.getTimeInMillis());
//
                return new Timestamp(cal.getTime().getTime());
            case 4:
                // A smalldatetime is two 16 bit integers.
                // The first is the number of days past January 1, 1900,
                // the second smallint is the number of minutes past
                // midnight. 
                // The full range is 1900-01-01 to 2079-06-06.
                daysSince1900 = ((int) comm.getTdsShort()) & 0xFFFF;
                sybaseToCalendar(daysSince1900, cal);
                minutes = comm.getTdsShort();
                hours = minutes / 60;
                cal.set(Calendar.HOUR_OF_DAY, hours);
                minutes = minutes - hours * 60;
                cal.set(Calendar.MINUTE, minutes);
                cal.set(Calendar.SECOND, 0);
                cal.set(Calendar.MILLISECOND, 0);
//                return new Timestamp(cal.getTimeInMillis());
                return new Timestamp(cal.getTime().getTime());
            default:
                throw new TdsNotImplemented("Invalid DATETIME value with size of " +
                                        len + " bytes.");
            }
        }
    }

    private Object getIntValue(final int type)
            throws java.io.IOException, TdsException {
        final int len;

        switch (type) {
        case SYBINTN:
            len = comm.getByte(); // No need to & with 0xff
            break;

        case SYBINT4:
            len = 4;
            break;

        case SYBINT2:
            len = 2;
            break;

        case SYBINT1:
            len = 1;
            break;

        default:
            throw new TdsNotImplemented("Can't handle integer of type "
                                        + Integer.toHexString(type));
        }

        switch (len) {
        case 4:
            return new Integer(comm.getTdsInt());

        case 2:
            return new Integer((short) comm.getTdsShort());

        case 1:
            return new Integer(comm.getByte() & 0xff);

        case 0:
            return null;

        default:
            throw new TdsConfused("Bad SYBINTN length of " + len);
        }
    }

    private Object getCharValue(final boolean wideChars, final boolean outputParam)
            throws TdsException, java.io.IOException {
        final boolean shortLen = (tdsVer == Tds.TDS70) && (wideChars || !outputParam);
        final int len = shortLen ? comm.getTdsShort() : (comm.getByte() & 0xFF);

        // SAfe 2002-08-23
        // TDS 7.0 no longer uses 0 length values as NULL
        if (tdsVer < TDS70 && len == 0 || tdsVer == TDS70 && len == 0xFFFF) {
            return null;
        } else if (len >= 0) {
            Object result;

            if (wideChars) {
                result = comm.getString(len >> 1, conn.getEncoder());
            } else {
                result = conn.getEncoder().getString(
                        comm.getBytes(len, false), 0, len);
            }

            // SAfe 2002-08-23
            // TDS 7.0 does not use the same logic, one space is one space
            if (tdsVer < TDS70 && " ".equals(result)) {
                // In SQL trailing spaces are stripped from strings
                // MS SQLServer denotes a zero length string
                // as a single space.
                return "";
            }

            return result;
        } else {
            throw new TdsConfused("String with length<0");
        }
    }

    private Object getTextValue(final boolean wideChars)
            throws TdsException, java.io.IOException {
        final byte hasValue = comm.getByte();

        if (hasValue == 0) {
            return null;
        } else {
            String result;

            // XXX Have no idea what these 24 bytes are
            // 2000-06-06 RMK They are the TEXTPTR (16 bytes) and the TIMESTAMP.
            comm.skip(24);

            final int len = comm.getTdsInt();

            // RMK 2000-06-11
            // The logic immediately below does not agree with test t0031,
            // so I'm commenting it out.  On the other hand, it's a bit
            // puzzling that a column defined as TEXT NOT NULL needs the
            // hasValue byte read just above, but apparently it does.
            //if (len == 0)
            //{
            //   result = null;
            //}
            //else
            if (len >= 0) {
                if (wideChars) {
                    result = comm.getString(len / 2, conn.getEncoder());
                } else {
                    result = conn.getEncoder().getString(
                            comm.getBytes(len, false), 0, len);
                }

                // SAfe 2002-08-23
                // TDS 7.0 does not use the same logic, one space is one space
                if (tdsVer < TDS70 && " ".equals(result)) {
                    // In SQL trailing spaces are stripped from strings
                    // MS SQLServer denotes a zero length string
                    // as a single space.
                    return "";
                }
            } else {
                throw new TdsConfused("String with length<0");
            }

            return result;
        }
    }

    private Object getImageValue() throws TdsException, java.io.IOException {
        final byte hasValue = comm.getByte();

        if (hasValue == 0) {
            return null;
        } else {
            // XXX Have no idea what these 24 bytes are
            // 2000-06-06 RMK They are the TEXTPTR (16 bytes) and the TIMESTAMP.
            comm.skip(24);

            final int len = comm.getTdsInt();

            // RMK 2000-06-11
            // The logic immediately below does not agree with test t0031,
            // so I'm commenting it out.  On the other hand, it's a bit
            // puzzling that a column defined as TEXT NOT NULL needs the
            // hasValue byte read just above, but apparently it does.
            //if (len == 0)
            //{
            //   result = null;
            //}
            //else
            if (len >= 0) {
                return comm.getBytes(len, true);
            } else {
                throw new TdsConfused("String with length<0");
            }
        }
    }

    /**
     * Get one result row from the TDS stream.
     * <p>
     * This will read a full row from the TDS stream and store it in a
     * PacketRowResult object.
     *
     * @param  result                   packet to load row from
     * @return                          the row value
     * @exception  TdsException
     * @exception  java.io.IOException
     *
     */
    private PacketRowResult loadRow(final PacketRowResult result)
            throws SQLException, TdsException, java.io.IOException {
        final Columns columnsInfo = result.getContext().getColumnInfo();

        for (int i = 1; i <= columnsInfo.realColumnCount(); i++) {
            final Object element;
            final int colType = columnsInfo.getNativeType(i);

            if (Logger.isActive())
                Logger.println("colno=" + i +
                               " type=" + colType +
                               " offset=" + Integer.toHexString(comm.inBufferIndex));

            switch (colType) {
            case SYBINTN:
            case SYBINT1:
            case SYBINT2:
            case SYBINT4:
                element = getIntValue(colType);
                break;

            case SYBIMAGE:
                element = getImageValue();
                break;

            case SYBTEXT:
                element = getTextValue(false);
                break;

            case SYBNTEXT:
                element = getTextValue(true);
                break;

            case SYBCHAR:
            case SYBVARCHAR:
                element = getCharValue(false, false);
                break;

            case SYBNCHAR:
            case SYBNVARCHAR:
                element = getCharValue(true, false);
                break;

            case SYBREAL:
                element = readFloatN(4);
                break;

            case SYBFLT8:
                element = readFloatN(8);
                break;

            case SYBFLTN:
                element = readFloatN(comm.getByte()); // No need to & with 0xff
                break;

            case SYBSMALLMONEY:
            case SYBMONEY:
            case SYBMONEYN:
                element = getMoneyValue(colType);
                break;

            case SYBNUMERIC:
            case SYBDECIMAL:
                element = getDecimalValue(columnsInfo.getScale(i));
                break;

            case SYBDATETIME4:
            case SYBDATETIMN:
            case SYBDATETIME:
                element = getDatetimeValue(colType);
                break;

            case SYBVARBINARY:
            case SYBBINARY:
                int len = (tdsVer == Tds.TDS70)
                        ? comm.getTdsShort()
                        : (comm.getByte() & 0xff);
                if (tdsVer == Tds.TDS70 && len == 0xffff) {
                    element = null;
                } else {
                    element = comm.getBytes(len, true);
                }
                break;

            case SYBBITN:
            case SYBBIT:
                if (colType == SYBBITN && comm.getByte() == 0) {
                    element = null;
                } else {
                    element = (comm.getByte() != 0) ? Boolean.TRUE : Boolean.FALSE;
                }
                break;

            case SYBUNIQUEID:
                len = comm.getByte() & 0xff;
                element = len == 0 ? null : TdsUtil.uniqueIdToString(comm.getBytes(len, false));
                break;

            default:
                throw new TdsNotImplemented("Don't now how to handle " +
                                            "column type 0x" +
                                            Integer.toHexString(colType));
            }

            result.setElementAt(i, element);
        }

        return result;
    }

    /**
     * Creates a <code>PacketRowResult</code> and calls
     * <code>loadRow(PacketRowResult)</code> with it.
     */
    private PacketRowResult getRow(final Context context)
            throws SQLException, TdsException, java.io.IOException {
        final PacketRowResult result = new PacketRowResult(context);
        return loadRow(result);
    }

    /**
     * Log onto the SQLServer.<p>
     *
     * This method is not synchronized and does not need to be so long as it
     * can only be called from the constructor.
     * <p>
     * <U>TDS 4.2 Login Packet</U>
     * <P>
     * Packet type (first byte) is 2. The following is from tds.h the numbers
     * on the left are offsets <I>not including</I> the packet header. <br>
     * Note: The logical logon packet is split into two physical packets. Each
     * physical packet has its own header.
     * <PRE>
     * -- 0 -- DBCHAR host_name[30];
     * -- 30 -- DBTINYINT host_name_length;
     * -- 31 -- DBCHAR user_name[30];
     * -- 61 -- DBTINYINT user_name_length;
     * -- 62 -- DBCHAR password[30];
     * -- 92 -- DBTINYINT password_length;
     * -- 93 -- DBCHAR host_process[30];
     * -- 123 -- DBTINYINT host_process_length;
     * -- 124 -- DBCHAR magic1[6]; -- here were most of the mystery stuff is --
     * -- 130 -- DBTINYINT bulk_copy;
     * -- 131 -- DBCHAR magic2[9]; -- here were most of the mystery stuff is --
     * -- 140 -- DBCHAR app_name[30];
     * -- 170 -- DBTINYINT app_name_length;
     * -- 171 -- DBCHAR server_name[30];
     * -- 201 -- DBTINYINT server_name_length;
     * -- 202 -- DBCHAR magic3; -- 0, dont know this one either --
     * -- 203 -- DBTINYINT password2_length;
     * -- 204 -- DBCHAR password2[30];
     * -- 234 -- DBCHAR magic4[223];
     * -- 457 -- DBTINYINT password2_length_plus2;
     * -- 458 -- DBSMALLINT major_version; -- TDS version --
     * -- 460 -- DBSMALLINT minor_version; -- TDS version --
     * -- 462 -- DBCHAR library_name[10]; -- Ct-Library or DB-Library --
     * -- 472 -- DBTINYINT library_length; -- Ct-Library or DB-Library --
     * -- 473 -- DBSMALLINT major_version2; -- program version --
     * -- 475 -- DBSMALLINT minor_version2; -- program version --
     * -- 477 -- DBCHAR magic6[3]; -- ? last two octets are 13 and 17 --
     *                             -- bdw reports last two as 12 and 16 here --
     *                             -- possibly a bitset flag --
     * -- 480 -- DBCHAR language[30]; -- ie us-english --
     * -- second packet --
     * -- 524 -- DBTINYINT language_length; -- 10 in this case --
     * -- 525 -- DBCHAR magic7; -- no clue... has 1 in the first octet --
     *                          -- bdw reports 0x0 --
     * -- 526 -- DBSMALLINT old_secure; -- explaination? --
     * -- 528 -- DBTINYINT encrypted; -- 1 means encrypted all password fields blank --
     * -- 529 -- DBCHAR magic8; -- no clue... zeros --
     * -- 530 -- DBCHAR sec_spare[9]; -- explaination --
     * -- 539 -- DBCHAR char_set[30]; -- ie iso_1 --
     * -- 569 -- DBTINYINT char_set_length; -- 5 --
     * -- 570 -- DBTINYINT magic9; -- 1 --
     * -- 571 -- DBCHAR block_size[6]; -- in text --
     * -- 577 -- DBTINYINT block_size_length;
     * -- 578 -- DBCHAR magic10[25]; -- lots of stuff here...no clue --
     * </PRE>
     * This routine will basically eat all of the data returned from the
     * SQL Server.
     *
     * @exception  TdsUnknownPacketSubType
     * @exception  TdsException
     * @exception  java.io.IOException
     * @exception  SQLException
     */
    protected synchronized void logon(final String serverName,
                                      final String database,
                                      final String user,
                                      final String password,
                                      final String domain,
                                      final String charset,
                                      final String appName,
                                      final String libName,
                                      final String macAddress)
            throws SQLException, TdsUnknownPacketSubType,
            java.io.IOException, TdsException {
        final byte pad = (byte) 0;
        final byte[] empty = new byte[0];

        try {
            // Added 2000-06-07.
            if (tdsVer == Tds.TDS70) {
                send70Login(serverName, database, user, password, domain,
                            appName, libName, macAddress);
            } else {
                EncodingHelper encoder = conn.getEncoder();

                comm.startPacket(TdsComm.LOGON);

                // hostname  (offset0)
                // comm.appendString("TOLEDO", 30, (byte)0);
                byte[] tmp = encoder.getBytes(getClientName());
                comm.appendBytes(tmp, 30, pad);
                comm.appendByte((byte) (tmp.length < 30 ? tmp.length : 30));

                // username (offset 31 0x1f)
                tmp = encoder.getBytes(user);
                comm.appendBytes(tmp, 30, pad);
                comm.appendByte((byte) (tmp.length < 30 ? tmp.length : 30));

                // password (offset 62 0x3e)
                tmp = encoder.getBytes(password);
                comm.appendBytes(tmp, 30, pad);
                comm.appendByte((byte) (tmp.length < 30 ? tmp.length : 30));

                // hostproc (offset 93 0x5d)
                tmp = encoder.getBytes("00000116");
                comm.appendBytes(tmp, 8, pad);

                // unused (offset 109 0x6d)
                comm.appendBytes(empty, (30 - 14), pad);

                // apptype (offset )
                comm.appendByte((byte) 0x0);
                comm.appendByte((byte) 0xA0);
                comm.appendByte((byte) 0x24);
                comm.appendByte((byte) 0xCC);
                comm.appendByte((byte) 0x50);
                comm.appendByte((byte) 0x12);

                // hostproc length (offset )
                comm.appendByte((byte) 8);

                // type of int2
                comm.appendByte((byte) 3);

                // type of int4
                comm.appendByte((byte) 1);

                // type of char
                comm.appendByte((byte) 6);

                // type of flt
                comm.appendByte((byte) 10);

                // type of date
                comm.appendByte((byte) 9);

                // notify of use db
                comm.appendByte((byte) 1);

                // disallow dump/load and bulk insert
                comm.appendByte((byte) 1);

                // sql interface type
                comm.appendByte((byte) 0);

                // type of network connection
                comm.appendByte((byte) 0);

                // spare[7]
                comm.appendBytes(empty, 7, pad);

                // appname
                tmp = encoder.getBytes(appName);
                comm.appendBytes(tmp, 30, pad);
                comm.appendByte((byte) (tmp.length < 30 ? tmp.length : 30));

                // server name
                tmp = encoder.getBytes(serverName);
                comm.appendBytes(tmp, 30, pad);
                comm.appendByte((byte) (tmp.length < 30 ? tmp.length : 30));

                // remote passwords
                comm.appendBytes(empty, 2, pad);
                tmp = encoder.getBytes(password);
                comm.appendBytes(tmp, 253, pad);
                comm.appendByte((byte) (tmp.length < 253 ? tmp.length + 2 : 253 + 2));

                // tds version
                comm.appendByte((byte) 4);
                comm.appendByte((byte) 2);
                comm.appendByte((byte) 0);
                comm.appendByte((byte) 0);

                // prog name
                tmp = encoder.getBytes(libName);
                comm.appendBytes(tmp, 10, pad);
                comm.appendByte((byte) (tmp.length < 10 ? tmp.length : 10));

                // prog version
                comm.appendByte((byte) 6);
                // Tell the server we can handle SQLServer version 6
                comm.appendByte((byte) 0);
                // Send zero to tell the server we can't handle any other version
                comm.appendByte((byte) 0);
                comm.appendByte((byte) 0);

                // auto convert short
                comm.appendByte((byte) 0);

                // type of flt4
                comm.appendByte((byte) 0x0D);

                // type of date4
                comm.appendByte((byte) 0x11);

                // language
                tmp = encoder.getBytes("us_english");
                comm.appendBytes(tmp, 30, pad);
                comm.appendByte((byte) (tmp.length < 30 ? tmp.length : 30));

                // notify on lang change
                comm.appendByte((byte) 1);

                // security label hierachy
                comm.appendShort((short) 0);

                // security components
                comm.appendBytes(empty, 8, pad);

                // security spare
                comm.appendShort((short) 0);

                // security login role
                comm.appendByte((byte) 0);

                // charset
                tmp = encoder.getBytes(charset);
                comm.appendBytes(tmp, 30, pad);
                comm.appendByte((byte) (tmp.length < 30 ? tmp.length : 30));

                // notify on charset change
                comm.appendByte((byte) 1);

                // length of tds packets
                tmp = encoder.getBytes("512");
                comm.appendBytes(tmp, 6, pad);
                comm.appendByte((byte) 3);

                // pad out to a longword
                comm.appendBytes(empty, 8, pad);
            }

            moreResults = true;
            comm.sendPacket();
        } finally {
            comm.packetType = 0;
        }

        final SQLWarningChain wChain = new SQLWarningChain();

        // Get the reply to the logon packet.
        while (moreResults) {
            final PacketResult res = processSubPacket();

            if (res instanceof PacketMsgResult) {
                wChain.addOrReturn((PacketMsgResult) res);
            } else if (res instanceof PacketAuthTokenResult) {
                //mdb: handle ntlm challenge by sending a response...
                sendNtlmChallengeResponse(
                        (PacketAuthTokenResult) res, user, password, domain);
            } else if (res instanceof PacketEndTokenResult) {
                handleEndToken(res, wChain);
            }
        }
        wChain.checkForExceptions();
    }

    /**
     * New code added to handle TDS 7.0 login, which uses a completely
     * different packet layout. Logic taken directly from freetds C code in
     * tds/login.c. Lots of magic values: No idea what most of this means.
     *
     * Added 2000-06-05.
     */
    private void send70Login(final String serverName,
                             final String database,
                             final String user,
                             final String password,
                             final String domain,
                             final String appName,
                             final String libName,
                             final String macAddress)
            throws java.io.IOException, TdsException {
        final byte pad = (byte) 0;
        final byte[] empty = new byte[0];
        final String clientName = getClientName();

        //mdb
        final boolean ntlmAuth = (domain.length() > 0);

        //mdb:begin-change
        short packSize = (short) (86 + 2 *
                (clientName.length() +
                appName.length() +
                serverName.length() +
                libName.length() +
                database.length()));
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

        comm.startPacket(TdsComm.LOGON70);
        comm.appendTdsInt(packSize);
        // TDS version
        comm.appendTdsInt(0x70000000);

        // Magic!
        comm.appendBytes(empty, 16, pad);

        comm.appendByte((byte) 0xE0);
        //mdb: this byte controls what kind of auth we do.
        if (ntlmAuth)
            comm.appendByte((byte) 0x83);
        else
            comm.appendByte((byte) 0x03);

        comm.appendBytes(empty, 10, pad);

        // Pack up value lengths, positions.
        short curPos = 86;

        // Hostname
        comm.appendTdsShort(curPos);
        comm.appendTdsShort((short) clientName.length());
        curPos += clientName.length() * 2;

        //mdb: NTLM doesn't send username and password...
        if (!ntlmAuth) {
            // Username
            comm.appendTdsShort(curPos);
            comm.appendTdsShort((short) user.length());
            curPos += user.length() * 2;

            // Password
            comm.appendTdsShort(curPos);
            comm.appendTdsShort((short) password.length());
            curPos += password.length() * 2;
        } else {
            comm.appendTdsShort(curPos);
            comm.appendTdsShort((short) 0);

            comm.appendTdsShort(curPos);
            comm.appendTdsShort((short) 0);
        }

        // App name
        comm.appendTdsShort(curPos);
        comm.appendTdsShort((short) appName.length());
        curPos += appName.length() * 2;

        // Server name
        comm.appendTdsShort(curPos);
        comm.appendTdsShort((short) serverName.length());
        curPos += serverName.length() * 2;

        // Unknown
        comm.appendTdsShort((short) 0);
        comm.appendTdsShort((short) 0);

        // Library name
        comm.appendTdsShort(curPos);
        comm.appendTdsShort((short) libName.length());
        curPos += libName.length() * 2;

        // Another unknown value
        // mdb: this is the "language"
        comm.appendTdsShort(curPos);
        comm.appendTdsShort((short) 0);

        // Database
        comm.appendTdsShort(curPos);
        comm.appendTdsShort((short) database.length());
        curPos += database.length() * 2;

        // MAC address
        comm.appendBytes(getMACAddress(macAddress));

        //mdb: location of ntlm auth block. note that for sql auth, authLen==0.
        comm.appendTdsShort(curPos);
        comm.appendTdsShort(authLen);

        //"next position" (same as total packet size)
        comm.appendTdsInt(packSize);

        comm.appendChars(clientName);

        // Pack up the login values.
        //mdb: for ntlm auth, uname and pwd aren't sent up...
        if (!ntlmAuth) {
            final String scrambledPw = tds7CryptPass(password);
            comm.appendChars(user);
            comm.appendChars(scrambledPw);
        }

        comm.appendChars(appName);
        comm.appendChars(serverName);
        comm.appendChars(libName);
        comm.appendChars(database);

        //mdb: add the ntlm auth info...
        if (ntlmAuth) {
            // host and domain name are _narrow_ strings.
            final byte[] domainBytes = domain.getBytes("UTF8");
            //byte[] hostBytes   = localhostname.getBytes("UTF8");

            final byte[] header = {0x4e, 0x54, 0x4c, 0x4d, 0x53, 0x53, 0x50, 0x00};
            comm.appendBytes(header); //header is ascii "NTLMSSP\0"
            comm.appendTdsInt(1);          //sequence number = 1
            comm.appendTdsInt(0xb201);     //flags (???)

            //domain info
            comm.appendTdsShort((short) domainBytes.length);
            comm.appendTdsShort((short) domainBytes.length);
            comm.appendTdsInt(32); //offset, relative to start of auth block.

            //host info
            //NOTE(mdb): not sending host info; hope this is ok!
            comm.appendTdsShort((short) 0);
            comm.appendTdsShort((short) 0);
            comm.appendTdsInt(32); //offset, relative to start of auth block.

            // add the variable length data at the end...
            comm.appendBytes(domainBytes);
        }
    }

    /**
     * Select a new database to use.
     *
     * @param  database                   Name of the database to use.
     */
    protected synchronized void changeDB(final String database, final SQLWarningChain wChain) {
        // XXX Check to make sure the database name
        // doesn't have funny characters.

        if (database.length() > 32 || database.length() < 1) {
            wChain.addException(
                    new SQLException("Name too long - " + database, "3D000"));
        }

        for (int i = 0; i < database.length(); i++) {
            final char ch = database.charAt(i);

            if (!
                    ((ch == '_' && i != 0)
                    || (ch == ' ' && i != 0)
                    || (ch >= 'a' && ch <= 'z')
                    || (ch >= 'A' && ch <= 'Z')
                    || (ch >= '0' && ch <= '9'))) {
                wChain.addException(
                        new SQLException("Bad database name- " + database, "3D000"));
            }
        }

        final String query = tdsVer == TDS70 ?
                ("use [" + database + ']') : "use " + database;

        try {
            submitProcedure(query, wChain);
        } catch (SQLException e) {
            // SAfe Have to do this because submitProcedure
            wChain.addException(e);
        }
    }

    /**
     * This will read a error (or warning) message from the SQLServer and
     * create a SqlMessage object from that message. <p>
     *
     * <b>Warning!</b> This is not synchronized because it assumes it will only
     * be called by processSubPacket() which is synchronized.
     *
     * @param packetSubType type of the current subpacket
     * @return              the message returned by the SQL Server
     * @exception  java.io.IOException
     * @exception  TdsException
     */
    private PacketMsgResult processMsg(final byte packetSubType)
            throws java.io.IOException, TdsException {
        final SqlMessage msg = new SqlMessage(serverType);
        final EncodingHelper encoder = conn.getEncoder();

        getSubPacketLength(); // Packet length

        msg.number = comm.getTdsInt();
        msg.state = comm.getByte() & 0xff;
        msg.severity = comm.getByte() & 0xff;

        final int msgLen = comm.getTdsShort();
        msg.message = comm.getString(msgLen, encoder);

        final int srvNameLen = comm.getByte() & 0xFF;
        msg.server = comm.getString(srvNameLen, encoder);

        if (packetSubType == TDS_INFO || packetSubType == TDS_ERROR) {
            // nop
            final int procNameLen = comm.getByte() & 0xFF;
            msg.procName = comm.getString(procNameLen, encoder);
        } else {
            throw new TdsConfused("Was expecting a msg or error token.  " +
                                  "Found 0x" +
                                  Integer.toHexString(packetSubType & 0xff));
        }

        msg.line = comm.getTdsShort();

        if (packetSubType == TDS_ERROR) {
            return new PacketErrorResult(packetSubType, msg);
        } else {
            return new PacketMsgResult(packetSubType, msg);
        }
    }

    /**
     * Process an env change message (TDS_ENV_CHG_TOKEN).<p>
     *
     * <b>Warning!</b> This is not synchronized because it assumes it will only
     * be called by processSubPacket() which is synchronized.
     *
     * @exception java.io.IOException
     * @exception TdsException
     */
    private PacketResult processEnvChange()
            throws java.io.IOException, TdsException {
        final EncodingHelper encoder = conn.getEncoder();
        final int len = getSubPacketLength();
        final int type = comm.getByte();

        switch (type) {
        case TDS_ENV_BLOCKSIZE:
            {
                final int blocksize;
                final int clen = comm.getByte() & 0xFF;
                blocksize = Integer.parseInt(comm.getString(clen, encoder));
                if (tdsVer == TDS70) {
                    comm.skip(len - 2 - clen * 2);
                } else {
                    comm.skip(len - 2 - clen);
                }
                conn.setNetworkPacketSize(blocksize);
                // SAfe This was only done for TDS70. Why?
                comm.resizeOutbuf(blocksize);
                if (Logger.isActive()) {
                    Logger.println("Changed blocksize to " + blocksize);
                }
            }
            break;
        case TDS_ENV_CHARSET:
            {
                final int clen = comm.getByte() & 0xFF;
                final String charset = comm.getString(clen, encoder);
                if (tdsVer == TDS70) {
                    comm.skip(len - 2 - clen * 2);
                } else {
                    comm.skip(len - 2 - clen);
                }
                conn.setCharset(charset);
                break;
            }
        case TDS_ENV_DATABASE:
            {
                int clen = comm.getByte() & 0xFF;
                final String newDb = comm.getString(clen, encoder);
                clen = comm.getByte() & 0xFF;
                final String oldDb = comm.getString(clen, encoder);

                conn.databaseChanged(newDb, oldDb);
                break;
            }
        default:
            {
                // XXX Should actually look at the env change
                // instead of ignoring it.
                comm.skip(len - 1);
                break;
            }
        }

        return new PacketResult(TDS_ENVCHANGE);
    }

    /**
     * Process an column name subpacket.
     * <p>
     * <b>Warning!</b> This is not synchronized because it assumes it will only
     * be called by processSubPacket() which is synchronized.
     *
     * @return a <code>PacketColumnNamesResult</code> instance
     * @exception  java.io.IOException
     * @exception  TdsException
     */
    private PacketColumnNamesResult processColumnNames()
            throws java.io.IOException, TdsException {
        final Columns columns = new Columns();
        final EncodingHelper encoder = conn.getEncoder();

        final int totalLen = comm.getTdsShort();

        int bytesRead = 0;
        int i = 0;
        while (bytesRead < totalLen) {
            final int colNameLen = comm.getByte() & 0xff;
            final String colName = comm.getString(colNameLen, encoder);
            bytesRead = bytesRead + 1 + colNameLen;
            i++;
            columns.setName(i, colName);
            columns.setLabel(i, colName);
        }

        currentContext = new Context(columns, encoder);

        return new PacketColumnNamesResult(columns);
    }

    /**
     * Process the columns information subpacket.
     * <p>
     * <b>Warning!</b> This is not synchronized because it assumes it will only
     * be called by processSubPacket() which is synchronized.
     *
     * @return a <code>PacketColumnInfoResult</code> instance
     * @exception  java.io.IOException
     * @exception  TdsException
     */
    private PacketColumnInfoResult processColumnInfo()
            throws java.io.IOException, TdsException {
        final Columns columns = new Columns();
        int precision;
        int scale;

        final int totalLen = comm.getTdsShort();

        int bytesRead = 0;
        int numColumns = 0;
        while (bytesRead < totalLen) {
            scale = -1;
            precision = -1;

            final int bufLength;
            final int dispSize = -1;

            final byte[] flagData = new byte[4];
            for (int i = 0; i < 4; i++) {
                flagData[i] = comm.getByte();
                bytesRead++;
            }
            final boolean nullable = (flagData[2] & 0x01) > 0;
            final boolean caseSensitive = (flagData[2] & 0x02) > 0;
            final boolean writable = (flagData[2] & 0x0C) > 0;
            final boolean autoIncrement = (flagData[2] & 0x10) > 0;
            String tableName = "";

            // Get the type of column
            final byte columnType = comm.getByte();
            bytesRead++;

            if (columnType == SYBTEXT || columnType == SYBIMAGE) {
                // XXX Need to find out what these next 4 bytes are
                //     Could they be the column size?
                comm.skip(4);
                bytesRead += 4;

                final int tableNameLen = comm.getTdsShort();
                bytesRead += 2;
                tableName = comm.getString(tableNameLen, conn.getEncoder());
                bytesRead += tableNameLen;

                bufLength = 2 << 31 - 1;
            } else if (columnType == SYBDECIMAL || columnType == SYBNUMERIC) {
                bufLength = comm.getByte(); // No need to & with 0xff
                bytesRead++;
                precision = comm.getByte(); // No need to & with 0xff
                // Total number of digits
                bytesRead++;
                scale = comm.getByte(); // No need to & with 0xff
                // # of digits after the decimal point
                bytesRead++;
            } else if (isFixedSizeColumn(columnType)) {
                bufLength = lookupBufferSize(columnType);
            } else {
                bufLength = (int) comm.getByte() & 0xff;
                bytesRead++;
            }
            numColumns++;

            populateColumn(columns.getColumn(numColumns), columnType, null,
                           dispSize, bufLength, nullable, autoIncrement, writable,
                           caseSensitive, tableName, precision, scale);
        }

        // Don't know what the rest is except that the
        final int skipLen = totalLen - bytesRead;
        if (skipLen != 0) {
            throw new TdsException(
                    "skipping " + skipLen + " bytes");
        }

        currentContext.getColumnInfo().merge(columns);

        return new PacketColumnInfoResult(columns);
    }

    /**
     * Process a TDS_TABNAME packet and place the list of table names into the
     * current context. These will be assigned to columns when the TDS_COLINFO
     * packet will be processed (it should be the very next packet).
     *
     * @return a <code>PacketTabNameResult</code> containing the table names
     * @throws java.io.IOException if an I/O error occurs
     * @throws TdsException if an internal error occurs
     */
    private PacketTabNameResult processTabName()
            throws java.io.IOException, TdsException {
        final int totalLen = comm.getTdsShort();
        int bytesRead = 0;

        String tabName;
        ArrayList tables = new ArrayList();

        if (tdsVer == TDS70) {
            while (bytesRead < totalLen) {
                int nameLen = comm.getTdsShort();
                bytesRead += 2;
                tabName = comm.getString(nameLen, conn.getEncoder());
                bytesRead += (nameLen << 1);
                tables.add(tabName);
            }
        } else {
            while (bytesRead < totalLen) {
                int nameLen = comm.getByte() & 0xff;
                bytesRead++;
                tabName = comm.getString(nameLen, conn.getEncoder());
                bytesRead += nameLen;
                tables.add(tabName);
            }
        }

        currentContext.setTables(tables);

        return new PacketTabNameResult(tables);
    }

    /**
     * Process a TDS_COLINFO packet and assign table names to columns in the
     * current context.
     *
     * @return a <code>PacketColInfoResult</code>
     * @throws java.io.IOException if an I/O error occurs
     * @throws TdsException if an internal error occurs
     */
    private PacketColInfoResult processColInfo()
            throws java.io.IOException, TdsException {
        final int totalLen = comm.getTdsShort();
        int bytesRead = 0;

        List tables = currentContext.getTables();
        Columns columns = currentContext.getColumnInfo();

        // In some cases (e.g. if the user calls 'CREATE CURSOR', the
        // TDS_TABNAME packet seems to be missing. Weird.
        if (tables == null) {
            comm.skip(totalLen);
        } else {
            while (bytesRead < totalLen) {
                int columnIndex = comm.getByte() & 0xff;
                Column col = columns.getColumn(columnIndex);
                int tableIndex = (comm.getByte() & 0xff) - 1;
                byte flags = comm.getByte(); // flags
                bytesRead += 3;

                if (tableIndex != -1) {
                    String tabName = tables.get(tableIndex).toString();

                    // tabName can be a fully qualified name
                    int dotPos = tabName.lastIndexOf('.');
                    if (dotPos > 0) {
                        col.setTableName(tabName.substring(dotPos + 1));

                        int nextPos = tabName.lastIndexOf('.', dotPos-1);
                        if (nextPos + 1 < dotPos) {
                            col.setSchema(tabName.substring(nextPos + 1, dotPos));
                        }

                        dotPos = nextPos;
                        nextPos = tabName.lastIndexOf('.', dotPos-1);
                        if (nextPos + 1 < dotPos) {
                            col.setCatalog(tabName.substring(nextPos + 1, dotPos));
                        }
                    } else {
                        col.setTableName(tabName);
                    }
                }

                // Hidden columns are (usually) at the end; make them invisible
                if ((flags & 0x10) != 0) {
                    if (columns.fakeColumnCount() >= columnIndex) {
                        columns.setFakeColumnCount(columnIndex - 1);
                    }
                }

                // If bit 5 is set, we have a column name
                if ((flags & 0x20) != 0) {
                    final int nameLen = comm.getByte() & 0xff;
                    bytesRead += 1;
                    final String colName = comm.getString(nameLen, conn.getEncoder());
                    bytesRead += (tdsVer == TDS70)
                            ? 2*colName.length() : colName.length();

                    col.setName(colName);
                }
            }
        }

        return new PacketColInfoResult();
    }

    /**
     * Process an end subpacket.
     * <p>
     * This routine assumes that the TDS_END_TOKEN byte has already been read.
     *
     * @param  packetType type of the end token
     * @return a <code>PacketEndTokenResult</code> for the next DONEINPROC,
     *        DONEPROC or DONE packet
     * @exception  TdsException
     * @exception  java.io.IOException Thrown if some sort of error occured
     *                                 reading bytes from the network.
     */
    private PacketEndTokenResult processEndToken(
            final byte packetType)
            throws TdsException, java.io.IOException {
        final int status = comm.getTdsShort();
        final int opcode = comm.getTdsShort();
        int rowCount = comm.getTdsInt();

        // If it isn't a valid row count or it was a SELECT, ignore it
        if ((status & 0x10) == 0 || packetType == TdsDefinitions.TDS_DONEPROC) {
            // DDL statements should return 0
            if (opcode == OPCODE_CREATE_TABLE
                    || opcode == OPCODE_DROP_TABLE
                    || opcode == OPCODE_ALTER_TABLE
                    || opcode == OPCODE_CREATE_PROC
                    || opcode == OPCODE_DROP_PROC) {
                rowCount = 0;
            } else {
                // Other statements should not return update counts
                rowCount = -1;
            }
        } else {
            // Only INSERT, UPDATE and DELETE should return update counts
            if (opcode == OPCODE_SELECT) {
                rowCount = -1;
            }
        }

        final PacketEndTokenResult result =
                new PacketEndTokenResult(packetType, status, rowCount);

        return result;
    }

    /**
     * Find out how many bytes a particular SQLServer data type takes.
     *
     * @param nativeColumnType
     * @return number of bytes required by the given type
     * @exception TdsException thrown if the given type either doesn't exist or
     *                         is a variable sized data type.
     */
    private static int lookupBufferSize(final byte nativeColumnType)
            throws TdsException {
        switch (nativeColumnType) {
        case SYBINT1:
            return 1;

        case SYBINT2:
            return 2;

        case SYBINT4:
            return 4;

        case SYBREAL:
            return 4;

        case SYBFLT8:
            return 8;

        case SYBDATETIME:
            return 8;

        case SYBDATETIME4:
            return 4;

        case SYBBIT:
            return 1;

        case SYBMONEY:
            return 8;

        case SYBMONEY4:
        case SYBSMALLMONEY:
            return 4;

        default:
            throw new TdsException("Not fixed size column "
                                   + nativeColumnType);
        }
    }

    private static int lookupDisplaySize(final byte nativeColumnType)
            throws TdsException {
        switch (nativeColumnType) {
        case SYBINT1:
            return 3;

        case SYBINT2:
            return 6;

        case SYBINT4:
            return 11;

        case SYBREAL:
            return 14;

        case SYBFLT8:
            return 24;

        case SYBDATETIME:
            return 23;

        case SYBDATETIME4:
            return 16;

        case SYBBIT:
            return 1;

        case SYBMONEY:
            return 21;

        case SYBMONEY4:
        case SYBSMALLMONEY:
            return 12;

        default:
            throw new TdsException("Not fixed size column "
                                   + nativeColumnType);
        }
    }

    /**
     * Convert a calendar date into days since 1900 (Sybase epoch).
     * <p>
     * Algorithm  from Fliegel, H F and van Flandern, T C (1968). 
     * Communications of the ACM, Vol 11, No 10 (October, 1968). 
     * 
     * <pre> 
     *           INTEGER FUNCTION JD (YEAR,MONTH,DAY)
     *     C
     *     C---COMPUTES THE JULIAN DATE (JD) GIVEN A GREGORIAN CALENDAR
     *     C   DATE (YEAR,MONTH,DAY).
     *     C
     *           INTEGER YEAR,MONTH,DAY,I,J,K
     *     C
     *           I= YEAR
     *           J= MONTH
     *           K= DAY
     *     C
     *           JD= K-32075+1461*(I+4800+(J-14)/12)/4+367*(J-2-(J-14)/12*12)
     *          2    /12-3*((I+4900+(J-14)/12)/100)/4
     *     C
     *           RETURN
     *           END
     * </pre>
     * @param year The year eg 2003.
     * @param month The month 1-12.
     * @param day The day in month 1-31.
     * @return The julian date adjusted for Sybase epoch of 1900.
     */
    private static int calendarToSybase(int year, int month, int day) {
        return day - 32075 + 1461 * (year + 4800 + (month - 14) / 12) / 
                   4 + 367 * (month - 2 - (month - 14) / 12 * 12) /
                        12 - 3 * ((year + 4900 + (month -14) / 12) / 100) / 
                            4 - 2415021;
    }

    private void writeDatetimeValue(final Object value)
            throws java.io.IOException {
        comm.appendByte(SYBDATETIMN);
        comm.appendByte((byte) 8);

        if (value == null) {
            comm.appendByte((byte) 0);
        } else {
            int daysSince1900;
            int time;

            synchronized (cal) {
                cal.setTime((java.util.Date) value);

                if (!Driver.JDBC3) {
                    // Not Running under 1.4 so need to add milliseconds
                    cal.set(Calendar.MILLISECOND, 
                        ((java.sql.Timestamp) value).getNanos() / 1000000);
                }

                if (value instanceof java.sql.Time) {
                    daysSince1900 = 0;
                } else {
                    daysSince1900 = calendarToSybase(cal.get(Calendar.YEAR), 
                                                     cal.get(Calendar.MONTH) + 1,
                                                     cal.get(Calendar.DAY_OF_MONTH));
                }   
                time  = cal.get(Calendar.HOUR_OF_DAY) * 1080000;
                time += cal.get(Calendar.MINUTE) * 18000;
                time += cal.get(Calendar.SECOND) * 300;

                if (value instanceof java.sql.Timestamp) {
                    time += cal.get(Calendar.MILLISECOND) * 300 / 1000;
                }
            }

            comm.appendByte((byte) 8);
            comm.appendTdsInt((int) daysSince1900);
            comm.appendTdsInt((int) time);
        }
    }

    private void writeDecimalValue(final Object o, final int jdbcType, final int reqScale)
            throws TdsException, java.io.IOException {
        final byte prec = conn.getMaxPrecision();
        final byte maxLen = (prec <= 28) ? (byte)13 : (byte)17;

        comm.appendByte(SYBDECIMAL);

        if (jdbcType == Types.BIGINT) {
            comm.appendByte(maxLen);
            comm.appendByte(prec);
            comm.appendByte((byte) 0);

            if (o == null) {
                comm.appendByte((byte) 0);
            } else {
                comm.appendByte((byte) 9);

                long value = ((Number) o).longValue();

                if (value >= 0L) {
                    comm.appendByte((byte) 1);
                } else {
                    comm.appendByte((byte) 0);
                    value = -value;
                }

                for (int valueidx = 0; valueidx < 8; valueidx++) {
                    // comm.appendByte((byte)(value & 0xFF));
                    comm.appendByte((byte) (value & 0xFF));
                    value >>>= 8;
                }
            }
        } else {
            comm.appendByte(maxLen);
            comm.appendByte(prec);

            if (o == null) {
                comm.appendByte((byte) ((reqScale == -1) ? 0 : reqScale));
                comm.appendByte((byte) 0);
            } else {
                BigDecimal bd;

                if (o instanceof BigDecimal) {
                    bd = (BigDecimal) o;
                } else if (o instanceof Number) {
                    bd = new BigDecimal(((Number) o).doubleValue());
                } else if (o instanceof Boolean) {
                    bd = new BigDecimal(((Boolean) o).booleanValue() ? 1 : 0);
                } else {
                    bd = new BigDecimal(o.toString());
                }

                if (reqScale != -1) {
                    bd = bd.setScale(reqScale, BigDecimal.ROUND_HALF_UP);
                }
                if (bd.scale() > prec) {
                    bd = bd.setScale(prec, BigDecimal.ROUND_HALF_UP);
                }

                byte scale;
                final byte signum = (byte) (bd.signum() < 0 ? 0 : 1);
                byte[] mantisse;
                byte len;
                do {
                    scale = (byte) bd.scale();
                    final BigInteger bi = bd.unscaledValue();
                    mantisse = bi.abs().toByteArray();
                    len = (byte) (mantisse.length + 1);

                    if (len > maxLen) {   // diminish scale as long as len is to much
                        final int dif = len - maxLen;
                        scale -= dif * 2;
                        if (scale < 0) {
                            throw new TdsException("can't sent this BigDecimal");
                        }
                        bd = bd.setScale(scale, BigDecimal.ROUND_HALF_UP);
                        continue;
                    } else
                        break;
                } while (true);

                comm.appendByte(scale);
                comm.appendByte(len);
                comm.appendByte(signum);

                for (int i = mantisse.length - 1; i >= 0; i--) {
                    comm.appendByte(mantisse[i]);
                }
            }
        }
    }

    private Object readFloatN(final int len)
            throws TdsException, java.io.IOException {
        switch (len) {
        case 8:
            final long l = comm.getTdsInt64();
            return new Double(Double.longBitsToDouble(l));

        case 4:
            final int i = comm.getTdsInt();
            return new Float(Float.intBitsToFloat(i));

        case 0:
            return null;

        default:
            throw new TdsNotImplemented(
                    "Don't now how to handle float with size of " + len
                    + "(0x" + Integer.toHexString(len & 0xff) + ")");
        }
    }

    private void sendSybImage(final java.io.InputStream value, int length)
            throws java.io.IOException {
        if (value == null) {
            comm.appendByte(SYBVARBINARY);
            comm.appendByte((byte) 255);
            comm.appendByte((byte) 0);
        } else {
            comm.appendByte(SYBIMAGE);
    
            // send the length of this piece of data
            comm.appendTdsInt(Integer.MAX_VALUE);
    
            // send the length of this piece of data again
            comm.appendTdsInt(length);
    
            if (value != null) {
                byte[] buffer = new byte[1024];
    
                for (int i = 0; i < length; i += buffer.length) {
                    int result = value.read(buffer);
    
                    // TODO The connection should be closed if this happens
                    if (result == -1 && i < length) {
                        throw new java.io.IOException(
                                "Data in stream less than specified by length");
                    } else if (i + result > length) {
                        throw new java.io.IOException(
                                "More data in stream than specified with length");
                    }
    
                    comm.appendBytes(buffer, result, (byte) 0);
                }
            }
        }
    }

    private void sendSybText(final java.io.Reader value, int length,
                             boolean isUnicode)
            throws java.io.IOException, SQLException {
        if (value == null) {
            sendSybChar(null, 255);
        } else {
            EncodingHelper encodingHelper = conn.getEncoder();

            if (encodingHelper.isDBCS()) {
                // Is this acceptable? Multi-byte character sets may send more
                // bytes to the database than the number of characters.
                // How does SQL Server/Sybase treat this length value?
                // Is it the maximum amount of data that is expected or the
                // exact length of the data expected or???
                //
                // SAfe I'm sure the length must be the length of the actual
                //      data, which is impossible to figure out like this, so
                //      we should just send it as Unicode and let the server
                //      handle it
                isUnicode = true;
            }

            comm.appendByte(isUnicode ? SYBNTEXT : SYBTEXT);

            // send the length of this piece of data
            comm.appendTdsInt(Integer.MAX_VALUE);

            // send the length of this piece of data again
            comm.appendTdsInt(isUnicode ? 2 * length : length);

            char[] buffer = new char[1024];

            for (int i = 0; i < length; i += buffer.length) {
                int result = value.read(buffer);

                // TODO The connection should be closed if this happens
                if (result == -1 && i < length) {
                    throw new java.io.IOException(
                            "Data in stream less than specified by length");
                } else if (i + result > length) {
                    throw new java.io.IOException(
                            "More data in stream than specified with length");
                }

                if (isUnicode) {
                    comm.appendChars(buffer, result);
                } else {
                    comm.appendBytes(encodingHelper.getBytes(new String(buffer, 0, result)));
                }
            }
        }
    }

    private void sendSybChar(final String value, final int maxLength)
            throws java.io.IOException, SQLException {
        if (value == null) {
            comm.appendByte(SYBVARCHAR);
            comm.appendByte((byte) 255);
            comm.appendByte((byte) 0);
        } else {
            final byte[] converted = conn.getEncoder().getBytes(value);

            // set the type of the column
            // set the maximum length of the field
            // set the actual lenght of the field.
            if (tdsVer >= TDS70) {
                comm.appendByte((byte) (SYBVARCHAR | 0x80));
                comm.appendTdsShort((short) maxLength);
                comm.appendTdsShort((short) converted.length);
            } else {
                if (maxLength > 255 || converted.length > 255) {
                    throw new SQLException(
                            "String or binary data would be truncated.",
                            "22001", 8152);
                }

                comm.appendByte(SYBVARCHAR);
                comm.appendByte((byte) maxLength);

                if (converted.length != 0) {
                    comm.appendByte((byte) converted.length);
                } else {
                    comm.appendByte((byte) 1);
                    comm.appendByte((byte) 32);
                }
            }

            comm.appendBytes(converted);
        }
    }


    /**
     * Process a login ACK subpacket.
     *
     * @exception TdsException
     * @exception java.io.IOException
     */
    private PacketResult processLoginAck()
            throws TdsException, java.io.IOException {
        String product;
        int major, minor, build = 0;
        getSubPacketLength(); // Packet length

        comm.skip(5);
        product = comm.getString(comm.getByte() & 0xff, conn.getEncoder());
        if (product.length() > 1 && -1 != product.indexOf('\0')) {
            product = product.substring(0, product.indexOf('\0'));
        }

        if (tdsVer < Tds.TDS70) {
            comm.skip(1);
        }
        major = comm.getByte() & 0xff;
        minor = comm.getByte() & 0xff;

        if (tdsVer == Tds.TDS70) {
            build = comm.getNetShort();
        } else {
            comm.skip(1);
        }

        conn.setDBServerInfo(product, major, minor, build);

        return new PacketResult(TDS_LOGINACK);
    }

    /**
     * Process an proc id subpacket.
     * <p>
     * This routine assumes that the TDS_PROCID byte has already been read.
     *
     * @exception TdsException
     * @exception java.io.IOException thrown if some sort of error occured
     *                                reading bytes from the network.
     */
    private PacketResult processProcId() throws java.io.IOException, TdsException {
        // XXX Try to find out what meaning this subpacket has.
        comm.skip(8);
        return new PacketResult(TDS_PROCID);
    }

    /**
     * Process a TDS_RET_STAT_TOKEN subpacket.
     * <p>
     * This routine assumes that the TDS_RET_STAT_TOKEN byte has already been
     * read.
     *
     * @exception TdsException
     * @exception java.io.IOException thrown if some sort of error occured
     *                                reading bytes from the network.
     */
    private PacketRetStatResult processRetStat()
            throws java.io.IOException, TdsException {
        // XXX Not completely sure of this.
        return new PacketRetStatResult(comm.getTdsInt());
    }

    /**
     * Processes a TDS 7.0-style result packet, extracting column information
     * for the result set. Added 2000-06-05.
     *
     * @exception java.io.IOException
     * @exception TdsException
     */
    private PacketResult processTds7Result()
            throws java.io.IOException, TdsException {
        EncodingHelper encoder = conn.getEncoder();
        final int numColumns = comm.getTdsShort();
        final Columns columns = new Columns(numColumns);

        for (int colNum = 1; colNum <= numColumns; ++colNum) {

            /*
             * The freetds C code didn't know what to do with these four
             * bytes, but initial inspection appears to tentatively confirm
             * that they serve the same purpose as the flag bytes read by the
             * Java code for 4.2 column information.
             */
            final byte[] flagData = new byte[4];
            for (int i = 0; i < 4; i++) {
                flagData[i] = comm.getByte();
            }
            final boolean nullable = (flagData[2] & 0x01) > 0;
            final boolean caseSensitive = (flagData[2] & 0x02) > 0;
            final boolean writable = (flagData[2] & 0x0C) > 0;
            final boolean autoIncrement = (flagData[2] & 0x10) > 0;

            /*
             * Get the type of column.  Large types have 2-byte size fields and
             * type codes OR'd with 0x80.  Except SYBNCHAR, whose type code
             * is already above 0x80.
             */
            int columnType = comm.getByte() & 0xFF;
            if (columnType == 0xEF) {
                columnType = SYBNCHAR;
            }
            int xColType = -1;
            if (isLargeType(columnType)) {
                xColType = columnType;
                if (columnType != SYBNCHAR) {
                    columnType -= 128;
                }
            }

            // Determine the column size.
            final int dispSize = -1;
            final int bufLength;
            String tableName = "";
            if (isBlobType(columnType)) {

                // Text and image columns have 4-byte size fields.
                bufLength = comm.getTdsInt();

                // Get table name.
                tableName = comm.getString(comm.getTdsShort(), encoder);
            }
            // Fixed types have no size field in the packet.
            else if (isFixedSizeColumn((byte) columnType)) {
                bufLength = lookupBufferSize((byte) columnType);
            } else if (isLargeType(xColType)) {
                bufLength = comm.getTdsShort();
            } else {
                bufLength = comm.getByte() & 0xff;
            }

            // Get precision, scale for decimal types.
            int precision = -1;
            int scale = -1;
            if (columnType == SYBDECIMAL || columnType == SYBNUMERIC) {
                precision = comm.getByte(); // No need to & with 0xff
                scale = comm.getByte(); // No need to & with 0xff
            }

            /*
             * NB: under 7.0 lengths are number of characters, not number of
             * bytes.  The getString() method handles this.
             */
            final int colNameLen = comm.getByte() & 0xff;
            final String columnName = comm.getString(colNameLen, encoder);

            // Populate the Column object.
            populateColumn(columns.getColumn(colNum), columnType, columnName,
                           dispSize, bufLength, nullable, autoIncrement, writable,
                           caseSensitive, tableName, precision, scale);
        }

        currentContext = new Context(columns, encoder);

        return new PacketColumnNamesResult(columns);
    }

    private static void populateColumn(
            final Column col, final int columnType, final String columnName, int dispSize,
            final int bufLength, final boolean nullable, final boolean autoIncrement,
            final boolean writable, final boolean caseSensitive, String tableName,
            int precision, final int scale) {
        if (columnName != null) {
            col.setName(columnName);
            col.setLabel(columnName);
        }

        col.setType(columnType);
        col.setBufferSize(bufLength);
        col.setNullable(nullable
                        ? ResultSetMetaData.columnNullable
                        : ResultSetMetaData.columnNoNulls);
        col.setAutoIncrement(autoIncrement);
        col.setReadOnly(!writable);
        col.setCaseSensitive(caseSensitive);

        // Set table name (and catalog and schema, if available)
        if (tableName != null) {
            int pos = tableName.lastIndexOf('.');
            col.setTableName(tableName.substring(pos + 1));
            pos = pos == -1 ? 0 : pos;
            tableName = tableName.substring(0, pos);
            if (pos > 0) {
                pos = tableName.lastIndexOf('.');
                col.setSchema(tableName.substring(pos + 1));
                pos = pos == -1 ? 0 : pos;
                tableName = tableName.substring(0, pos);
            }
            if (pos > 0) {
                col.setCatalog(tableName);
            }
        }

        // Set scale
        switch (columnType) {
        case SYBMONEY:
        case SYBMONEYN:
        case SYBMONEY4:
        case SYBSMALLMONEY:
            col.setScale(4);
            break;

        case SYBDATETIME:
            col.setScale(3);
            break;

        case SYBDATETIMN:
            if (bufLength == 8)
                col.setScale(3);
            else
                col.setScale(0);
            break;

        default:
            col.setScale(scale < 0 ? 0 : scale);
        }

        // Set precision and display size
        switch (columnType) {
        case SYBBINARY:
        case SYBIMAGE:
        case SYBVARBINARY:
            dispSize = 2 * (precision = bufLength);
            break;

        case SYBCHAR:
        case SYBTEXT:
        case SYBVARCHAR:
            dispSize = precision = bufLength;
            break;

        case SYBNCHAR:
        case SYBNTEXT:
        case SYBNVARCHAR:
            dispSize = precision = bufLength >> 1;
            break;

        case SYBBIT:
            dispSize = precision = 1;
            break;

        case SYBUNIQUEID:
            dispSize = precision = 36;
            break;

        case SYBDATETIME:
            dispSize = precision = 23;
            break;

        case SYBDATETIME4:
            dispSize = 3 + (precision = 16);
            break;

        case SYBDATETIMN:
            if (bufLength == 8)
                dispSize = precision = 23;
            else
                dispSize = 3 + (precision = 16);
            break;

        case SYBDECIMAL:
        case SYBNUMERIC:
            dispSize = (bufLength == scale ? 3 : 2) + precision;
            break;

        case SYBFLT8:
            dispSize = 9 + (precision = 15);
            break;

        case SYBREAL:
            dispSize = 7 + (precision = 7);
            break;

        case SYBFLTN:
            if (bufLength == 8)
                dispSize = 9 + (precision = 15);
            else
                dispSize = 7 + (precision = 7);
            break;

        case SYBINT4:
            dispSize = 1 + (precision = 10);
            break;

        case SYBINT2:
            dispSize = 1 + (precision = 5);
            break;

        case SYBINT1:
            dispSize = 1 + (precision = 2);
            break;

        case SYBINTN:
            if (bufLength == 4)
                dispSize = 1 + (precision = 10);
            else if (bufLength == 2)
                dispSize = 1 + (precision = 5);
            else
                dispSize = 1 + (precision = 2);
            break;

        case SYBMONEY:
            dispSize = 2 + (precision = 19);
            break;

        case SYBMONEY4:
        case SYBSMALLMONEY:
            dispSize = 2 + (precision = 10);
            break;

        case SYBMONEYN:
            if (bufLength == 8)
                dispSize = 2 + (precision = 19);
            else
                dispSize = 2 + (precision = 10);
            break;
        }
        col.setDisplaySize(dispSize);
        col.setPrecision(precision);
    }

    private void waitForDataOrTimeout(final SQLWarningChain wChain, final int timeout) throws java.io.IOException, TdsException {
        // SAfe No synchronization needed, this is only used internally so it's
        //      already synched.
        if (timeout == 0 || wChain == null)
            comm.peek();
        else {
            // MJH - 06/02/04 Use the socket time out option
            // rather than the timeout handler.
            sock.setSoTimeout(comm, timeout * 1000);
            try {
                comm.peek();
            } catch (TdsTimeoutException e) {
                moreResults = false;
                wChain.addException(new SQLException("Query has timed out.", "HYT00"));
            } finally {
                sock.setSoTimeout(comm, 0); // restore default no time out
            }
        }
    }

    /**
     * Convert a JDBC java.sql.Types identifier to a SQLServer type identifier.
     *
     * @param  jdbcType JDBC type to convert. Should be one of the constants
     *                  from java.sql.Types.
     * @return          The corresponding SQLServer type identifier.
     * @exception  TdsNotImplemented
     */
    public static byte cvtJdbcTypeToNativeType(final int jdbcType)
            throws TdsNotImplemented {
        // This function is thread safe.
        switch (jdbcType) {
        case java.sql.Types.CHAR:
        case java.sql.Types.VARCHAR:
            return SYBCHAR;

        case java.sql.Types.LONGVARCHAR:
        case java.sql.Types.CLOB:
            return SYBTEXT;

        case java.sql.Types.TINYINT:
            return SYBINT1;

        case java.sql.Types.SMALLINT:
            return SYBINT2;

        case java.sql.Types.INTEGER:
            return SYBINT4;

        case java.sql.Types.BIT:
        case java.sql.Types.BOOLEAN:
            return SYBBIT;

        case java.sql.Types.FLOAT:
        case java.sql.Types.REAL:
        case java.sql.Types.DOUBLE:
            return SYBFLT8;

        case java.sql.Types.DATE:
        case java.sql.Types.TIME:
        case java.sql.Types.TIMESTAMP:
            return SYBDATETIMN;

        case java.sql.Types.BINARY:
            return SYBBINARY;

        case java.sql.Types.VARBINARY:
            return SYBVARBINARY;

        case java.sql.Types.BLOB:
        case java.sql.Types.LONGVARBINARY:
            return SYBIMAGE;

        case java.sql.Types.BIGINT:
            return SYBDECIMAL;

        case java.sql.Types.NUMERIC:
            return SYBNUMERIC;

        case java.sql.Types.DECIMAL:
            return SYBDECIMAL;

        default:
            throw new TdsNotImplemented("cvtJdbcTypeToNativeType ("
                                        + TdsUtil.javaSqlTypeToString(jdbcType) + ")");
        }
    }

    /**
     * Convert a JDBC <code>java.sql.Types</code> identifier to a SQL Server
     * type identifier
     *
     * @param  nativeType SQL Server type to convert.
     * @param  size       maximum size of data coming back from server.
     * @return            yhe corresponding JDBC type identifier.
     * @exception  TdsException
     */
    public static int cvtNativeTypeToJdbcType(final int nativeType, final int size)
            throws TdsException {
        // This function is thread safe.

        switch (nativeType) {
        // XXX We need to figure out how to map _all_ of these types
        case SYBBINARY:
            return java.sql.Types.BINARY;

        case SYBBIT:
            return java.sql.Types.BIT;

        case SYBBITN:
            return java.sql.Types.BIT;

        case SYBCHAR:
            return java.sql.Types.CHAR;

        case SYBNCHAR:
            return java.sql.Types.CHAR;

        case SYBDATETIME4:
            return java.sql.Types.TIMESTAMP;

        case SYBDATETIME:
            return java.sql.Types.TIMESTAMP;

        case SYBDATETIMN:
            return java.sql.Types.TIMESTAMP;

        case SYBDECIMAL:
            return java.sql.Types.DECIMAL;

        case SYBNUMERIC:
            return java.sql.Types.NUMERIC;

        case SYBFLT8:
            return java.sql.Types.FLOAT;

        case SYBFLTN:
            switch (size) {
            case 4:
                return java.sql.Types.REAL;
            case 8:
                return java.sql.Types.FLOAT;
            default:
                throw new TdsException("Bad size of SYBFLTN");
            }

        case SYBINT1:
            return java.sql.Types.TINYINT;

        case SYBINT2:
            return java.sql.Types.SMALLINT;

        case SYBINT4:
            return java.sql.Types.INTEGER;

        case SYBINTN:
            switch (size) {
            case 1:
                return java.sql.Types.TINYINT;
            case 2:
                return java.sql.Types.SMALLINT;
            case 4:
                return java.sql.Types.INTEGER;
            default:
                throw new TdsException("Bad size of SYBINTN");
            }

        case SYBSMALLMONEY:
            return java.sql.Types.DECIMAL;

        case SYBMONEY4:
            return java.sql.Types.DECIMAL;

        case SYBMONEY:
            return java.sql.Types.DECIMAL;

        case SYBMONEYN:
            return java.sql.Types.DECIMAL;

        case SYBREAL:
            return java.sql.Types.REAL;

        case SYBTEXT:
            return java.sql.Types.CLOB;

        case SYBNTEXT:
            return java.sql.Types.CLOB;

        case SYBIMAGE:
            return java.sql.Types.BLOB;

        case SYBVARBINARY:
            return java.sql.Types.VARBINARY;

        case SYBVARCHAR:
            return java.sql.Types.VARCHAR;

        case SYBNVARCHAR:
            return java.sql.Types.VARCHAR;

        case SYBUNIQUEID:
            return java.sql.Types.VARCHAR;

        default:
            throw new TdsException("Unknown native data type "
                                   + Integer.toHexString(nativeType & 0xff));
        }
    }

    /**
     * Reports whether the type is for a large object. Name is a bit of a
     * misnomer, since it returns true for large text types, not just binary
     * objects (took it over from the freetds C code).
     *
     * @param type SQL Server type
     * @return     <code>true</code> if the type is a LOB
     */
    private static boolean isBlobType(final int type) {
        return type == SYBTEXT || type == SYBIMAGE || type == SYBNTEXT;
    }

    /**
     * Reports whether the type uses a 2-byte size value.
     *
     * @param type SQL Server type
     * @return     <code>true</code> if the type uses a 2-byte size value
     */
    private static boolean isLargeType(final int type) {
        return type == SYBNCHAR || type > 128;
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

    /**
     * All procedures of the current transaction were submitted.
     */
    synchronized void commit() throws SQLException {
        // SAfe Consume all outstanding packets first
        skipToEnd();

        submitProcedure("IF @@TRANCOUNT>0 COMMIT TRAN", new SQLWarningChain());
    }

    /**
     * All procedures of the current transaction were rolled back.
     */
    synchronized void rollback() throws SQLException {
        // SAfe Consume all outstanding packets first
        skipToEnd();

        submitProcedure("IF @@TRANCOUNT>0 ROLLBACK TRAN", new SQLWarningChain());
    }

    // SAfe This method is *completely* unsafe (although I was the brilliant
    //      mind who wrote it :o( ). It will have to wait until the Tds will
    //      manage its own Context; that way it will know exactly how to deal
    //      with packages. Anyway, it seems a little bit useless. Or not?
    // SAfe No longer true! The method should work just fine, now that we have
    //      a Tds-managed Context. :o)
    synchronized void skipToEnd() throws java.sql.SQLException {
        try {
            while (moreResults) {
                processSubPacket();
            }
        } catch (Exception e) {
            if (e instanceof SQLException) {
                throw (SQLException) e;
            }

            throw TdsUtil.getSQLException("Error occured while consuming output", "HY000", e);
        }
    }

    private String sqlStatementToInitialize() throws SQLException {
        final StringBuffer statement = new StringBuffer(150);

        statement.append("SET QUOTED_IDENTIFIER ON ");

        if (serverType == Tds.SYBASE) {
            statement.append("SET TEXTSIZE 50000 ");
        } else {
            // Options set per Microsoft ODBC/ANSI-92 recommendations:
            // http://msdn.microsoft.com/library/default.asp?url=/library/en-us/odbcsql/od_6_015_0tf7.asp
            statement.append("SET TEXTSIZE 2147483647 ");
//            statement.append("SET ANSI_DEFAULTS ON ");
//            statement.append("SET CURSOR_CLOSE_ON_COMMIT OFF ");
        }

        // SAfe We also have to add these until we find out how to put them in
        //      the login packet (if that is possible at all)
        sqlStatementToSetTransactionIsolationLevel(statement, this.conn.getTransactionIsolation());
        statement.append(' ');
        sqlStatementToSetCommit(statement, this.conn.getAutoCommit());

        return statement.toString();
    }

    private void sqlStatementToSetTransactionIsolationLevel(
            final StringBuffer sql, int transactionIsolation)
            throws SQLException {
        sql.append("SET TRANSACTION ISOLATION LEVEL ");

        switch (transactionIsolation) {
        case Connection.TRANSACTION_READ_UNCOMMITTED:
            sql.append("READ UNCOMMITTED");
            break;

        case Connection.TRANSACTION_READ_COMMITTED:
            sql.append("READ COMMITTED");
            break;

        case Connection.TRANSACTION_REPEATABLE_READ:
            sql.append("REPEATABLE READ");
            break;

        case Connection.TRANSACTION_SERIALIZABLE:
            sql.append("SERIALIZABLE");
            break;

        case Connection.TRANSACTION_NONE:
        default:
            throw new SQLException("Bad transaction level", "HY024");
        }
    }

    private void sqlStatementToSetCommit(
            final StringBuffer sql, boolean autoCommit) {
        if (serverType == Tds.SYBASE) {
            if (autoCommit) {
                sql.append("SET CHAINED OFF");
            } else {
                sql.append("SET CHAINED ON");
            }
        } else {
            if (autoCommit) {
                sql.append("SET IMPLICIT_TRANSACTIONS OFF");
            } else {
                sql.append("SET IMPLICIT_TRANSACTIONS ON");
            }
        }
    }

    private String sqlStatementForSettings(
            final boolean autoCommit, final int transactionIsolationLevel) throws SQLException {
        final StringBuffer res = new StringBuffer(64);
        // SAfe The javadoc for setAutoCommit states that "if the method is
        //      called during a transaction, the transaction is committed"
        // @todo SAfe Only execute this when setAutoCommit is called, not for
        //       setTransactionIsolationLevel
        res.append("IF @@TRANCOUNT>0 COMMIT TRAN ");

        if (autoCommit != this.conn.getAutoCommit()) {
            sqlStatementToSetCommit(res, autoCommit);
            res.append(' ');
        }

        if (transactionIsolationLevel != this.conn.getTransactionIsolation()) {
            sqlStatementToSetTransactionIsolationLevel(res, transactionIsolationLevel);
            res.append(' ');
        }

        res.setLength(res.length() - 1);
        return res.toString();
    }

    protected synchronized void changeSettings(
            final boolean autoCommit, final int transactionIsolationLevel)
            throws SQLException {
        final String query = sqlStatementForSettings(autoCommit, transactionIsolationLevel);

        if (query != null) {
            try {
                skipToEnd();
                changeSettings(query);
            } catch (SQLException e) {
                throw e;
            } catch (Exception e) {
                throw TdsUtil.getSQLException(null, "HY000", e);
            }
        }
    }

    private void changeSettings(final String query) throws SQLException {
        if (query.length() == 0) {
            return;
        }

        final SQLWarningChain wChain = new SQLWarningChain();
        submitProcedure(query, wChain);
        wChain.checkForExceptions();
    }

    /**
     * Process an NTLM challenge.
     * <p>
     * Added by mdb to handle NT authentication to MS SQL Server.
     */
    private PacketResult processNtlmChallenge()
            throws TdsException, java.io.IOException {
        final int packetLength = getSubPacketLength(); // Packet length

        final int headerLength = 40;

        if (packetLength < headerLength)
            throw new TdsException("NTLM challenge: packet is too small:" + packetLength);

        comm.skip(8);  //header "NTLMSSP\0"
        final int seq = comm.getTdsInt(); //sequence number (2)
        if (seq != 2)
            throw new TdsException("NTLM challenge: got unexpected sequence number:" + seq);
        comm.skip(4); //domain length (repeated 2x)
        comm.skip(4); //domain offset
        comm.skip(4); //flags
        final byte[] nonce = comm.getBytes(8, true); // this is what we really care about
        comm.skip(8); //?? unknown

        //skip the end, which may include the domain name, among other things...
        comm.skip(packetLength - headerLength);

        return new PacketAuthTokenResult(nonce);
    }

    private void sendNtlmChallengeResponse(final PacketAuthTokenResult authToken,
                                           final String user,
                                           final String password,
                                           final String domain)
            throws TdsException, java.io.IOException {
        try {
            comm.startPacket(TdsComm.NTLMAUTH);

            // host and domain name are _narrow_ strings.
            //byte[] domainBytes = domain.getBytes("UTF8");
            //byte[] user        = user.getBytes("UTF8");

            final byte[] header = {0x4e, 0x54, 0x4c, 0x4d, 0x53, 0x53, 0x50, 0x00};
            comm.appendBytes(header); //header is ascii "NTLMSSP\0"
            comm.appendTdsInt(3);          //sequence number = 3

            final int domainLenInBytes = domain.length() * 2;
            final int userLenInBytes = user.length() * 2;
            //mdb: not sending hostname; I hope this is ok!
            final int hostLenInBytes = 0; //localhostname.length()*2;

            int pos = 64 + domainLenInBytes + userLenInBytes + hostLenInBytes;

            // lan man response: length and offset
            comm.appendTdsShort((short) 24);
            comm.appendTdsShort((short) 24);
            comm.appendTdsInt(pos);
            pos += 24;

            // nt response: length and offset
            comm.appendTdsShort((short) 24);
            comm.appendTdsShort((short) 24);
            comm.appendTdsInt(pos);

            pos = 64;

            //domain
            comm.appendTdsShort((short) domainLenInBytes);
            comm.appendTdsShort((short) domainLenInBytes);
            comm.appendTdsInt(pos);
            pos += domainLenInBytes;

            //user
            comm.appendTdsShort((short) userLenInBytes);
            comm.appendTdsShort((short) userLenInBytes);
            comm.appendTdsInt(pos);
            pos += userLenInBytes;

            //local hostname
            comm.appendTdsShort((short) hostLenInBytes);
            comm.appendTdsShort((short) hostLenInBytes);
            comm.appendTdsInt(pos);
            pos += hostLenInBytes;

            //unknown
            comm.appendTdsShort((short) 0);
            comm.appendTdsShort((short) 0);
            comm.appendTdsInt(pos);

            //flags
            comm.appendTdsInt(0x8201);     //flags (???)

            //variable length stuff...
            comm.appendChars(domain);
            comm.appendChars(user);
            //Not sending hostname...I hope this is OK!
            //comm.appendChars(localhostname);

            //the response to the challenge...
            final byte[] lmAnswer = NtlmAuth.answerLmChallenge(password, authToken.getNonce());
            final byte[] ntAnswer = NtlmAuth.answerNtChallenge(password, authToken.getNonce());
            comm.appendBytes(lmAnswer);
            comm.appendBytes(ntAnswer);

            comm.sendPacket();
        } finally {
            comm.packetType = 0;
        }
    }

    public Context getContext() {
        return currentContext;
    }

    public boolean useUnicode() {
        return useUnicode;
    }

    synchronized void startResultSet(final SQLWarningChain wChain,
                                     final TdsStatement stmt) {
        try {
            while (!isResultRow() && !isEndOfResults()) {
                final PacketResult res = processSubPacket();
                if (res instanceof PacketMsgResult) {
                    wChain.addOrReturn((PacketMsgResult) res);
                } else if (res instanceof PacketOutputParamResult) {
                    stmt.handleParamResult((PacketOutputParamResult) res);
                } else if (res instanceof PacketRetStatResult) {
                    stmt.handleRetStat((PacketRetStatResult) res);
                }
            }
        } catch (java.io.IOException e) {
            wChain.addException(TdsUtil.getSQLException("Network problem", "08S01", e));
        } catch (TdsUnknownPacketSubType e) {
            wChain.addException(TdsUtil.getSQLException("Unknown response", "HY000", e));
        } catch (TdsException e) {
            wChain.addException(TdsUtil.getSQLException(null, "HY000", e));
        } catch (SQLException e) {
            wChain.addException(e);
        }
    }

    synchronized PacketRowResult fetchRow(final TdsStatement stmt,
                                          final SQLWarningChain wChain,
                                          final Context context) {
        PacketResult res;

        try {
            // Keep eating garbage and warnings until we reach the next result
            while (true) {
                res = processSubPacket(context);

                switch (res.getPacketType()) {
                case TDS_ROW:
                    return (PacketRowResult) res;

                case TDS_COL_NAME_TOKEN:
                case TDS_COLMETADATA:
                    // SAfe This shouldn't really happen
                    wChain.addException(
                            new SQLException("Unexpected packet type.", "HY000"));
                    break;

                case TDS_DONE:
                case TDS_DONEINPROC:
                case TDS_DONEPROC:
                    handleEndToken(res, wChain);
                    goToNextResult(wChain, stmt);
                    return null;

                case TDS_PROCID:
                    // SAfe Find out what this packet means
                    break;

                case TDS_RETURNSTATUS:
                    stmt.handleRetStat((PacketRetStatResult) res);
                    break;

                case TDS_PARAM:
                    stmt.handleParamResult((PacketOutputParamResult) res);
                    break;

                case TDS_INFO:
                case TDS_ERROR:
                    wChain.addOrReturn((PacketMsgResult) res);
                    break;

                default:
                    wChain.addException(new SQLException(
                            "Protocol confusion. Got a 0x"
                            + Integer.toHexString((res.getPacketType() & 0xff))
                            + " packet", "HY000"));
                    break;
                }
            }
        } catch (java.io.IOException e) {
            wChain.addException(TdsUtil.getSQLException("Network problem", "08S01", e));
        } catch (TdsUnknownPacketSubType e) {
            wChain.addException(TdsUtil.getSQLException("Unknown response", "HY000", e));
        } catch (TdsException e) {
            wChain.addException(TdsUtil.getSQLException(null, "HY000", e));
        } catch (SQLException e) {
            wChain.addException(e);
        }

        return null;
    }

    /**
     * Handle a <code>TDS_DONE</code>, <code>TDS_DONEPROC</code> or
     * <code>TDS_DONEINPROC</code> packet, checking for the cancel and error
     * flags.
     *
     * @param res    a <code>PacketEndTokenResult</code> instance
     * @param wChain <code>SQLWarningChain</code> to add any exceptions to
     * @return       <code>true</code> if an exception was generated
     */
    static boolean handleEndToken(PacketResult res, SQLWarningChain wChain) {
        PacketEndTokenResult pack = (PacketEndTokenResult) res;

        if (pack.wasCanceled()) {
            wChain.addException(
                    new SQLException("Query was cancelled or timed out.", "HYT00"));
            return true;
        }

        if (pack.wasError()) {
            wChain.addException(
                    new SQLException("Unspecified error returned.", "HY000"));
            return true;
        }

        return false;
    }

    private static byte[] getMACAddress(String macString) {
        byte[] mac = new byte[6];
        boolean ok = false;

        if (macString != null && macString.length() == 12) {
            try {
                for (int i=0, j=0; i<6; i++, j+=2) {
                    mac[i] = (byte) Integer.parseInt(
                            macString.substring(j, j+2), 16);
                }
                ok = true;
            } catch (Exception e) {
                // Ignore it. ok will be false.
            }
        }

        if (!ok) {
            Arrays.fill(mac, (byte)0);
        }

        return mac;
    }
}
