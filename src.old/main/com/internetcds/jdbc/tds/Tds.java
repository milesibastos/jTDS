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




package com.internetcds.jdbc.tds;

import java.net.Socket;
import java.util.Vector;
import java.lang.Thread;
import java.util.StringTokenizer;
import java.sql.*;
import com.internetcds.jdbc.tds.TdsComm;
import com.internetcds.util.Logger;
import java.math.BigInteger;
import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Properties;
import java.util.TimeZone;
import java.util.Locale;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Iterator;

/**
 *  Cancel the current SQL if the timeout expires.
 *
 *@author     Craig Spannring
 *@created    March 17, 2001
 *@version    $Id: Tds.java,v 1.11 2001-09-25 15:20:19 aschoerk Exp $
 */
class TimeoutHandler extends Thread {

    java.sql.Statement stmt;
    int timeout;
    boolean dataReceived;
    /**
     *  Description of the Field
     */
    public final static String cvsVersion = "$Id: Tds.java,v 1.11 2001-09-25 15:20:19 aschoerk Exp $";


    public TimeoutHandler(
            java.sql.Statement stmt_,
            int timeout_)
    {
        stmt = stmt_;
        timeout = timeout_;
    }


    public void run()
    {
        try {
            sleep( timeout * 1000 );
            stmt.cancel();
        }
        catch (SQLException e) {
            // nop
        }
        catch (java.lang.InterruptedException e) {
            // nop
        }
    }

}


/**
 *  Implement the TDS protocol.
 *
 *@author     Craig Spannring
 *@author     Igor Petrovski
 *@author     The FreeTDS project
 *@created    March 17, 2001
 *@version    $Id: Tds.java,v 1.11 2001-09-25 15:20:19 aschoerk Exp $
 */
public class Tds implements TdsDefinitions {


    Socket sock = null;
    TdsComm comm = null;

    String databaseProductName;
    String databaseProductVersion;

    java.sql.Connection connection;
    String host;
    int serverType = -1;
    // either Tds.SYBASE or Tds.SQLSERVER
    int port;
    // Port numbers are _unsigned_ 16 bit, short is too small
    String database;
    String user;
    String password;
    String appName;
    String serverName;
    String progName;
    byte progMajorVersion;
    byte progMinorVersion;

    boolean haveProcNameTable = false;
    String procNameGeneratorName = null;
    String procNameTableName = null;

    String initialSettings = null;
    HashMap             procedureCache = null; // XXX: as
    ArrayList           proceduresOfTra = null;

    CancelController cancelController = null;

    SqlMessage lastServerMessage = null;

    private Properties initialProps = null;
    private EncodingHelper encoder = null;
    private String charset = null;

    // int          rowCount    = -1;    // number of rows selected or updated
    private boolean moreResults = false;
    // Is there another result set?

    // Jens Jakobsen 1999-01-10
    // Until TDS_END_TOKEN is reached we assume that there are outstanding
    // UpdateCounts or ResultSets
    private boolean moreResults2 = true;

    // Added 2000-06-07.  Used to control TDS version-specific behavior.
    private int tdsVer = Tds.TDS42;

    // RMK 2000-06-08.
    private boolean showWarnings = false;

    // RMK 2000-06-12.  Time zone offset on client (disregarding DST).
    private int zoneOffset = Calendar.getInstance().get(Calendar.ZONE_OFFSET);

    int maxRows = 0;
    /**
     *  Description of the Field
     */
    public final static String cvsVersion = "$Id: Tds.java,v 1.11 2001-09-25 15:20:19 aschoerk Exp $";

    //
    // If the following variable is false we will consider calling
    // unimplemented methods to be an error and will raise an exception.
    // If you want to ignore any unimplemented methods, set it to
    // true.  Only do this if you know what you are doing and can tolerate
    // bogus results from the unimplemented methods.
    //
    static boolean ignoreNotImplemented = false;


    public Tds(
            java.sql.Connection connection_,
            Properties props_,
            String initialSettings)
             throws java.io.IOException, java.net.UnknownHostException,
            java.sql.SQLException, com.internetcds.jdbc.tds.TdsException
    {
        connection = (java.sql.Connection) connection_;
        initialProps = props_;

        host = props_.getProperty("HOST");
        serverType = Integer.parseInt(props_.getProperty("SERVERTYPE"));
        port = Integer.parseInt(props_.getProperty("PORT"));
        database = props_.getProperty("DBNAME");
        user = props_.getProperty("user");
        password = props_.getProperty("password");
        appName = props_.getProperty("APPNAME", "jdbclib");
        serverName = props_.getProperty("SERVERNAME", host);
        progName = props_.getProperty("PROGNAME", "java_app");
        progMajorVersion = (byte) DriverVersion.getDriverMajorVersion();
        progMinorVersion = (byte) DriverVersion.getDriverMinorVersion();
        String verString = props_.getProperty("TDS", "42");
        procedureCache = new HashMap(); // new Vector();   // XXX as
        proceduresOfTra = new ArrayList();

        // XXX This driver doesn't properly support TDS 5.0, AFAIK.
        // Added 2000-06-07.
        tdsVer = TDS42;
        // tdsVer = TDS70;
        if (verString.equals("5.0")) {
            tdsVer = Tds.TDS50;
        }
        else if (verString.equals("7.0")) {
            tdsVer = Tds.TDS70;
        }

        // RMK 2000-06-08
        if (System.getProperty("TDS_SHOW_WARNINGS") != null
                 ||
                props_.getProperty("TDS_SHOW_WARNINGS") != null) {
            showWarnings = true;
        }

        cancelController = new CancelController();

        // Send the logon packet to the server
        sock = new Socket(host, port);
        sock.setTcpNoDelay(true);
        comm = new TdsComm(sock, tdsVer);

        setCharset(props_.getProperty("CHARSET"));

        if (logon()) {
            // everything is hunky-dory.
        }
        else {
            throw new SQLException("Logon failed.  " + lastServerMessage);
        }
    }


    /*
     * cvtNativeTypeToJdbcType()
     */

    /**
     *  Return the type of server that we attempted to connect to.
     *
     *@return    TdsDefinitions.SYBASE or TdsDefinitions.SQLSERVER
     */
    public int getServerType()
    {
        return serverType;
    }


    /**
     *  Create a new and unique name for a store procedure. This routine will
     *  return a unique name for a stored procedure that will be associated with
     *  a PreparedStatement(). <p>
     *
     *  Since SQLServer supports temporary procedure names we can just use
     *  UniqueId.getUniqueId() to generate a unique (for the connection) name.
     *  <p>
     *
     *  Sybase does not support temporary procedure names so we will have to
     *  have a per user table devoted to storing user specific stored
     *  procedures. The table name will be of the form
     *  database.user.jdbc_temp_stored_proc_names. The table will be defined as
     *  <code>CREATE TABLE database.user.jdbc_temp_stored_proc_names ( id
     *  NUMERIC(10, 0) IDENTITY; session int not null; name char(29) ) <code>
     *  This routine will use that table to track names that are being used.
     *
     *@return                            The UniqueProcedureName value
     *@exception  java.sql.SQLException  Description of Exception
     */
    public String getUniqueProcedureName()
             throws java.sql.SQLException
    {
        String result = null;

        if (serverType == SYBASE) {
            if (null == procNameTableName) {
                procNameTableName = database + "." + user
                         + ".jdbc_temp_stored_proc_names";
                procNameGeneratorName = user + ".jdbc_gen_temp_sp_names";
            }

            //
            // Attempt to create the table for the stored procedure names
            // If it already exists we'll get an error, but we don't care.
            // Also create a stored procedure for generating the unique
            // names.
            //
            haveProcNameTable = createStoredProcedureNameTable();

            result = generateUniqueProcName();
        }
        else {
            result = "#jdbc#" + UniqueId.getUniqueId();
        }
        return result;
    }
    // peek()

    synchronized public byte getByte()
      throws java.io.IOException, com.internetcds.jdbc.tds.TdsException
    {
      return comm.getByte();
    } // peek()


    /**
     *  Determine if the next subpacket is a result set. <p>
     *
     *  This does not eat any input.
     *
     *@return                                            true if the next piece
     *      of data to read is a result set.
     *@exception  com.internetcds.jdbc.tds.TdsException
     *@exception  java.io.IOException
     */
    public synchronized boolean isResultSet()
             throws com.internetcds.jdbc.tds.TdsException, java.io.IOException
    {
        byte type = comm.peek();

        /*
         * XXX to support 5.0 we need to expand our view of what a result
         * set is.
         */
        return type == TDS_COL_NAME_TOKEN || type == TDS7_RESULT_TOKEN;
    }


    /**
     *  Determine if the next subpacket is a ret stat <p>
     *
     *  This does not eat any input.
     *
     *@return                                            true if the next piece
     *      of data to read is a result row.
     *@exception  com.internetcds.jdbc.tds.TdsException
     *@exception  java.io.IOException
     */
    public synchronized boolean isRetStat()
             throws com.internetcds.jdbc.tds.TdsException, java.io.IOException
    {
        byte type = comm.peek();

        return type == TDS_RET_STAT_TOKEN;
    }


    /**
     *  Determine if the next subpacket is a result row or a computed result row.
     *  <p>
     *  This does not eat any input.
     *
     *@return true if the next piece of data to read is a result row.
     *@exception  com.internetcds.jdbc.tds.TdsException
     *@exception  java.io.IOException
     */
    public synchronized boolean isResultRow()
             throws com.internetcds.jdbc.tds.TdsException, java.io.IOException
    {
        byte type = comm.peek();

        return type == TDS_ROW_TOKEN /*|| type == TDS_CMP_ROW_TOKEN*/;
    }


    /**
     *  Determine if the next subpacket is an end of result set marker. <p>
     *
     *  This does not eat any input.
     *
     *@return                                            true if the next piece
     *      of data to read is end of result set marker.
     *@exception  com.internetcds.jdbc.tds.TdsException
     *@exception  java.io.IOException
     */
    public synchronized boolean isEndOfResults()
             throws com.internetcds.jdbc.tds.TdsException, java.io.IOException
    {
        byte type = comm.peek();

        return type == TDS_END_TOKEN || type == TDS_DONEPROC || type == TDS_DONEINPROC;
    }


    /**
     *  Determine if the next subpacket is a DONEINPROC marker <p>
     *
     *  This does not eat any input.
     *
     *@return
     *@exception  com.internetcds.jdbc.tds.TdsException
     *@exception  java.io.IOException
     */
    public synchronized boolean isDoneInProc()
             throws com.internetcds.jdbc.tds.TdsException, java.io.IOException
    {
        byte type = comm.peek();

        return type == TDS_DONEINPROC;
    }


    /**
     *  Determine if the next subpacket is a message packet <p>
     *
     *  This does not eat any input.
     *
     *@return                                            true if the next piece
     *      of data to read is message
     *@exception  com.internetcds.jdbc.tds.TdsException
     *@exception  java.io.IOException
     */
    public synchronized boolean isMessagePacket()
             throws com.internetcds.jdbc.tds.TdsException, java.io.IOException
    {
        byte type = comm.peek();
        return type == TDS_MSG_TOKEN;
    }


     /**
      * Determine if the next subpacket is an output parameter from a stored proc
      * <p>
      * This does not eat any input.
      *
      * @return true if the next piece of data to read is an output parameter
      *
      * @exception com.internetcds.jdbc.tds.TdsException
      * @exception java.io.IOException
      */
     synchronized public boolean isParamResult()
        throws com.internetcds.jdbc.tds.TdsException, java.io.IOException
     {
        byte  type = comm.peek();
        return type==TDS_PARAM_TOKEN;
     }
     
    /**
     *  Determine if the next subpacket is a text update packet <p>
     *
     *  This does not eat any input.
     *
     *@return                                            true if the next piece
     *      of data to read is text update
     *@exception  com.internetcds.jdbc.tds.TdsException
     *@exception  java.io.IOException
     */
     /* strange replaced by paramToken 
    public synchronized boolean isTextUpdate()
             throws com.internetcds.jdbc.tds.TdsException, java.io.IOException
    {
        byte type = comm.peek();
        return type == TDS_TEXT_UPD_TOKEN;
    }
      */


    /**
     *  Determine if the next subpacket is an error packet <p>
     *
     *  This does not eat any input.
     *
     *@return                                            true if the next piece
     *      of data to read is an error
     *@exception  com.internetcds.jdbc.tds.TdsException
     *@exception  java.io.IOException
     */
    public synchronized boolean isErrorPacket()
             throws com.internetcds.jdbc.tds.TdsException, java.io.IOException
    {
        byte type = comm.peek();
        return type == TDS_ERR_TOKEN;
    }


    /**
     *  Determine if the next subpacket is an procid subpacket <p>
     *
     *  This does not eat any input.
     *
     *@return                                            true if the next piece
     *      of data to read is end of result set marker.
     *@exception  com.internetcds.jdbc.tds.TdsException
     *@exception  java.io.IOException
     */
    public synchronized boolean isProcId()
             throws com.internetcds.jdbc.tds.TdsException, java.io.IOException
    {
        byte type = comm.peek();

        return type == TDS_PROCID;
    }


    public void close()
    {
        comm.close();
        try {
            sock.close();
        }
        catch (java.io.IOException e) {
            // XXX should do something here
        }
    }


    public String toString()
    {
        return ""
                 + database + ", "
                 + sock.getLocalAddress() + ":" + sock.getLocalPort()
                 + " -> " + sock.getInetAddress() + ":" + sock.getPort();
    }


    /**
     *  change the connection level settings for this connection stream to the
     *  database.
     *
     *@param  database                   Description of Parameter
     *@param  settings                   Description of Parameter
     *@return                            true if the database accepted the
     *      changes, false if rejected.
     *@exception  java.sql.SQLException  Description of Exception
     */
    public synchronized boolean changeSettings(
            String database,
            String settings)
             throws java.sql.SQLException
    {
        boolean isOkay = true;
        try {
            PacketResult result;

            if (database != null) {
                isOkay = changeDB(database);
            }

            if (isOkay && (settings != null && settings.length() > 0)) {
                String query = settings;
                comm.startPacket(TdsComm.QUERY);
                if (tdsVer == Tds.TDS70) {
                    comm.appendChars(query);
                }
                else {
                    byte[] queryBytes = encoder.getBytes(query);
                    comm.appendBytes(queryBytes, queryBytes.length, (byte) 0);
                }
                moreResults2 = true;
                //JJ 1999-01-10
                comm.sendPacket();

                boolean done = false;
                while (!done) {
                    result = processSubPacket();
                    done = (result instanceof PacketEndTokenResult) &&
                            !((PacketEndTokenResult) result).moreResults();
                    if (result instanceof PacketErrorResult) {
                        isOkay = false;
                    }
                    // XXX Should really process some more types of packets.
                }
            }
        }
        catch (com.internetcds.jdbc.tds.TdsUnknownPacketSubType e) {
            throw new SQLException("Unknown response. " + e.getMessage());
        }
        catch (java.io.IOException e) {
            throw new SQLException("Network problem. " + e.getMessage());
        }
        catch (com.internetcds.jdbc.tds.TdsException e) {
            throw new SQLException(e.getMessage());
        }
        return isOkay;
    }
    // changeDB()


    public void cancel()
             throws java.io.IOException, com.internetcds.jdbc.tds.TdsException
    {
        // XXX How should this be synchronized?  What sort of deadlock
        // conditions do we need to consider?

        cancelController.doCancel(comm);
    }


    public boolean moreResults()
    {
        return moreResults2;
    }
    // getUniqueProcedureName()


    /**
     *@param  sql               Description of Parameter
     *@param  chain             Description of Parameter
     *@return                   Description of the Returned Value
     *@exception  SQLException  Description of Exception
     */
    public synchronized PacketResult submitProcedure(String sql,
            SQLWarningChain chain)
             throws SQLException
    {
        PacketResult result = null;
        PacketResult tmp = null;
        boolean okay = true;
        byte tmpByte;
        SQLException exception = null;

        try {
            executeQuery(sql, null, 0);

            tmpByte = (byte) (comm.peek() & 0xff);

            boolean done;
            do  // skip to end, why not ?
            {
               tmp = processSubPacket();
               done = (tmp instanceof PacketEndTokenResult)
                  && (! ((PacketEndTokenResult)tmp).moreResults());
            } while (! done);
           /*
            while (!((tmp = processSubPacket())
                     instanceof PacketEndTokenResult)) {
                if (tmp instanceof PacketErrorResult) {
                    result = tmp;
                    okay = false;
                    // XXX I'm sure we need to do more here.

                    //     how about throwing an Exception? --SB
                    exception = ((PacketErrorResult) tmp).getMsg().toSQLException();
                }
                else if (tmp instanceof PacketMsgResult) {
                    chain.addOrReturn((PacketMsgResult) tmp);
                }
                else {
                    throw new SQLException(
                            "Confused.  Was expecting the "
                             + "end of result, found a "
                             + tmp.getClass().getName());
                }
            }
            if (result == null) {
                result = tmp;
            }
            */
        }
        catch (java.io.IOException e) {
            throw new SQLException("Network error" + e.getMessage());
        }
        catch (com.internetcds.jdbc.tds.TdsUnknownPacketSubType e) {
            throw new SQLException(e.getMessage());
        }
        catch (com.internetcds.jdbc.tds.TdsException e) {
            throw new SQLException(e.getMessage());
        }

        if (!okay) {
            throw exception;
        }

        return result;
    }


    /**
     *  Execute a stored procedure on the SQLServer <p>
     *
     *
     *
     *@param  procedureName                              Description of
     *      Parameter
     *@param  formalParameterList                        Description of
     *      Parameter
     *@param  actualParameterList                        Description of
     *      Parameter
     *@param  stmt                                       Description of
     *      Parameter
     *@param  timeout                                    Description of
     *      Parameter
     *@exception  java.sql.SQLException
     *@exception  com.internetcds.jdbc.tds.TdsException
     */
    public synchronized void executeProcedure(
            String procedureName,
            ParameterListItem[] formalParameterList,
            ParameterListItem[] actualParameterList,
            java.sql.Statement stmt,
            int timeout)
             throws java.sql.SQLException, com.internetcds.jdbc.tds.TdsException
    {
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
      checkMaxRows(stmt);
      int outparams = 0;

        int i;

        try {
            // mark that we are performing a query
            cancelController.setQueryInProgressFlag();
            moreResults2 = true;

            // Start sending the procedure execute packet.
            comm.startPacket(TdsComm.PROC);
            if (tdsVer == Tds.TDS70) {
                comm.appendTdsShort((short) (procedureName.length()));
                comm.appendChars(procedureName);
            }
            else {
                byte[] nameBytes = encoder.getBytes(procedureName);
                comm.appendByte((byte) nameBytes.length);
                comm.appendBytes(nameBytes,
                        nameBytes.length,
                        (byte) 0);
            }
            comm.appendByte((byte) 0);
            comm.appendByte((byte) 0);

            // Now handle the parameters
            for (i = 0; i < formalParameterList.length; i++) {
                byte nativeType = cvtJdbcTypeToNativeType(formalParameterList[i].type);

                comm.appendByte((byte) 0);
                if (actualParameterList[i].isOutput)
                {
                   comm.appendByte((byte)1);
                   outparams++;

                   if (nativeType == SYBBIT && actualParameterList[i].value == null)
                   {
                      actualParameterList[i].value = Boolean.FALSE;
                   }
                }
                else
                {
                  comm.appendByte((byte) 0);
                }

                if (actualParameterList[i].value == null &&
                   (nativeType == SYBINT1 || nativeType == SYBINT2 || nativeType == SYBINT4))
                {
                   nativeType = SYBINTN;
                }
                
                switch (nativeType) {
                    case SYBCHAR:
                    {
                        String val;
                        try {
                          val = (String)actualParameterList[i].value;
                        }
                        catch (ClassCastException e) {
                          val = actualParameterList[i].value.toString();
                        }
                        int len = val != null ? val.length() : 0;
                        int max = formalParameterList[i].maxLength;

                        if (actualParameterList[i].formalType != null &&
                             actualParameterList[i].formalType.startsWith("n")) {
                            /*
                             * This is a Unicode column, save to assume TDS 7.0
                             */

                            if (max > 4000) {
                                comm.appendByte(SYBNTEXT);
                                comm.appendTdsInt(max * 2);
                                if (val == null) {
                                    comm.appendTdsInt(0xFFFFFFFF);
                                }
                                else {
                                    comm.appendTdsInt(len * 2);
                                    comm.appendChars(val);
                                }
                            }
                            else {
                                comm.appendByte((byte) (SYBNVARCHAR | 0x80));
                                comm.appendTdsShort((short) (max * 2));
                                if (val == null) {
                                    comm.appendTdsShort((short) 0xFFFF);
                                }
                                else {
                                    comm.appendTdsShort((short) (len * 2));
                                    comm.appendChars(val);
                                }
                            }

                        }
                        else {
                            /*
                             * Either VARCHAR or TEXT, TEXT can not happen
                             * with TDS 7.0 as we would always use NTEXT there
                             */
                            if (tdsVer != TDS70 && max > 255) {
                                // TEXT
                                comm.appendByte((byte) SYBTEXT);
                                if (len > 0)
                                  sendSybImage(encoder.getBytes((String) actualParameterList[i]
                                        .value));
                                else 
                                  sendSybImage((byte[]) actualParameterList[i].value);
                            }
                            else {
                                // VARCHAR
                                sendSybChar(((String) actualParameterList[i].value),
                                        formalParameterList[i].maxLength);
                            }
                        }
                        break;
                    }

                    case SYBINTN:
                      comm.appendByte(SYBINTN);
                      comm.appendByte((byte)4); // maximum length of the field,

                      if (actualParameterList[i].value == null)
                      {
                         comm.appendByte((byte)0); // actual length
                      }
                      else
                      {
                         comm.appendByte((byte)4); // actual length
                         comm.appendTdsInt(((Number)(actualParameterList[i].value)).intValue());
                      }
                      break;

                    case SYBINT4:
                      comm.appendByte(SYBINT4);
                      comm.appendTdsInt(((Number)(actualParameterList[i].value)).intValue());
                      break;

                    case SYBINT2:
                      comm.appendByte(SYBINT2);
                      comm.appendTdsShort(((Number)(actualParameterList[i].value)).shortValue());
                      break;

                    case SYBINT1:
                      comm.appendByte(SYBINT1);
                      comm.appendByte(((Number)(actualParameterList[i].value)).byteValue());
                      break;

                    case SYBFLT8:
                    case SYBFLTN:
                    {
                      if (actualParameterList[i].value == null)
                      {
                         comm.appendByte((byte)SYBFLTN);
                         comm.appendByte((byte)8);
                         comm.appendByte((byte)0);
                      }
                      else
                      {
                         Number n = (Number)(actualParameterList[i].value);
                         Double d = new Double(n.doubleValue());

                         comm.appendByte((byte)SYBFLT8);
                         comm.appendFlt8(d);
                      }
                      break;
                    }
                    case SYBDATETIMN:
                    {
                      writeDatetimeValue(actualParameterList[i].value, SYBDATETIMN);
                      break;
                    }
                    case SYBIMAGE:
                    {
                        comm.appendByte((byte) nativeType);

                        sendSybImage((byte[]) actualParameterList[i].value);
                        break;
                    }
                    case SYBTEXT:
                    {
                        comm.appendByte((byte) SYBTEXT);
                        sendSybImage(encoder.getBytes((String) actualParameterList[i].
                                value));
                        break;
                    }
                    case SYBBIT:
                    case SYBBITN:
                    {
                      if (actualParameterList[i].value == null)
                      {
                         comm.appendByte((byte)SYBBITN);  // 
                         comm.appendByte((byte)1);
                         comm.appendByte((byte)0);
                      }
                      else
                      { 
                         comm.appendByte((byte)SYBBIT);
                         if (((Boolean)actualParameterList[i].value).equals(Boolean.TRUE))
                            comm.appendByte((byte)1);
                             else 
                                comm.appendByte((byte)0);
                          }    
                      break;
                    }
                    case SYBNUMERIC:
                    case SYBDECIMAL:
                    {
                      writeDecimalValue((byte)nativeType, actualParameterList[i].value, 0, -1);
                      break;
                    }
                    case SYBBINARY:
                    case SYBVARBINARY:
                    {
                      byte[] value = (byte[])actualParameterList[i].value;
                      int maxLength = formalParameterList[i].maxLength;
                      if (value == null)
                      {
                         comm.appendByte(SYBVARBINARY);
                         comm.appendByte((byte)(maxLength));
                         comm.appendByte((byte)0);
                      }
                      else
                      {
                         if (value.length > 255)
                         {
                            if (tdsVer != TDS70)
                            {
                               throw new java.io.IOException("Field too long");
                            }                            
                            comm.appendByte((byte) (SYBBIGVARBINARY));
                            if (maxLength < 0 || maxLength > 8000) {
                              comm.appendTdsShort((short)8000);
                            }
                            else                              
                              comm.appendTdsShort((short)maxLength);
                            comm.appendTdsShort((short)(value.length));
                         }
                         else
                         {
                            comm.appendByte(SYBVARBINARY);
                            comm.appendByte((byte)(maxLength));
                            comm.appendByte((byte)(value.length));
                         }
                         comm.appendBytes(value);
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
                    {
                        throw new SQLException("Not implemented for nativeType 0x"
                                 + Integer.toHexString(nativeType));
                    }
                }
            }
            comm.sendPacket();

            waitForDataOrTimeout(stmt, timeout);
        }
        catch (java.io.IOException e) {
            throw new SQLException("Network error-  " + e.getMessage());
        }
    }


    void checkMaxRows(java.sql.Statement stmt)
             throws java.sql.SQLException, TdsException
    {
      if (stmt == null) // internal usage
        return;
      int maxRows = stmt.getMaxRows();
      if ( maxRows != this.maxRows) {
        submitProcedure("set rowcount " + maxRows, new SQLWarningChain());
        this.maxRows = maxRows;
      }
    }
    /**
     *  send a query to the SQLServer for execution. <p>
     *
     *
     *
     *@param  sql                                        sql statement to
     *      execute.
     *@param  stmt
     *@param  timeout
     *@exception  java.io.IOException
     *@exception  java.sql.SQLException                  Description of
     *      Exception
     *@exception  TdsException                           Description of
     *      Exception
     */
    public synchronized void executeQuery(
            String sql,
            java.sql.Statement stmt,
            int timeout)
             throws java.io.IOException, java.sql.SQLException, TdsException
    {
      checkMaxRows(stmt);
         {
            cancelController.setQueryInProgressFlag();
            comm.startPacket(TdsComm.QUERY);
            if (tdsVer == Tds.TDS70) {
                comm.appendChars(sql);
            }
            else {
                byte[] sqlBytes = encoder.getBytes(sql);
                comm.appendBytes(sqlBytes, sqlBytes.length, (byte) 0);
            }
            moreResults2 = true;
            //JJ 1999-01-10
            comm.sendPacket();

            waitForDataOrTimeout(stmt, timeout);
        }
    }


    /**
     *  skip over and discard any remaining data from a result set.
     *
     *@param  context                                    Description of
     *      Parameter
     *@exception  com.internetcds.jdbc.tds.TdsException
     *@exception  java.io.IOException
     */
    public synchronized void discardResultSet( PacketRowResult row )
             throws SQLException, java.io.IOException, TdsException
    {

        while ( isResultRow()) {
            comm.skip(1);
            if ( row != null ) {
                loadRow( row );
            }
            if ( Logger.isActive() ) {
                Logger.println( "Discarded row." );
            }
        }

        if (comm.peek() == TDS_DONEINPROC) {
            PacketResult tmp = processSubPacket();
            if ( Logger.isActive() ) {
                Logger.println( "Discarded TDS_DONE_IN_PROC" );
            }
        }


        // XXX Is there ever going to be a situation where the
        // TDS_DONEINPROC is the real end of data?  If so then the
        // next section of code will hang forever waiting for more data
        // from the socket.
        if (isEndOfResults() || isRetStat()) {
            processSubPacket();
        }

        // RMK 2000-06-08 Don't choke on additional result sets.
        else if (!isResultSet()) {
            throw new TdsConfused("Was expecting an end of results token.  "
                     + "Found a 0x"
                     + Integer.toHexString(comm.peek() & 0xff));
        }
    }

    /**
     * @deprecated
     */
    public synchronized void discardResultSetOld( Context context )
             throws SQLException, java.io.IOException, TdsException
    {
        discardResultSet( new PacketRowResult( context ) );
    }


    public synchronized byte peek()
             throws java.io.IOException, com.internetcds.jdbc.tds.TdsException
    {
        return comm.peek();
    }


    EncodingHelper getEncoder()
    {
        return encoder;
    }


    /**
     *  Accessor method to determine the TDS level used.
     *
     *@return    TDS42, TDS50, or TDS70.
     */
    int getTdsVer()
    {
        return tdsVer;
    }


    /**
     *  Return the name that this database server program calls itself.
     *
     *@return    The DatabaseProductName value
     */
    String getDatabaseProductName()
    {
        return databaseProductName;
    }


    /**
     *  Return the name that this database server program calls itself.
     *
     *@return    The DatabaseProductVersion value
     */
    String getDatabaseProductVersion()
    {
        return databaseProductVersion;
    }



   /**
    * Process an output parameter subpacket.
    * <p>
    * This routine assumes that the TDS_PARAM_TOKEN byte has already
    * been read.
    *
    * @return
    *
    * @exception com.internetcds.jdbc.tds.TdsException
    *
    * @exception java.io.IOException
    * Thrown if some sort of error occured reading bytes from the network.
    */
   private PacketOutputParamResult processOutputParam()
      throws com.internetcds.jdbc.tds.TdsException, java.io.IOException
   {
      int packetLen = getSubPacketLength();
      String colname = comm.getString(comm.getByte() & 0xff);
      comm.getBytes(5);

      byte colType = comm.getByte();
      
      // TODO: Refactor to combine this code with that in getRow()
      Object element;
      switch (colType)
      {
         case SYBINTN:
         {
            int column_size = comm.getByte();
            element = getIntValue(colType);
            break;
         }
         case SYBINT1:
         case SYBINT2:
         case SYBINT4:
         {
            element = getIntValue(colType);
            break;
         }
         case SYBIMAGE:
         {
            int column_size = comm.getByte();
            element = getImageValue();
            break;
         }
         case SYBTEXT:
         {
            int column_size = comm.getByte();
            element = getTextValue(false);
            break;
         }
         case SYBNTEXT:
         {
            int column_size = comm.getByte();
            element = getTextValue(true);
            break;
         }
         case SYBCHAR:
         case SYBVARCHAR:
         {
            int column_size = comm.getByte();
            element = getCharValue(false, true);
            break;
         }
         case (byte)(SYBBIGVARBINARY):
         {
            int column_size = comm.getTdsShort();
            int len = comm.getTdsShort();
            // if (tdsVer == Tds.TDS70 && len == 0xffff)
            element = comm.getBytes(len);
            break;
         }
         case (byte)(SYBBIGVARCHAR):
         {
            int column_size = comm.getTdsShort();
            element = getCharValue(false, false);
            break;
         }
         case SYBNCHAR:
         case SYBNVARCHAR:
         {
            int column_size = comm.getByte();
            element = getCharValue(true, true);
            break;
         }
         case SYBREAL:
         {
            element = readFloatN(4);
            break;
         }
         case SYBFLT8:
         {
            element = readFloatN(8);
            break;
         }
         case SYBFLTN:
         {
            int column_size = comm.getByte();
            int actual_size = comm.getByte();
            element = readFloatN(actual_size);
            break;
         }
         case SYBSMALLMONEY:
         case SYBMONEY:
         case SYBMONEYN:
         {
            int column_size = comm.getByte();
            element = getMoneyValue(colType);
            break;
         }
         case SYBNUMERIC:
         case SYBDECIMAL:
         {
            int column_size = comm.getByte();
            int precision = comm.getByte();
            int scale = comm.getByte();
            element = getDecimalValue(scale);
            break;
         }
         case SYBDATETIMN:
         {
            int column_size = comm.getByte();
            element = getDatetimeValue(colType);
            break;
         }
         case SYBDATETIME4:
         case SYBDATETIME:
         {
            element = getDatetimeValue(colType);
            break;
         }
         case SYBVARBINARY:
         case SYBBINARY:
         {
            int column_size = comm.getByte();
            int len = (comm.getByte() & 0xff);
            // if (tdsVer == Tds.TDS70 && len == 0xffff)
            element = comm.getBytes(len);
            break;
         }
         case SYBBITN:
         {
            int column_size = comm.getByte();
            if (comm.getByte() == 0)
               element = null;
            else
               element = new Boolean((comm.getByte()!=0) ? true : false);
            break;
         }
         case SYBBIT:
         {
            int column_size = comm.getByte();
            element = new Boolean((column_size != 0) ? true : false);
            break;
         }
         default:
         {
            element = null;
            throw new TdsNotImplemented("Don't now how to handle " +
                                        "column type 0x" +
                                        Integer.toHexString(colType));
         }
      }

      return new PacketOutputParamResult(element);
   }

    /**
     *  Process a subpacket reply <p>
     *
     *  <b>Note-</b> All subpackets must be processed through here. This is the
     *  only routine has the proper locking to support the cancel method in the
     *  Statement class. <br>
     *
     *
     *@return                                            packet subtype the was
     *      processed.
     *@exception  TdsUnknownPacketSubType                Description of
     *      Exception
     *@exception  java.io.IOException                    Description of
     *      Exception
     *@exception  com.internetcds.jdbc.tds.TdsException  Description of
     *      Exception
     */
    PacketResult processSubPacket()
             throws TdsUnknownPacketSubType,
            java.io.IOException,
            com.internetcds.jdbc.tds.TdsException,
            SQLException
    {
        return processSubPacket(null);
    }


    /**
     *  Process a subpacket reply <p>
     *
     *  <b>Note-</b> All subpackets must be processed through here. Only this
     *  routine has the proper locking to support the cancel method in the
     *  Statement class. <br>
     *
     *
     *@param  context                                    Description of
     *      Parameter
     *@return                                            packet subtype the was
     *      processed.
     *@exception  TdsUnknownPacketSubType                Description of
     *      Exception
     *@exception  java.io.IOException                    Description of
     *      Exception
     *@exception  com.internetcds.jdbc.tds.TdsException  Description of
     *      Exception
     */
    synchronized PacketResult processSubPacket(Context context)
             throws TdsUnknownPacketSubType,
            SQLException,
            java.io.IOException,
            com.internetcds.jdbc.tds.TdsException
    {
        // NOTE!!! Before adding anything to this list you must
        // consider the ramifications to the the handling of cancels
        // as implemented by the CancelController class.
        //
        // The CancelController class might implicitly assume it can call
        // processSubPacket() whenever it is looking for a cancel
        // acknowledgment.  It assumes that any results of the call
        // can be discarded.

        PacketResult result = null;

        moreResults = false;

        byte packetSubType = comm.getByte();

        Logger.println("processSubPacket: " +
                Integer.toHexString(packetSubType & 0xFF) + " " +
                "moreResults: " + moreResults());

        switch (packetSubType) {
            case TDS_ENV_CHG_TOKEN:
            {
                result = processEnvChange();
                break;
            }
            case TDS_ERR_TOKEN:
            case TDS_MSG_TOKEN:
            case TDS_MSG50_TOKEN:
            {
                result = processMsg(packetSubType);
                break;
            }
            case TDS_PARAM_TOKEN:
            {
              result = processOutputParam();
              break;
            }
            /* strange replaced by PARAM_TOKEN
            case TDS_TEXT_UPD_TOKEN:
            {
                int len = getSubPacketLength();
                comm.skip(len);
                result = new PacketResult(TDS_TEXT_UPD_TOKEN);
                break;
            }
             */
            case TDS_LOGIN_ACK_TOKEN:
            {
                result = processLoginAck();
                break;
            }
            case TDS_RET_STAT_TOKEN:
            {
                result = processRetStat();
                break;
            }
            case TDS_PROCID:
            {
                result = processProcId();
                break;
            }
            case TDS_DONEINPROC:
              /*
            {
                result = processDoneInProc(packetSubType);
                break;
            }
               */
            case TDS_DONEPROC:
            case TDS_END_TOKEN:
            {
                result = processEndToken(packetSubType);
                moreResults2 = ((PacketEndTokenResult) result).moreResults();
                break;
            }
            case TDS_COL_NAME_TOKEN:
            {
                result = processColumnNames();
                break;
            }
            case TDS_COL_INFO_TOKEN:
            {
                result = processColumnInfo();
                break;
            }
            case TDS_UNKNOWN_0xA5:
            case TDS_UNKNOWN_0xA7:
            case TDS_UNKNOWN_0xA8:
            {
                // XXX Need to figure out what this packet is
                comm.skip(comm.getTdsShort());
                result = new PacketUnknown(packetSubType);
                break;
            }
            case TDS_TABNAME:
            {
                result = processTabName();
                break;
            }
            case TDS_ORDER:
            {
                int len = comm.getTdsShort();
                comm.skip(len);

                result = new PacketColumnOrderResult();
                break;
            }
            case TDS_CONTROL:
            {
                int len = comm.getTdsShort();
                comm.skip(len);
                // FIXME - I'm just ignoring this
                result = new PacketControlResult();
                break;
            }
            case TDS_ROW_TOKEN:
            {
                result = getRow(context);
                break;
            }
            case TDS7_RESULT_TOKEN:
            {
                result = processTds7Result();
                break;
            }
            default:
            {
                throw new TdsUnknownPacketSubType(packetSubType);
            }
        }

        return result;
    }


    private void setCharset(String charset)
    {
        try {
            Logger.println("Trying to change charset to " + charset);
        }
        catch (java.io.IOException e) {
            // nop
        }

        if (charset == null || charset.length() > 30) {
            charset = "iso_1";
        }

        if (!charset.equals(this.charset)) {
            encoder = EncodingHelper.getHelper(charset);
            if (encoder == null) {
                charset = "iso_1";
                encoder = EncodingHelper.getHelper(charset);
            }
            this.charset = charset;
        }
    }


    /**
     *  Try to figure out what client name we should identify ourselves as. Get
     *  the hostname of this machine,
     *
     *@return    name we will use as the client.
     */
    private String getClientName()
    {
        // This method is thread safe.
        String tmp;
        try {
            tmp = java.net.InetAddress.getLocalHost().getHostName();
        }
        catch (java.net.UnknownHostException e) {
            tmp = "";
        }
        StringTokenizer st = new StringTokenizer(tmp, ".");


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
        }
        else if (Character.isDigit(tmp.charAt(0))) {
            // This probably means that the name was a quad-decimal
            // number.  We don't want to send that as our name,
            // so make one up.
            return "BABYDOE";
        }
        else {
            // Ah, Life is good.  We have a name.  All other
            // applications I've seen have upper case client names,
            // and so shall we.
            return tmp.toUpperCase();
        }
    }


    /**
     *  Get the length of the current subpacket. <p>
     *
     *  This will eat two bytes from the input socket.
     *
     *@return                                            length of the current
     *      subpacket.
     *@exception  java.io.IOException                    Description of
     *      Exception
     *@exception  com.internetcds.jdbc.tds.TdsException  Description of
     *      Exception
     */
    private int getSubPacketLength()
             throws java.io.IOException, com.internetcds.jdbc.tds.TdsException
    {
        return comm.getTdsShort();
    }
    // lookupColumnSize()



    /**
     *  determine if a given datatype is a fixed size
     *
     *@param  nativeColumnType                           The SQLServer datatype
     *      to check
     *@return                                            <code>true</code> if
     *      the datatype is a fixed size, <code>false</code> if the datatype is
     *      a variable size
     *@exception  com.internetcds.jdbc.tds.TdsException  If the <code>
     *      nativeColumnType</code> is not a knowm datatype.
     */
    private boolean isFixedSizeColumn(byte nativeColumnType)
             throws com.internetcds.jdbc.tds.TdsException
    {
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
            {
                return true;
            }
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
            {
                return false;
            }
            default:
            {
                throw new TdsException("Unrecognized column type 0x"
                         + Integer.toHexString(nativeColumnType));
            }
        }
    }


    private Object getMoneyValue(
            int type)
             throws java.io.IOException, TdsException
    {
        int len;
        Object result;

        if (type == SYBMONEYN) {
            len = comm.getByte();
        }
        else {
            len = lookupColumnSize((byte) type);
        }

        if (len == 0) {
            result = null;
        }
        else {
            BigInteger x = null;

            if (len == 4) {
                x = BigInteger.valueOf(comm.getTdsInt());
            }
            else if (len == 8) {
                byte b4 = comm.getByte();
                byte b5 = comm.getByte();
                byte b6 = comm.getByte();
                byte b7 = comm.getByte();
                byte b0 = comm.getByte();
                byte b1 = comm.getByte();
                byte b2 = comm.getByte();
                byte b3 = comm.getByte();
                long l =
                        (long) (b0 & 0xff) + ((long) (b1 & 0xff) << 8) +
                        ((long) (b2 & 0xff) << 16) + ((long) (b3 & 0xff) << 24) +
                        ((long) (b4 & 0xff) << 32) + ((long) (b5 & 0xff) << 40) +
                        ((long) (b6 & 0xff) << 48) + ((long) (b7 & 0xff) << 56);
                x = BigInteger.valueOf(l);
            }
            else {
                throw new TdsConfused("Don't know what to do with len of "
                         + len);
            }
            x = x.divide(BigInteger.valueOf(100));
            result = new BigDecimal(x, 2);
        }
        return result;
    }
    // getMoneyValue


    /**
     *  Extracts decimal value from the server's results packet. Takes advantage
     *  of Java's superb handling of large numbers, which does all the heavy
     *  lifting for us. Format is:
     *  <UL>
     *    <LI> Length byte <code>len</code> ; count includes sign byte.</LI>
     *
     *    <LI> Sign byte (0=negative; 1=positive).</LI>
     *    <LI> Magnitude bytes (array of <code>len</code> - 1 bytes, in
     *    little-endian order.</LI>
     *  </UL>
     *
     *
     *@param  scale                      number of decimal digits after the
     *      decimal point.
     *@return                            <code>BigDecimal</code> for extracted
     *      value (or ( <code>null</code> if appropriate).
     *@exception  TdsException           Description of Exception
     *@exception  java.io.IOException    Description of Exception
     *@exception  NumberFormatException  Description of Exception
     */
    private Object getDecimalValue(int scale)
             throws TdsException, java.io.IOException, NumberFormatException
    {
        int len = comm.getByte() & 0xff;
        if (--len < 1) {
            return null;
        }

        // RMK 2000-06-10.  Deduced from some testing/packet sniffing.
        byte[] bytes = new byte[len];
        int signum = comm.getByte() == 0 ? -1 : 1;
        while (len > 0) {
            bytes[--len] = comm.getByte();
        }
        BigInteger bigInt = new BigInteger(signum, bytes);
        return new BigDecimal(bigInt, scale);
    }


    private Object getDatetimeValue(
            int type)
             throws java.io.IOException, TdsException
    {
        // Some useful constants
        final long SECONDS_PER_DAY = 24L * 60L * 60L;
        final long DAYS_BETWEEN_1900_AND_1970 = 25567L;

        int len;
        Object result;

        if (type == SYBDATETIMN) {
            len = comm.getByte();
        }
        else if (type == SYBDATETIME4) {
            len = 4;
        }
        else {
            len = 8;
            // XXX shouldn't this be an error?
        }

        switch (len) {
            case 0:
            {
                result = null;
                break;
            }
            case 8:
            {
                // It appears that a datetime is made of of 2 32bit ints
                // The first one is the number of days since 1900
                // The second integer is the number of seconds*300
                // The reason the calculations below are sliced up into
                // such small baby steps is to avoid a bug in JDK1.2.2's
                // runtime, which got confused by the original complexity.
                long tdsDays = (long) comm.getTdsInt();
                long tdsTime = (long) comm.getTdsInt();
                long sqlDays = tdsDays - DAYS_BETWEEN_1900_AND_1970;
                long seconds = sqlDays * SECONDS_PER_DAY + tdsTime / 300L;
                long micros = ((tdsTime % 300L) * 1000000L) / 300L;
                long millis = seconds * 1000L + micros / 1000L - zoneOffset;

                // Round up if appropriate.
                if (micros % 1000L >= 500L) {
                    millis++;
                }

                result = new Timestamp(millis - getDstOffset(millis));
                break;
            }
            case 4:
            {
                // Accroding to Transact SQL Reference
                // a smalldatetime is two small integers.
                // The first is the number of days past January 1, 1900,
                // the second smallint is the number of minutes past
                // midnight.

                long tdsDays = (long) comm.getTdsShort();
                long minutes = (long) comm.getTdsShort();
                long sqlDays = tdsDays - DAYS_BETWEEN_1900_AND_1970;
                long seconds = sqlDays * SECONDS_PER_DAY + minutes * 60L;
                long millis = seconds * 1000L - zoneOffset;

                result = new Timestamp(millis - getDstOffset(millis));
                break;
            }
            default:
            {
                result = null;
                throw new TdsNotImplemented("Don't now how to handle "
                         + "date with size of "
                         + len);
            }
        }
        return result;
    }
    // getDatetimeValue()


    /**
     *  Determines the number of milliseconds needed to adjust for daylight
     *  savings time for a given date/time value. Note that there is a problem
     *  with the way SQL Server sends a DATETIME value, since it is constructed
     *  to represent the local time for the server. This means that each fall
     *  there is a window of approximately one hour during which a single value
     *  can represent two different times.
     *
     *@param  time  Description of Parameter
     *@return       The DstOffset value
     */
    private long getDstOffset(long time)
    {
        Calendar cal = Calendar.getInstance();
        cal.setTime(new java.util.Date(time));
        return cal.get(Calendar.DST_OFFSET);
    }


    private Object getIntValue(int type)
             throws java.io.IOException, TdsException
    {
        Object result;
        int len;

        switch (type) {
            case SYBINTN:
            {
                len = comm.getByte();
                break;
            }
            case SYBINT4:
            {
                len = 4;
                break;
            }
            case SYBINT2:
            {
                len = 2;
                break;
            }
            case SYBINT1:
            {
                len = 1;
                break;
            }
            default:
            {
                result = null;
                throw new TdsNotImplemented(
                        "can't handle integer of type "
                         + Integer.toHexString(type));
            }
        }

        switch (len) {
/*  curt´s version 
          case 4:
            result = new Integer(comm.getTdsInt());
            break;
         
         case 2:
            result = new Short((short)comm.getTdsShort());
            break;
         
         case 1: 
            result = new Byte(comm.getByte());
            break;
*/
          case 4:
            {
                result = new Integer(comm.getTdsInt());
                break;
            }
            case 2:
            {
                result = new Short((short)comm.getTdsShort());
                break;
            }
            case 1:
            {
                int tmp = toUInt(comm.getByte());
                // XXX Are we sure this should be unsigned?
                // AS: no, there is no unsigned Byte in Java so -> use smallint in Params and Table-Creation
                result = new Long(tmp);
                break;
            }
            case 0:
            {
                result = null;
                break;
            }
            default:
            {
                result = null;
                throw new TdsConfused("Bad SYBINTN length of " +
                        len);
            }
        }
        return result;
    }
    // getIntValue()


   private Object getCharValue(boolean wideChars, boolean outputParam)
      throws TdsException, java.io.IOException
   {
      Object result;
      
      boolean shortLen = (tdsVer == Tds.TDS70) && (wideChars || !outputParam);
      int len = shortLen ? comm.getTdsShort() : (comm.getByte() & 0xFF);
      if (len == 0 || tdsVer == Tds.TDS70 && len == 0xFFFF) {
          result = null;
      }
      else if (len > 0) {
          if (wideChars) {
              result = comm.getString(len / 2);
          }
          else {
              result = encoder.getString(comm.getBytes(len));
          }

          if (result.equals(" ")) {
              // In SQL trailing spaces are stripped from strings
              // MS SQLServer denotes a zero length string
              // as a single space.
              result = "";
          }
      }
      else {
          throw new TdsConfused("String with length<0");
      }
      return result;
    }
    // getCharValue()


    private Object getTextValue(boolean wideChars)
             throws TdsException, java.io.IOException
    {
        String result;

        byte hasValue = comm.getByte();

        if (hasValue == 0) {
            result = null;
        }
        else {
            // XXX Have no idea what these 24 bytes are
            // 2000-06-06 RMK They are the TEXTPTR (16 bytes) and the TIMESTAMP.
            comm.skip(24);

            int len = comm.getTdsInt();

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
                    result = comm.getString(len / 2);
                }
                else {
                    result = encoder.getString(comm.getBytes(len));
                }

                if (" ".equals(result)) {
                    // In SQL trailing spaces are stripped from strings
                    // MS SQLServer denotes a zero length string
                    // as a single space.
                    result = "";
                }
            }
            else {
                throw new TdsConfused("String with length<0");
            }
        }
        return result;
    }
    // getTextValue()


    private Object getImageValue() throws TdsException, java.io.IOException
    {
        byte[] result;

        byte hasValue = comm.getByte();

        if (hasValue == 0) {
            result = null;
        }
        else {
            // XXX Have no idea what these 24 bytes are
            // 2000-06-06 RMK They are the TEXTPTR (16 bytes) and the TIMESTAMP.
            comm.skip(24);

            int len = comm.getTdsInt();

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
                result = comm.getBytes(len);
            }
            else {
                throw new TdsConfused("String with length<0");
            }
        }
        return result;
    }
    // getImageValue()

    /**
     *  get one result row from the TDS stream <p>
     *
     *  This will read a full row from the TDS stream and store it in a
     *  PacketRowResult object.
     *
     *@param  context                  Description of Parameter
     *@return                          The Row value
     *@exception  TdsException         Description of Exception
     *@exception  java.io.IOException  Description of Exception
     *
     */
    private synchronized PacketRowResult loadRow( PacketRowResult result )
             throws SQLException, TdsException, java.io.IOException
    {
        int i;

        Columns columnsInfo = result.getContext().getColumnInfo();

        for (i = 1; i <= columnsInfo.getColumnCount(); i++) {
            Object element;
            int colType = columnsInfo.getNativeType(i);

            Logger.println("colno=" + i +
                    " type=" + colType +
                    " offset=" + Integer.toHexString(comm.inBufferIndex));

            switch (colType) {
                case SYBINTN:
                case SYBINT1:
                case SYBINT2:
                case SYBINT4:
                {
                    element = getIntValue(colType);
                    break;
                }
                case SYBIMAGE:
                {
                    element = getImageValue();
                    break;
                }
                case SYBTEXT:
                {
                    element = getTextValue(false);
                    break;
                }
                case SYBNTEXT:
                {
                    element = getTextValue(true);
                    break;
                }
                case SYBCHAR:
                case SYBVARCHAR:
                {
                    element = getCharValue(false,false);
                    break;
                }
                case SYBNCHAR:
                case SYBNVARCHAR:
                {
                    element = getCharValue(true,false);
                    break;
                }
                case SYBREAL:
                {
                    element = readFloatN(4);
                    break;
                }
                case SYBFLT8:
                {
                    element = readFloatN(8);
                    break;
                }
                case SYBFLTN:
                {
                    int len;

                    len = comm.getByte();

                    element = readFloatN(len);
                    break;
                }
                case SYBSMALLMONEY:
                case SYBMONEY:
                case SYBMONEYN:
                {
                    element = getMoneyValue(colType);
                    break;
                }
                case SYBNUMERIC:
                case SYBDECIMAL:
                {
                    element = getDecimalValue(columnsInfo.getScale(i));
                    break;
                }
                case SYBDATETIME4:
                case SYBDATETIMN:
                case SYBDATETIME:
                {
                    element = getDatetimeValue(colType);
                    break;
                }
                case SYBVARBINARY:
                case SYBBINARY:
                {
                    int len = tdsVer == Tds.TDS70
                             ? comm.getTdsShort()
                             : (comm.getByte() & 0xff);
                    if (tdsVer == Tds.TDS70 && len == 0xffff) {
                        element = null;
                    }
                    else {
                        element = comm.getBytes(len);
                    }
                    break;
                }
                case SYBBITN:
                case SYBBIT:
                {
                    if (colType == SYBBITN && comm.getByte() == 0) {
                        element = null;
                    }
                    else {
                        element = new Boolean((comm.getByte() != 0) ? true : false);
                    }
                    break;
                }
                default:
                {
                    element = null;
                    throw new TdsNotImplemented("Don't now how to handle " +
                            "column type 0x" +
                            Integer.toHexString(colType));
                }
            }
            result.setElementAt(i, element);
        }

        return result;
    }

    /**
     * @deprecated use loadRow() - its much more efficient as it
     * doesn't create a new PacketRowResult for each row...
     */
    private synchronized PacketRowResult getRow(Context context)
             throws SQLException, TdsException, java.io.IOException
    {
        PacketRowResult result = new PacketRowResult(context);
        return loadRow( result );
    }


    /**
     *  Log onto the SQLServer <p>
     *
     *  This method is not synchronized and does not need to be so long as it
     *  can only be called from the constructor. <p>
     *
     *  <U>Login Packet</U> <P>
     *
     *  Packet type (first byte) is 2. The following is from tds.h the numbers
     *  on the left are offsets <I>not including</I> the packet header. <br>
     *  Note: The logical logon packet is split into two physical packets. Each
     *  physical packet has its own header. <br>
     *  <PRE>
     *
     *  -- 0 -- DBCHAR host_name[30]; -- 30 -- DBTINYINT host_name_length; -- 31
     *  -- DBCHAR user_name[30]; -- 61 -- DBTINYINT user_name_length; -- 62 --
     *  DBCHAR password[30]; -- 92 -- DBTINYINT password_length; -- 93 -- DBCHAR
     *  host_process[30]; -- 123 -- DBTINYINT host_process_length; -- 124 --
     *  DBCHAR magic1[6]; -- here were most of the mystery stuff is -- -- 130 --
     *  DBTINYINT bulk_copy; -- 131 -- DBCHAR magic2[9]; -- here were most of
     *  the mystery stuff is -- -- 140 -- DBCHAR app_name[30]; -- 170 --
     *  DBTINYINT app_name_length; -- 171 -- DBCHAR server_name[30]; -- 201 --
     *  DBTINYINT server_name_length; -- 202 -- DBCHAR magic3; -- 0, dont know
     *  this one either -- -- 203 -- DBTINYINT password2_length; -- 204 --
     *  DBCHAR password2[30]; -- 234 -- DBCHAR magic4[223]; -- 457 -- DBTINYINT
     *  password2_length_plus2; -- 458 -- DBSMALLINT major_version; -- TDS
     *  version -- -- 460 -- DBSMALLINT minor_version; -- TDS version -- -- 462
     *  -- DBCHAR library_name[10]; -- Ct-Library or DB-Library -- -- 472 --
     *  DBTINYINT library_length; -- Ct-Library or DB-Library -- -- 473 --
     *  DBSMALLINT major_version2; -- program version -- -- 475 -- DBSMALLINT
     *  minor_version2; -- program version -- -- 477 -- DBCHAR magic6[3]; -- ?
     *  last two octets are 13 and 17 -- -- bdw reports last two as 12 and 16
     *  here -- -- possibly a bitset flag -- -- 480 -- DBCHAR language[30]; --
     *  ie us-english -- -- second packet -- -- 524 -- DBTINYINT
     *  language_length; -- 10 in this case -- -- 525 -- DBCHAR magic7; -- no
     *  clue... has 1 in the first octet -- -- bdw reports 0x0 -- -- 526 --
     *  DBSMALLINT old_secure; -- explaination? -- -- 528 -- DBTINYINT
     *  encrypted; -- 1 means encrypted all password fields blank -- -- 529 --
     *  DBCHAR magic8; -- no clue... zeros -- -- 530 -- DBCHAR sec_spare[9]; --
     *  explaination -- -- 539 -- DBCHAR char_set[30]; -- ie iso_1 -- -- 569 --
     *  DBTINYINT char_set_length; -- 5 -- -- 570 -- DBTINYINT magic9; -- 1 --
     *  -- 571 -- DBCHAR block_size[6]; -- in text -- -- 577 -- DBTINYINT
     *  block_size_length; -- 578 -- DBCHAR magic10[25]; -- lots of stuff
     *  here...no clue --</PRE> This routine will basically eat all of the data
     *  returned from the SQLServer.
     *
     *@return                                            Description of the
     *      Returned Value
     *@exception  TdsUnknownPacketSubType
     *@exception  com.internetcds.jdbc.tds.TdsException
     *@exception  java.io.IOException
     *@exception  java.sql.SQLException
     *@author                                            Craig Spannring
     */
    private boolean logon()
             throws java.sql.SQLException,
            TdsUnknownPacketSubType, java.io.IOException,
            com.internetcds.jdbc.tds.TdsException
    {
        boolean isOkay = true;
        byte pad = (byte) 0;
        byte[] empty = new byte[0];

        // Added 2000-06-07.
        if (tdsVer == Tds.TDS70) {
            send70Login();
        }
        else {

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
            tmp = encoder.getBytes(progName);
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

            moreResults2 = true;
            //JJ 1999-01-10
        }

        comm.sendPacket();

        // Get the reply to the logon packet.
        PacketResult result;

        while (!((result = processSubPacket()) instanceof PacketEndTokenResult)) {
            if (result instanceof PacketErrorResult) {
                isOkay = false;
            }
            // XXX Should really process some more types of packets.
        }

        if (isOkay) {
            // XXX Should we move this to the Connection class?
            isOkay = changeSettings(database, initialSettings);
        }

        // XXX Possible bug.  What happend if this is cancelled before the logon
        // takes place?  Should isOkay be false?
        return isOkay;
    }


    /*
     * New code added to handle TDS 7.0 login, which uses a completely
     * different packet layout.  Logic taken directly from freetds C
     * code in tds/login.c.  Lots of magic values: I don't pretend
     * to have any idea what most of this means.
     *
     * Added 2000-06-05.
     */
    private void send70Login() throws java.io.IOException
    {

        byte[] magic1 = {(byte) 0006, (byte) 0203, (byte) 0362, (byte) 0370,
                (byte) 0377, (byte) 0000, (byte) 0000, (byte) 0000,
                (byte) 0000, (byte) 0340, (byte) 0003, (byte) 0000,
                (byte) 0000, (byte) 0210, (byte) 0377, (byte) 0377,
                (byte) 0377, (byte) 0066, (byte) 0004, (byte) 0000,
                (byte) 0000};
        byte[] magic2 = {(byte) 0000, (byte) 0100, (byte) 0063, (byte) 0232,
                (byte) 0153, (byte) 0120};
        byte[] magic3 = encoder.getBytes("NTLMSSP");
        String libName = "DB-Library";
        byte pad = (byte) 0;
        byte[] empty = new byte[0];
        String appName = "CDR";
        short len = (short) (86 + 2 * (user.length() +
                password.length() +
                appName.length() +
                serverName.length() +
                libName.length()));
        short packSize = (short) (len + 48);
        comm.startPacket(TdsComm.LOGON70);
        comm.appendTdsShort(packSize);
        comm.appendBytes(empty, 5, pad);
        comm.appendByte((byte) 0x70);
        comm.appendBytes(empty, 7, pad);
        comm.appendBytes(magic1, 21, pad);

        // Pack up value lengths, positions.
        short curPos = 86;

        // Unknown
        comm.appendTdsShort(curPos);
        comm.appendTdsShort((short) 0);

        // Username
        comm.appendTdsShort(curPos);
        comm.appendTdsShort((short) user.length());
        curPos += user.length() * 2;

        // Password
        comm.appendTdsShort(curPos);
        comm.appendTdsShort((short) password.length());
        curPos += password.length() * 2;

        // App name
        comm.appendTdsShort(curPos);
        comm.appendTdsShort((short) appName.length());
        curPos += appName.length() * 2;

        // Server name
        comm.appendTdsShort(curPos);
        comm.appendTdsShort((short) serverName.length());
        curPos += serverName.length() * 2;

        // Another unknown value
        comm.appendTdsShort((short) 0);
        comm.appendTdsShort((short) 0);

        // Library name
        comm.appendTdsShort(curPos);
        comm.appendTdsShort((short) libName.length());
        curPos += libName.length() * 2;

        // Two more unknowns
        comm.appendTdsShort(curPos);
        comm.appendTdsShort((short) 0);
        comm.appendTdsShort(curPos);
        comm.appendTdsShort((short) 0);

        // More magic.
        comm.appendBytes(magic2, 6, pad);
        comm.appendTdsShort(len);
        comm.appendTdsShort((short) 0x30);
        comm.appendTdsShort(packSize);
        comm.appendTdsShort((short) 0);

        // Pack up the login values.
        String scrambledPw = tds7CryptPass(password);
        comm.appendChars(user);
        comm.appendChars(scrambledPw);
        comm.appendChars(appName);
        comm.appendChars(serverName);
        comm.appendChars(libName);

        // Still more magic!
        comm.appendBytes(magic3, 7, pad);
        comm.appendByte((byte) 0);
        comm.appendByte((byte) 1);
        comm.appendBytes(empty, 3, pad);
        comm.appendByte((byte) 6);
        comm.appendByte((byte) 130);
        comm.appendBytes(empty, 22, pad);
        comm.appendByte((byte) 48);
        comm.appendBytes(empty, 7, pad);
        comm.appendByte((byte) 48);
        comm.appendBytes(empty, 3, pad);
    }
    // changeSettings


    /**
     *  Select a new database to use.
     *
     *@param  database                   Name of the database to use.
     *@return                            true if the change was accepted, false
     *      otherwise
     *@exception  java.sql.SQLException  Description of Exception
     */
    private synchronized boolean changeDB(String database)
             throws java.sql.SQLException
    {
        boolean isOkay = true;
        ;

        try {
            PacketResult result;
            int i;

            // XXX Check to make sure the database name
            // doesn't have funny characters.


//      if (database name has funny characters)
            if (database.length() > 32 || database.length() < 1) {
                throw new SQLException("Name too long - " + database);
            }

            for (i = 0; i < database.length(); i++) {
                char ch;
                ch = database.charAt(i);
                if (!
                        ((ch == '_' && i != 0)
                         || (ch >= 'a' && ch <= 'z')
                         || (ch >= 'A' && ch <= 'Z')
                         || (ch >= '0' && ch <= '9'))) {
                    throw new SQLException("Bad database name- "
                             + database);
                }
            }
            if ((database.charAt(0) >= '0') && (database.charAt(0) <= '9'))
            {
              database = "\"" + database + "\"";
            }

            String query = "use " + database;
            comm.startPacket(TdsComm.QUERY);
            if (tdsVer == Tds.TDS70) {
                comm.appendChars(query);
            }
            else {
                byte[] queryBytes = encoder.getBytes(query);
                comm.appendBytes(queryBytes, queryBytes.length, (byte) 0);
            }
            moreResults2 = true;
            //JJ 1999-01-10
            comm.sendPacket();

            // XXX Should we check that the change actual was okay
            // and throw some sort of exception if it wasn't?
            // Get the reply to the change database request.
            while (!((result = processSubPacket())
                     instanceof PacketEndTokenResult)) {
                if (result instanceof PacketErrorResult) {
                    isOkay = false;
                }
                // XXX Should really process some more types of packets.
            }
        }
        catch (com.internetcds.jdbc.tds.TdsUnknownPacketSubType e) {
            throw new SQLException("Unknown response. " + e.getMessage());
        }
        catch (java.io.IOException e) {
            throw new SQLException("Network problem. " + e.getMessage());
        }
        catch (com.internetcds.jdbc.tds.TdsException e) {
            throw new SQLException(e.getMessage());
        }

        return isOkay;
    }


    /**
     *  This will read a error (or warning) message from the SQLServer and
     *  create a SqlMessage object from that message. <p>
     *
     *  <b>Warning!</b> This is not synchronized because it assumes it will only
     *  be called by processSubPacket() which is synchronized.
     *
     *@param  packetSubType                              type of the current
     *      subpacket
     *@return                                            The message returned by
     *      the SQLServer.
     *@exception  java.io.IOException                    Description of
     *      Exception
     *@exception  com.internetcds.jdbc.tds.TdsException  Description of
     *      Exception
     */
    private PacketMsgResult processMsg(byte packetSubType)
             throws java.io.IOException, com.internetcds.jdbc.tds.TdsException
    {
        SqlMessage msg = new SqlMessage();

        int len = getSubPacketLength();

        msg.number = comm.getTdsInt();

        msg.state = comm.getByte();

        msg.level = comm.getByte();
        // ?class?

        int msgLen = comm.getTdsShort();
        msg.message = comm.getString(msgLen);

        // RMK 2000-06-08: the getWarnings() methods aren't implemented, so we
        //                 need to do something with these.
        if (showWarnings && msg.message != null) {
            String warn = msg.message.trim();
            if (warn.length() > 0) {
                System.err.println("Server message: " + warn);
            }
        }

        int srvNameLen = comm.getByte() & 0xFF;
        msg.server = comm.getString(srvNameLen);

        if (packetSubType == TDS_MSG_TOKEN || packetSubType == TDS_ERR_TOKEN) {
            // nop
            int procNameLen = comm.getByte() & 0xFF;
            msg.procName = comm.getString(procNameLen);
        }
        else {
            throw new TdsConfused("Was expecting a msg or error token.  " +
                    "Found 0x" +
                    Integer.toHexString(packetSubType & 0xff));
        }

        msg.line = comm.getByte();

        // unknonw byte
        comm.getByte();

        lastServerMessage = msg;

        if (packetSubType == TDS_ERR_TOKEN) {
            return new PacketErrorResult(packetSubType, msg);
        }
        else {
            return new PacketMsgResult(packetSubType, msg);
        }
    }


    /**
     *  Process an env change message (TDS_ENV_CHG_TOKEN) <p>
     *
     *  <b>Warning!</b> This is not synchronized because it assumes it will only
     *  be called by processSubPacket() which is synchronized.
     *
     *@return                                            Description of the
     *      Returned Value
     *@exception  java.io.IOException
     *@exception  com.internetcds.jdbc.tds.TdsException
     */
    private PacketResult processEnvChange()
             throws java.io.IOException, com.internetcds.jdbc.tds.TdsException
    {
        final byte CHARSET_CHANGE = (byte) 3;

        int len = getSubPacketLength();
        int type = comm.getByte();
        switch (type) {
            case TDS_ENV_BLOCKSIZE: {
              String blocksize;
              int clen = comm.getByte() & 0xFF;
              if (tdsVer == TDS70) {
                blocksize = comm.getString(clen);
                comm.resizeOutbuf(Integer.parseInt(blocksize));
                comm.skip(len - 2 - clen * 2);
              }
              else {
                blocksize = encoder.getString(comm.getBytes(clen));
                comm.skip(len - 2 - clen);
              }
            }
            break;
            case CHARSET_CHANGE:
            {
                int clen = comm.getByte() & 0xFF;
                String charset;
                if (tdsVer == TDS70) {
                    charset = comm.getString(clen);
                    comm.skip(len - 2 - clen * 2);
                }
                else {
                    charset = encoder.getString(comm.getBytes(clen));
                    comm.skip(len - 2 - clen);
                }
                setCharset(charset);
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

        return new PacketResult( TDS_ENV_CHG_TOKEN );
    }


    /**
     *  Process an column name subpacket. <p>
     *
     *  <p>
     *
     *  <b>Warning!</b> This is not synchronized because it assumes it will only
     *  be called by processSubPacket() which is synchronized.
     *
     *@return                                            Description of the
     *      Returned Value
     *@exception  java.io.IOException                    Description of
     *      Exception
     *@exception  com.internetcds.jdbc.tds.TdsException  Description of
     *      Exception
     */
    private PacketColumnNamesResult processColumnNames()
             throws java.io.IOException, com.internetcds.jdbc.tds.TdsException
    {
        Columns columns = new Columns();

        int totalLen = comm.getTdsShort();

        int bytesRead = 0;
        int i = 0;
        while (bytesRead < totalLen) {
            int colNameLen = comm.getByte();
            String colName = encoder.getString(comm.getBytes(colNameLen));
            bytesRead = bytesRead + 1 + colNameLen;
            i++;
            columns.setName(i, colName);
            columns.setLabel(i, colName);
        }

        return new PacketColumnNamesResult(columns);
    }
    // processColumnNames()


    /**
     *  Process the columns information subpacket. <p>
     *
     *  <b>Warning!</b> This is not synchronized because it assumes it will only
     *  be called by processSubPacket() which is synchronized.
     *
     *@return                                            Description of the
     *      Returned Value
     *@exception  java.io.IOException                    Description of
     *      Exception
     *@exception  com.internetcds.jdbc.tds.TdsException  Description of
     *      Exception
     */
    private PacketColumnInfoResult processColumnInfo()
             throws java.io.IOException, com.internetcds.jdbc.tds.TdsException
    {
        Columns columns = new Columns();
        int precision;
        int scale;

        int totalLen = comm.getTdsShort();

        int bytesRead = 0;
        int numColumns = 0;
        while (bytesRead < totalLen) {
            scale = -1;
            precision = -1;

            int sizeOfColumn = -1;

            byte flagData[] = new byte[4];
            for (int i = 0; i < 4; i++) {
                flagData[i] = comm.getByte();
                bytesRead++;
            }
            boolean nullable = (flagData[2] & 0x01) > 0;
            boolean writeable = (flagData[2] & 0x08) > 0;
            boolean autoIncrement = (flagData[2] & 0x10) > 0;

            // Get the type of column
            byte columnType = comm.getByte();
            bytesRead++;

            if (columnType == SYBTEXT
                     || columnType == SYBIMAGE) {
                int i;
                int tmpByte;

                // XXX Need to find out what these next 4 bytes are
                //     Could they be the column size?
                comm.skip(4);
                bytesRead += 4;

                int tableNameLen = comm.getTdsShort();
                bytesRead += 2;
                String tableName = encoder.getString(comm.getBytes(tableNameLen));
                bytesRead += tableNameLen;

                sizeOfColumn = 2 << 31 - 1;
            }
            else if (columnType == SYBDECIMAL
                     || columnType == SYBNUMERIC) {
                int tmp;
                sizeOfColumn = comm.getByte();
                bytesRead++;
                precision = comm.getByte();
                // Total number of digits
                bytesRead++;
                scale = comm.getByte();
                // # of digits after the decimal point
                bytesRead++;
            }
            else if (isFixedSizeColumn(columnType)) {
                sizeOfColumn = lookupColumnSize(columnType);
            }
            else {
                sizeOfColumn = ((int) comm.getByte() & 0xff);
                bytesRead++;
            }
            numColumns++;

            if (scale != -1) {
                columns.setScale(numColumns, scale);
            }
            if (precision != -1) {
                columns.setPrecision(numColumns, precision);
            }
            columns.setNativeType(numColumns, columnType);
            columns.setDisplaySize(numColumns, sizeOfColumn);
            columns.setNullable(numColumns, (nullable
                     ? ResultSetMetaData.columnNullable
                     : ResultSetMetaData.columnNoNulls));
            columns.setAutoIncrement(numColumns, autoIncrement);
            columns.setReadOnly(numColumns, !writeable);
        }

        // Don't know what the rest is except that the
        int skipLen = totalLen - bytesRead;
        if (skipLen != 0) {
            throw new TdsException(
                    "skipping " + skipLen + " bytes");
        }

        return new PacketColumnInfoResult(columns);
    }
    // processColumnInfo



    private PacketTabNameResult processTabName()
             throws java.io.IOException, com.internetcds.jdbc.tds.TdsException
    {
        int totalLen = comm.getTdsShort();

        // RMK 2000-06-11.  Not sure why the original code is bothering
        // to extract the bytes with such meticulous care if it isn't
        // going to use the extracted strings for creating the returned
        // object.  At any rate, this approach doesn't work under TDS 7.0,
        // because (1) the name length is a short, not a byte under 7.0,
        // and (2) the name string is nameLen wide characters for 7.0,
        // not 8-bit characters.  So I'm commenting the wasted effort
        // and replacing it with a simple call to TdsComm.skip().
        //int bytesRead   = 0;
        //int nameLen     = 0;
        //String  tabName = null;

        //while(bytesRead < totalLen)
        //{
        //   nameLen = comm.getByte();
        //   bytesRead++;
        //   tabName = new String(comm.getBytes(nameLen));
        //   bytesRead += nameLen;
        //}

        comm.skip(totalLen);

        return new PacketTabNameResult();
    }
    // processTabName()



    /**
     *  Process an end subpacket. <p>
     *
     *  This routine assumes that the TDS_END_TOKEN byte has already been read.
     *
     *@param  packetType                                 Description of
     *      Parameter
     *@return
     *@exception  com.internetcds.jdbc.tds.TdsException
     *@exception  java.io.IOException                    Thrown if some sort of
     *      error occured reading bytes from the network.
     */
    private PacketEndTokenResult processEndToken(
            byte packetType)
             throws com.internetcds.jdbc.tds.TdsException, java.io.IOException
    {
        byte status = comm.getByte();
        comm.getByte();
        byte op = comm.getByte();
        comm.getByte();
        // comm.skip(3);
        int rowCount = comm.getTdsInt();
        if (op == (byte)0xC1)
          rowCount = 0;

        /*
        if (packetType == TdsDefinitions.TDS_DONEINPROC) {
            throw new TdsException("Internal error.  TDS_DONEINPROC "
                     + " is no longer considered an end token");
        }
         */
        if (packetType == TdsDefinitions.TDS_DONEPROC)
          rowCount = -1;

        PacketEndTokenResult result = new PacketEndTokenResult(packetType,
                status, rowCount);

        moreResults = result.moreResults();

        // XXX If we executed something that returns multiple result
        // sets then we don't want to clear the query in progress flag.
        // See the CancelController class for details.
        cancelController.finishQuery(result.wasCanceled(),
                result.moreResults());

        // XXX Problem handling cancels that were sent after the server
        //     send the endToken packet
        return result;
    }


    private PacketDoneInProcResult processDoneInProc(
            byte packetType)
             throws SQLException, TdsException, java.io.IOException
    {
        byte status = comm.getByte();
        comm.skip(3);
        int rowCount = comm.getTdsInt();

        PacketDoneInProcResult result = new PacketDoneInProcResult(packetType,
                status,
                rowCount);

        if (!result.moreResults()) {
            throw new TdsException("What? No more results with a DONEINPROC!");
        }

        if (result.moreResults() && peek() == TdsDefinitions.TDS_DONEINPROC) {
            result = (PacketDoneInProcResult) processSubPacket();
        }

        while (result.moreResults() &&
                (peek() == TdsDefinitions.TDS_PROCID
                 || peek() == TdsDefinitions.TDS_RET_STAT_TOKEN)) {
            if (peek() == TDS_PROCID) {
                PacketResult tmp = processSubPacket();
            }
            else if (peek() == TDS_RET_STAT_TOKEN) {
                PacketRetStatResult tmp = (PacketRetStatResult) processSubPacket();
                result.setRetStat(tmp.getRetStat());
            }
        }
        // XXX If we executed something that returns multiple result
        // sets then we don't want to clear the query in progress flag.
        // See the CancelController class for details.
        cancelController.finishQuery(result.wasCanceled(),
                result.moreResults());

        return result;
    }


    /**
     *  Find out how many bytes a particular SQLServer data type takes.
     *
     *@param  nativeColumnType
     *@return                                            number of bytes
     *      required by the given type
     *@exception  com.internetcds.jdbc.tds.TdsException  Thrown if the given
     *      type either doesn't exist or is a variable sized data type.
     */
    private int lookupColumnSize(byte nativeColumnType)
             throws com.internetcds.jdbc.tds.TdsException
    {
        switch (nativeColumnType) {
            case SYBINT1:
            {
                return 1;
            }
            case SYBINT2:
            {
                return 2;
            }
            case SYBINT4:
            {
                return 4;
            }
            case SYBREAL:
            {
                return 4;
            }
            case SYBFLT8:
            {
                return 8;
            }
            case SYBDATETIME:
            {
                return 8;
            }
            case SYBDATETIME4:
            {
                return 8;
            }
            case SYBBIT:
            {
                return 1;
            }
            case SYBMONEY:
            {
                return 8;
            }
            case SYBMONEY4:
            case SYBSMALLMONEY:
            {
                return 4;
            }
            default:
            {
                throw new TdsException("Not fixed size column "
                         + nativeColumnType);
            }
        }
    }


   private void writeDatetimeValue(Object value_in, int nativeType)
      throws TdsException, java.io.IOException
   {
      comm.appendByte((byte)nativeType);
      
      if (value_in == null)
      {
         comm.appendByte((byte)8);
         comm.appendByte((byte)0);
      }
      else
      {
         Timestamp value;
         if (value_in instanceof java.sql.Timestamp)
         {
            value = (Timestamp)value_in;
         }
         else
         {
            value = new Timestamp(((java.util.Date)value_in).getTime());
         }
         
         final int secondsPerDay = 24 * 60 * 60;
         final int msPerDay      = secondsPerDay * 1000;
         final int nsPerMs       = 1000 * 1000;
         final int msPerMinute   = 1000 * 60;
         // epochsDifference is the number of days between unix
         // epoch (1970 based) and the sybase epoch (1900 based)
         final int epochsDifference = 25567;

         long   nanoseconds = value.getNanos();

         // ms is the number of milliseconds into unix epoch

         long   ms = ((value.getTime() + (nanoseconds / nsPerMs))
                      + zoneOffset);
                ms += getDstOffset(ms); 
         long   msIntoCurrentDay  = ms % msPerDay;

         long   daysIntoUnixEpoch = (ms-msIntoCurrentDay)/msPerDay;

         int    jiffies = (int)((msIntoCurrentDay * 300) / 1000);
         int    daysIntoSybaseEpoch = (int)daysIntoUnixEpoch
            + epochsDifference;

         // If the number of seconds and milliseconds are set to zero, then
         // pass this as a smalldatetime parameter.  Otherwise, pass it as
         // a datetime parameter

         if ((msIntoCurrentDay % msPerMinute) == 0
            && daysIntoSybaseEpoch < Short.MAX_VALUE)
         {
            comm.appendByte((byte)4);
            comm.appendByte((byte)4);
            comm.appendTdsShort((short)daysIntoSybaseEpoch);
            comm.appendTdsShort((short)(msIntoCurrentDay / msPerMinute));
         }
         else
         {
            comm.appendByte((byte)8);
            comm.appendByte((byte)8);
            comm.appendTdsInt(daysIntoSybaseEpoch);
            comm.appendTdsInt(jiffies);
         }
      }
   }
   
   
   private void writeDecimalValue(byte nativeType, Object o, int scalePar, int precision)
      throws TdsException, java.io.IOException
   {
      comm.appendByte((byte)SYBDECIMAL);
      if (o == null) {
        comm.appendByte((byte)0);
        comm.appendByte((byte)28);
        comm.appendByte((byte)12);
        comm.appendByte((byte)0);
      }
      else {
        if (o instanceof Long) {
          long value = ((Long)o).longValue();
          comm.appendByte((byte)9);
          comm.appendByte((byte)28);
          comm.appendByte((byte)0);
          comm.appendByte((byte)9);
          if (value >= 0L) {
            comm.appendByte((byte)1);
          }
          else {
            comm.appendByte((byte)0);
            value = -value;
          }
          for (int valueidx = 0; valueidx<8; valueidx++) {
            // comm.appendByte((byte)(value & 0xFF));
            comm.appendByte((byte)(value & 0xFF));
            value >>>= 8;
          }
        }
        else {
          BigDecimal bd = (BigDecimal)o;
          byte scale = (byte)bd.scale();
          byte signum = (byte)(bd.signum() < 0 ? 0 : 1);
          BigInteger bi = bd.unscaledValue();
          long l = bi.longValue();
          byte[] mantisse = bi.abs().toByteArray();
          byte len = (byte)(mantisse.length + 1);
          byte prec = 28;
          comm.appendByte(len);
          comm.appendByte(prec);
          comm.appendByte(scale);
          comm.appendByte(len);
          comm.appendByte(signum);
          for (int mantidx = mantisse.length - 1; mantidx >= 0 ; mantidx--) {
            comm.appendByte(mantisse[mantidx]);
          }                                
        }
      }
                      /*
       BigDecimal d;
       if (value == null || value instanceof BigDecimal)
       {
           d = (BigDecimal)value;
       }
       else if (value instanceof BigInteger)
       {
           d = new BigDecimal((BigInteger)value, 0);
       }
       else if (value instanceof Number)
       {
           d = new BigDecimal(((Number)value).doubleValue());
       }
       else
       {
           throw new TdsException("Invalid decimal value");
       }

       byte[] data;
       int signum;
       if (value == null)
       {
           data = null;
           signum = 0;
           if (precision == -1)
           {
               precision = 10;
           }
       }
       else
       {
           BigInteger unscaled = d.movePointRight(scale).unscaledValue();
           BigInteger absunscaled = unscaled.abs();
           data = absunscaled.toByteArray();
           if (data.length > 0x11)
           {
               throw new TdsException("Decimal value too large");
           }
           signum = unscaled.signum();
           if (precision == -1)
           {
               precision = 10;
           }
           int bitLength = absunscaled.bitLength();
           int maxLength = (int)(1.0 + (float)bitLength * 0.30103);
           if (precision < maxLength)
           {
           	   precision = maxLength;
           }
       }
       comm.appendByte(nativeType);
       comm.appendByte((byte)0x11); // max length in bytes
       comm.appendByte((byte)precision); // precision
       comm.appendByte((byte)scale); // scale
       if (value == null)
       {
           comm.appendByte((byte)0);
       }
       else
       {
           comm.appendByte((byte)(data.length + 1));
           comm.appendByte(signum < 0 ? (byte)0 : (byte)1);
           comm.appendBytes(data);
       }
                       */
   }

    private Object readFloatN(int len)
             throws TdsException, java.io.IOException
    {
        Object tmp;

        switch (len) {
            case 8:
            {
                long l = comm.getTdsInt64();
                tmp = new Double(Double.longBitsToDouble(l));
                break;
            }
            case 4:
            {
                int i = comm.getTdsInt();
                tmp = new Float(Float.intBitsToFloat(i));
                break;
            }
            case 0:
            {
                tmp = null;
                break;
            }
            default:
            {
                throw new TdsNotImplemented("Don't now how to handle "
                         + "float with size of "
                         + len
                         + "(0x"
                         + Integer.toHexString(len & 0xff)
                         + ")");
            }
        }
        return tmp;
    }
    // getRow()


    private boolean createStoredProcedureNameTable()
    {
        boolean result = false;
        String sql = null;

        try {
            java.sql.Statement stmt = connection.createStatement();

            // ignore any of the exceptions thrown because they either
            // don't matter or they will make themselves known when we try
            // to use the name generator stored procedure.
            try {
                sql = ""
                         + "create table " + procNameTableName
                         + "(                                       "
                         + "     id        NUMERIC(10, 0) IDENTITY, "
                         + "     session   int not null,            "
                         + "     name      char(29) not null        "
                         + ")                                       ";
                stmt.executeUpdate(sql);
            }
            catch (java.sql.SQLException e) {
                // don't care
            }

            try {
                sql = ""
                         + "create procedure " + procNameGeneratorName + "        "
                         + "as                                                    "
                         + "begin tran                                            "
                         + "insert into " + procNameTableName + "                 "
                         + "    (session, name)                                   "
                         + " values                                               "
                         + "    (@@spid, '')                                      "
                         + "                                                      "
                         + "update " + procNameTableName + "                      "
                         + "  set name=('" + user + ".jdbctmpsp' +                "
                         + "            convert(varchar, @@IDENTITY))             "
                         + "  where id = @@IDENTITY                               "
                         + "                                                      "
                         + "select name from " + procNameTableName + "            "
                         + "   where id=@@IDENTITY                                "
                         + "                                                      "
                         + "commit tran                                           "
                         + "";

                stmt.execute(sql);
                stmt.execute("sp_procxmode " +
                        procNameGeneratorName +
                        ", 'anymode' ");
            }
            catch (java.sql.SQLException e) {
                // don't care
            }

            stmt = null;
        }
        catch (java.sql.SQLException e) {
            // don't care
        }
        return result;
    }


    private String generateUniqueProcName()
             throws java.sql.SQLException
    {
        java.sql.Statement stmt = connection.createStatement();

        boolean wasRs;

        wasRs = stmt.execute("exec " + procNameGeneratorName);
        if (!wasRs) {
            throw new java.sql.SQLException(
                    "Confused.  Was expecting a result set.");
        }

        java.sql.ResultSet rs;
        rs = stmt.getResultSet();
        if (!rs.next()) {
            throw new java.sql.SQLException("Couldn't get stored proc name");
        }
        return rs.getString(1);
    }


    /*
     * executeProcedure()
     */

    private void sendSybImage(
            byte[] value)
             throws java.io.IOException
    {
        int i;
        int length = (value == null ? 0 : value.length);

        // send the lenght of this piece of data
        comm.appendTdsInt(length);

        // send the length of this piece of data again
        comm.appendTdsInt(length);

        // send the data
        for (i = 0; i < length; i++) {
            comm.appendByte(value[i]);
        }
    }


    private void sendSybChar(
            String value,
            int maxLength)
             throws java.io.IOException
    {

        byte[] converted;
        if (value == null) {
            converted = new byte[0];
        }
        else {
            converted = encoder.getBytes(value);
        }


        // set the type of the column
        // set the maximum length of the field
        // set the actual lenght of the field.
        if (maxLength > 255 || converted.length > 255) {
            if (tdsVer != TDS70)
            {
               throw new java.io.IOException("String too long");
            }

            comm.appendByte((byte) (SYBVARCHAR | 0x80));
            comm.appendTdsShort((short) (maxLength));
            comm.appendTdsShort((short) (converted.length));
        }
        else {
            comm.appendByte(SYBVARCHAR);
            comm.appendByte((byte) (maxLength));
            comm.appendByte((byte) (converted.length));
        }

        comm.appendBytes(converted);
    }


    /**
     *  Process a login ack supacket
     *
     *@return                                            Description of the
     *      Returned Value
     *@exception  com.internetcds.jdbc.tds.TdsException  Description of
     *      Exception
     *@exception  java.io.IOException                    Description of
     *      Exception
     */
    private PacketResult processLoginAck()
             throws com.internetcds.jdbc.tds.TdsException, java.io.IOException
    {
        int len = getSubPacketLength();
        int bytesRead = 0;

        if (tdsVer == Tds.TDS70) {
            comm.skip(5);
            int nameLen = comm.getByte();
            databaseProductName = comm.getString(nameLen);
            databaseProductVersion = ("" + comm.getByte() + "."
                     + comm.getByte() + "."
                     + ((256 * comm.getByte()) + comm.getByte()));
        }
        else {
            comm.skip(5);
            short nameLen = comm.getByte();
            databaseProductName = comm.getString(nameLen);
            comm.skip(1);
            databaseProductVersion = ("" + comm.getByte() + "." + comm.getByte());
            comm.skip(1);
        }

        if (databaseProductName.length() > 1
                 && -1 != databaseProductName.indexOf('\0')) {
            int last = databaseProductName.indexOf('\0');
            databaseProductName = databaseProductName.substring(0, last);
        }

        return new PacketResult(TDS_LOGIN_ACK_TOKEN);
    }


    /**
     *  Process an proc id subpacket. <p>
     *
     *  This routine assumes that the TDS_PROCID byte has already been read.
     *
     *@return                                            Description of the
     *      Returned Value
     *@exception  com.internetcds.jdbc.tds.TdsException
     *@exception  java.io.IOException                    Thrown if some sort of
     *      error occured reading bytes from the network.
     */
    private PacketResult processProcId()
             throws java.io.IOException, com.internetcds.jdbc.tds.TdsException
    {
        // XXX Try to find out what meaning this subpacket has.
        int i;
        byte tmp;

        for (i = 0; i < 8; i++) {
            tmp = comm.getByte();
        }
        return new PacketResult(TDS_PROCID);
    }


    /**
     *  Process a TDS_RET_STAT_TOKEN subpacket. <p>
     *
     *  This routine assumes that the TDS_RET_STAT_TOKEN byte has already been
     *  read.
     *
     *@return                                            Description of the
     *      Returned Value
     *@exception  com.internetcds.jdbc.tds.TdsException
     *@exception  java.io.IOException                    Thrown if some sort of
     *      error occured reading bytes from the network.
     */
    private PacketRetStatResult processRetStat()
             throws java.io.IOException, com.internetcds.jdbc.tds.TdsException
    {
        // XXX Not completely sure of this.
        return new PacketRetStatResult(comm.getTdsInt());
    }


    /**
     *  Processes a TDS 7.0-style result packet, extracting column information
     *  for the result set. Added 2000-06-05.
     *
     *@return                                            Description of the
     *      Returned Value
     *@exception  java.io.IOException                    Description of
     *      Exception
     *@exception  com.internetcds.jdbc.tds.TdsException  Description of
     *      Exception
     */
    private PacketResult processTds7Result()
             throws java.io.IOException, com.internetcds.jdbc.tds.TdsException
    {
        int numColumns = comm.getTdsShort();
        Columns columns = new Columns();

        for (int colNum = 1; colNum <= numColumns; ++colNum) {

            /*
             * The freetds C code didn't know what to do with these four
             * bytes, but initial inspection appears to tentatively confirm
             * that they serve the same purpose as the flag bytes read by the
             * Java code for 4.2 column information.
             */
            byte flagData[] = new byte[4];
            for (int i = 0; i < 4; i++) {
                flagData[i] = comm.getByte();
            }
            boolean nullable = (flagData[2] & 0x01) > 0;
            boolean writeable = (flagData[2] & 0x08) > 0;
            boolean autoIncrement = (flagData[2] & 0x10) > 0;

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
            int colSize;
            if (isBlobType(columnType)) {

                // Text and image columns have 4-byte size fields.
                colSize = comm.getTdsInt();

                // Swallow table name.
                comm.getString(comm.getTdsShort());
            }

            // Fixed types have no size field in the packet.
            else if (isFixedSizeColumn((byte) columnType)) {
                colSize = lookupColumnSize((byte) columnType);
            }

            else if (isLargeType(xColType)) {
                colSize = comm.getTdsShort();
            }

            else {
                colSize = comm.getByte();
            }

            // Get precision, scale for decimal types.
            int precision = -1;
            int scale = -1;
            if (columnType == SYBDECIMAL || columnType == SYBNUMERIC) {
                precision = comm.getByte();
                scale = comm.getByte();
            }

            /*
             * NB: under 7.0 lengths are number of characters, not number of
             * bytes.  The getString() method handles this.
             */
            int colNameLen = comm.getByte();
            String columnName = comm.getString(colNameLen);

            // Populate the Column object.
            columns.setNativeType(colNum, columnType);
            columns.setName(colNum, columnName);
            columns.setLabel(colNum, columnName);
            columns.setDisplaySize(colNum, colSize);
            columns.setNullable(colNum, (nullable
                     ? ResultSetMetaData.columnNullable
                     : ResultSetMetaData.columnNoNulls));
            columns.setAutoIncrement(colNum, autoIncrement);
            columns.setReadOnly(colNum, !writeable);
            if (precision != -1) {
                columns.setPrecision(colNum, precision);
            }
            if (scale != -1) {
                columns.setScale(colNum, scale);
            }
        }

        return new PacketColumnNamesResult(columns);
    }


    private void waitForDataOrTimeout(
            java.sql.Statement stmt,
            int timeout)
             throws java.io.IOException, com.internetcds.jdbc.tds.TdsException
    {
        // XXX How should this be syncrhonized?

        if (timeout == 0 || stmt == null) {
            comm.peek();
        }
        else {
            // start the timeout thread
            TimeoutHandler t = new TimeoutHandler(stmt, timeout);

            t.start();

            // wait until there is at least one byte of data
            comm.peek();

            // kill the timeout thread
            t.interrupt();

            t = null;
        }
    }


    /**
     *  Convert a JDBC escaped SQL string into the native SQL
     *
     *@param  input             escaped string to convert
     *@param  serverType        Description of Parameter
     *@return                   native SQL string
     *@exception  SQLException  Description of Exception
     */
    public static String toNativeSql(String input, int serverType)
             throws SQLException
    {
        EscapeProcessor escape;
        if (serverType == TdsDefinitions.SYBASE) {
            escape = new SybaseEscapeProcessor(input);
        }
        else {
            escape = new MSSqlServerEscapeProcessor(input);
        }

        return escape.nativeString();
    }


    /**
     *  Convert a JDBC java.sql.Types identifier to a SQLServer type identifier
     *
     *@param  jdbcType               JDBC type to convert. Should be one of the
     *      constants from java.sql.Types.
     *@return                        The corresponding SQLServer type
     *      identifier.
     *@exception  TdsNotImplemented  Description of Exception
     *@author                        Craig Spannring
     */
    public static byte cvtJdbcTypeToNativeType(int jdbcType)
             throws TdsNotImplemented
    {
        // This function is thread safe.
        byte result = 0;
        switch (jdbcType) {
            case java.sql.Types.CHAR:
            case java.sql.Types.VARCHAR:
            case java.sql.Types.LONGVARCHAR:
            {
                result = SYBCHAR;
                break;
            }
            case java.sql.Types.TINYINT:
            {
                result = SYBINT1;
                break;
            }
            case java.sql.Types.SMALLINT:
            {
                result = SYBINT2;
                break;
            }
            case java.sql.Types.INTEGER:
            {
                result = SYBINT4;
                break;
            }
            case java.sql.Types.BIT: 
            {
                result = SYBBIT;
                break;
            }
            case java.sql.Types.FLOAT:
            case java.sql.Types.REAL:
            case java.sql.Types.DOUBLE:
            {
                result = SYBFLT8;
                break;
            }
            case java.sql.Types.DATE:
            case java.sql.Types.TIMESTAMP:
            {
                result = SYBDATETIMN;
                break;
            }
            case java.sql.Types.BINARY:
            {
                result = SYBBINARY;
                break;
            }
            case java.sql.Types.VARBINARY:
            {
                result = SYBVARBINARY;
                break;
            }
              
            case java.sql.Types.LONGVARBINARY:
            {
                result = SYBIMAGE;
                break;
            }
            case java.sql.Types.BIGINT:
            {
              result = SYBDECIMAL;
              break;
            }
            case java.sql.Types.NUMERIC:
            {
              result = SYBNUMERIC;
              break;
            }
            case java.sql.Types.DECIMAL:
            {
              result = SYBDECIMAL;
              break;
            }
            default:
            {
                throw new TdsNotImplemented("cvtJdbcTypeToNativeType ("
                         + TdsUtil.javaSqlTypeToString(jdbcType) + ")");
            }
        }
        return result;
    }


    /**
     *  Convert a JDBC java.sql.Types identifier to a SQLServer type identifier
     *
     *@param  nativeType        SQLServer type to convert.
     *@param  size              Maximum size of data coming back from server.
     *@return                   The corresponding JDBC type identifier.
     *@exception  TdsException  Description of Exception
     *@author                   Craig Spannring
     */
    public static int cvtNativeTypeToJdbcType(int nativeType,
            int size)
             throws TdsException
    {
        // This function is thread safe.

        int result = java.sql.Types.OTHER;
        switch (nativeType) {
            // XXX We need to figure out how to map _all_ of these types
            case SYBBINARY:
                result = java.sql.Types.BINARY;
                break;
            case SYBBIT:
                result = java.sql.Types.BIT;
                break;
            case SYBBITN:
                result = java.sql.Types.BIT;
                break;
            case SYBCHAR:
                result = java.sql.Types.CHAR;
                break;
            case SYBNCHAR:
                result = java.sql.Types.CHAR;
                break;
            case SYBDATETIME4:
                result = java.sql.Types.TIMESTAMP;
                break;
            case SYBDATETIME:
                result = java.sql.Types.TIMESTAMP;
                break;
            case SYBDATETIMN:
                result = java.sql.Types.TIMESTAMP;
                break;
            case SYBDECIMAL:
                result = java.sql.Types.DECIMAL;
                break;
            case SYBNUMERIC:
                result = java.sql.Types.NUMERIC;
                break;
            case SYBFLT8:
                result = java.sql.Types.DOUBLE;
                break;
            case SYBFLTN:
                result = java.sql.Types.DOUBLE;
                break;
            case SYBINT1:
                result = java.sql.Types.TINYINT;
                break;
            case SYBINT2:
                result = java.sql.Types.SMALLINT;
                break;
            case SYBINT4:
                result = java.sql.Types.INTEGER;
                break;
            case SYBINTN:
            {
                switch (size) {
                    case 1:
                        result = java.sql.Types.TINYINT;
                        break;
                    case 2:
                        result = java.sql.Types.SMALLINT;
                        break;
                    case 4:
                        result = java.sql.Types.INTEGER;
                        break;
                    default:
                        throw new TdsException("Bad size of SYBINTN");
                }
                break;
            }
            // XXX Should money types by NUMERIC or OTHER?
            case SYBSMALLMONEY:
                result = java.sql.Types.NUMERIC;
                break;
            case SYBMONEY4:
                result = java.sql.Types.NUMERIC;
                break;
            case SYBMONEY:
                result = java.sql.Types.NUMERIC;
                break;
            case SYBMONEYN:
                result = java.sql.Types.NUMERIC;
                break;
//         case SYBNUMERIC:      result = java.sql.Types.NUMERIC;     break;
            case SYBREAL:
                result = java.sql.Types.REAL;
                break;
            case SYBTEXT:
                result = java.sql.Types.LONGVARCHAR;
                break;
            case SYBNTEXT:
                result = java.sql.Types.LONGVARCHAR;
                break;
            case SYBIMAGE:
                // should be : result = java.sql.Types.LONGVARBINARY;
              result = java.sql.Types.VARBINARY;   // work around XXXX
                break;
            case SYBVARBINARY:
                result = java.sql.Types.VARBINARY;
                break;
            case SYBVARCHAR:
                result = java.sql.Types.VARCHAR;
                break;
            case SYBNVARCHAR:
                result = java.sql.Types.VARCHAR;
                break;
//         case SYBVOID:         result = java.sql.Types. ; break;
            default:
                throw new TdsException("Unknown native data type "
                         + Integer.toHexString(
                        nativeType & 0xff));
        }
        return result;
    }
    // processTds7Result()


    /**
     *  Reports whether the type is for a large object. Name is a bit of a
     *  misnomer, since it returns true for large text types, not just binary
     *  objects (took it over from the freetds C code).
     *
     *@param  type  Description of Parameter
     *@return       The BlobType value
     */
    private static boolean isBlobType(int type)
    {
        return type == SYBTEXT || type == SYBIMAGE || type == SYBNTEXT;
    }


    /**
     *  Reports whether the type uses a 2-byte size value.
     *
     *@param  type  Description of Parameter
     *@return       The LargeType value
     */
    private static boolean isLargeType(int type)
    {
        return type == SYBNCHAR || type > 128;
    }


    private static int toUInt(byte b)
    {
        int result = ((int) b) & 0x00ff;
        return result;
    }


    /**
     *  This is a <B>very</B> poor man's "encryption."
     *
     *@param  pw  Description of Parameter
     *@return     Description of the Returned Value
     */
    private static String tds7CryptPass(String pw)
    {
        int xormask = 0x5A5A;
        int len = pw.length();
        char[] chars = new char[len];
        for (int i = 0; i < len; ++i) {
            int c = (int) (pw.charAt(i)) ^ xormask;
            int m1 = (c >> 4) & 0x0F0F;
            int m2 = (c << 4) & 0xF0F0;
            chars[i] = (char) (m1 | m2);
        }
        return new String(chars);
    }
    void commit()
    {
      proceduresOfTra.clear();
    }

    boolean rollback()
    {
      boolean oncemore = false;
      try {
        Iterator it = proceduresOfTra.iterator();
        while (it.hasNext()) {
          Procedure p = (Procedure)it.next();
          String sql  = p.getPreparedSqlString();
          submitProcedure(sql, new SQLWarningChain());
          oncemore = true;
        }
        if (oncemore)
          submitProcedure("IF @@TRANCOUNT > 0 COMMIT TRAN ", new SQLWarningChain());
        proceduresOfTra.clear();
      }
      catch (Exception e) {
        Iterator it = proceduresOfTra.iterator();
        while (it.hasNext()) {
          Procedure p = (Procedure)it.next();
          String sql  = p.getPreparedSqlString();
          procedureCache.remove(p.rawQueryString);
        }
        oncemore = false;
        proceduresOfTra.clear();
      }
      return oncemore;
    }

    void skipToEnd()
      throws java.sql.SQLException, java.io.IOException,
        com.internetcds.jdbc.tds.TdsUnknownPacketSubType,
        com.internetcds.jdbc.tds.TdsException
    {
       boolean       done;
       PacketResult  tmp;
        if (moreResults)
       do
       {
          tmp = processSubPacket();
          done = (tmp instanceof PacketEndTokenResult)
             && (! ((PacketEndTokenResult)tmp).moreResults());
       } while (! done);
    }
}
