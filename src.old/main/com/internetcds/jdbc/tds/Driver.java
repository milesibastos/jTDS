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



package com.internetcds.jdbc.tds;

import java.sql.*;
import java.util.*;
import com.internetcds.jdbc.tds.TdsException;


/**
 *  <P>
 *
 *  The Java SQL framework allows for multiple database drivers. <P>
 *
 *  Each driver should supply a class that implements the Driver interface. <P>
 *
 *  The DriverManager will try to load as many drivers as it can find and then
 *  for any given connection request, it will ask each driver in turn to try to
 *  connect to the target URL. <P>
 *
 *  It is strongly recommended that each Driver class should be small and
 *  standalone so that the Driver class can be loaded and queried without
 *  bringing in vast quantities of supporting code. <P>
 *
 *  When a Driver class is loaded, it should create an instance of itself and
 *  register it with the DriverManager. This means that a user can load and
 *  register a driver by doing Class.forName("foo.bah.Driver").
 *
 *@author     Craig Spannring
 *@author     Igor Petrovski
 *@created    March 16, 2001
 *@version    $Id: Driver.java,v 1.7 2002-10-02 11:57:53 alin_sinpalean Exp $
 *@see        Connection
 */
public class Driver implements java.sql.Driver {
    /**
     *  Description of the Field
     */
    public final static String cvsVersion = "$Id: Driver.java,v 1.7 2002-10-02 11:57:53 alin_sinpalean Exp $";

    final static boolean debug = false;
    final static String oldSQLServerUrlPrefix = "jdbc:freetds://";
    final static String newSQLServerUrlPrefix = "jdbc:freetds:sqlserver://";
    final static String sybaseUrlPrefix = "jdbc:freetds:sybase://";
    final static String defaultSQLServerPort = "1433";
    final static String defaultSybasePort = "7100";


    /**
     *  Construct a new driver and register it with DriverManager
     */
    public Driver()
    {
    }


    /**
     *  <p>
     *
     *  The getPropertyInfo method is intended to allow a generic GUI tool to
     *  discover what properties it should prompt a human for in order to get
     *  enough information to connect to a database. Note that depending on the
     *  values the human has supplied so far, additional values may become
     *  necessary, so it may be necessary to iterate though several calls to
     *  getPropertyInfo.
     *
     *@param  Url               Description of Parameter
     *@param  Info              Description of Parameter
     *@return                   An array of DriverPropertyInfo objects
     *      describing possible properties. This array may be an empty array if
     *      no properties are required.
     *@exception  SQLException  if a database-access error occurs.
     */
    public DriverPropertyInfo[] getPropertyInfo(String Url, Properties Info)
             throws SQLException
    {
        DriverPropertyInfo result[] = new DriverPropertyInfo[0];

        return result;
    }


    /**
     *  Gets the drivers major version number
     *
     *@return    the drivers major version number
     */
    public int getMajorVersion()
    {
        return DriverVersion.getDriverMajorVersion();
    }


    /**
     *  Get the driver's minor version number. Initially this should be 0.
     *
     *@return    The MinorVersion value
     */
    public int getMinorVersion()
    {
        return DriverVersion.getDriverMinorVersion();
    }


    /**
     *  Try to make a database connection to the given URL. The driver should
     *  return "null" if it realizes it is the wrong kind of driver to connect
     *  to the given URL. This will be common, as when the JDBC driverManager is
     *  asked to connect to a given URL, it passes the URL to each loaded driver
     *  in turn. <p>
     *
     *  The driver should raise an SQLException if it is the right driver to
     *  connect to the given URL, but has trouble connecting to the database.
     *  <p>
     *
     *  The java.util.Properties argument can be used to pass arbitrary string
     *  tag/value pairs as connection arguments. This driver handles URLs of the
     *  form: <PRE>
     *
     *  jdbc:freetds://servername/database
     *  jdbc:freetds://servername:port/database
     *  jdbc:freetds:sqlserver://servername/database
     *  jdbc:freetds:sqlserver://servername:port/database
     *  jdbc:freetds:sybase://servername/database
     *  jdbc:freetds:sybase://servername:port/database</PRE> <p>
     *
     *
     *  <table>
     *
     *    <thead>
     *      Recognized Properties
     *    </thead>
     *    <tbody>
     *    <tr>
     *
     *      <td>
     *        PROGNAME
     *        <td>
     *          Send this name to server to identify the program
     *        </tr>
     *
     *        <tr>
     *
     *          <td>
     *            APPNAME
     *            <td>
     *              Send this name to server to identify the app
     *            </tr>
     *            </tbody>
     *          </table>
     *
     *
     *@param  info              a list of arbitrary tag/value pairs as
     *      connection arguments
     *@param  Url               Description of Parameter
     *@return                   a connection to the URL or null if it isnt us
     *@exception  SQLException  if a database access error occurs
     *@see                      java.sql.Driver#connect
     */
    public java.sql.Connection connect(String Url, Properties info)
             throws SQLException
    {
        java.sql.Connection result = null;

        if (!parseUrl(Url, info)) {
            return null;
        }
        else {
            try {
                result = new TdsConnection(info);
            }
            catch (NumberFormatException e) {
                throw new SQLException("NumberFormatException converting port number");
            }
            catch (com.internetcds.jdbc.tds.TdsException e) {
                throw new SQLException(e.getMessage());
            }
        }
        return result;
    }


    /**
     *  Returns true if the driver thinks it can open a connection to the given
     *  URL. Typically, drivers will return true if they understand the
     *  subprotocol specified in the URL and false if they don't. This driver's
     *  protocols start with jdbc:freetds: This driver handles URLs of the form:
     *  <PRE>
     *
     *  jdbc:freetds://host:port/database</PRE> or <PRE>
     *
     *  jdbc:freetds://host/database</PRE> <PRE>
     *
     *  jdbc:freetds:sqlserver://host:port/database</PRE> or <PRE>
     *
     *  jdbc:freetds:sqlserver://host/database</PRE> <PRE>
     *
     *  jdbc:freetds:sybase://host:port/database</PRE> or <PRE>
     *
     *  jdbc:freetds:sybase://host/database</PRE>
     *
     *@param  url               the URL of the driver
     *@return                   true if this driver accepts the given URL
     *@exception  SQLException  if a database-access error occurs
     *@see                      java.sql.Driver#acceptsURL
     */
    public boolean acceptsURL(String url) throws SQLException
    {
        boolean result = parseUrl(url, new Properties());
        return result;
    }


    /**
     *  Report whether the Driver is a genuine JDBC COMPLIANT (tm) driver. A
     *  driver may only report "true" here if it passes the JDBC compliance
     *  tests, otherwise it is required to return false. JDBC compliance
     *  requires full support for the JDBC API and full support for SQL 92 Entry
     *  Level. It is expected that JDBC compliant drivers will be available for
     *  all the major commercial databases. This method is not intended to
     *  encourage the development of non-JDBC compliant drivers, but is a
     *  recognition of the fact that some vendors are interested in using the
     *  JDBC API and framework for lightweight databases that do not support
     *  full database functionality, or for special databases such as document
     *  information retrieval where a SQL implementation may not be feasible.
     *
     *@return    Description of the Returned Value
     */
    public boolean jdbcCompliant()
    {
        // :-(  MS SQLServer 6.5 doesn't provide what JDBC wants.
        // See DatabaseMetaData.nullPlusNonNullIsNull() for more details.
        // XXX Need to check if Sybase could be jdbcCompliant
        return false;
    }


    /**
     *  Parses the properties specified in <i>url</i> and adds them to <i>result
     *  </i>.
     *
     *@param  url     Description of Parameter
     *@param  result  Description of Parameter
     *@return         true if the URL could be parsed successfully.
     */
    protected boolean parseUrl(String url, Properties result)
    {
        String tmpUrl = url;
        int serverType = -1;

        if (tmpUrl.startsWith(oldSQLServerUrlPrefix) ||
                tmpUrl.startsWith(newSQLServerUrlPrefix) ||
                tmpUrl.startsWith(sybaseUrlPrefix)) {
            if (tmpUrl.startsWith(oldSQLServerUrlPrefix)) {
                serverType = Tds.SQLSERVER;
                tmpUrl = tmpUrl.substring(oldSQLServerUrlPrefix.length());
            }
            else if (tmpUrl.startsWith(newSQLServerUrlPrefix)) {
                serverType = Tds.SQLSERVER;
                tmpUrl = tmpUrl.substring(newSQLServerUrlPrefix.length());
            }
            else if (tmpUrl.startsWith(sybaseUrlPrefix)) {
                serverType = Tds.SYBASE;
                tmpUrl = url.substring(sybaseUrlPrefix.length());
            }

            try {
                StringTokenizer tokenizer = new StringTokenizer(tmpUrl, ":/;",
                        true);
                String tmp;
                String host = null;
                String port = (serverType == Tds.SYBASE
                         ? defaultSybasePort
                         : defaultSQLServerPort);
                String database = "";

                // Get the hostname
                host = tokenizer.nextToken();

                if( tokenizer.hasMoreTokens() )
                {
                    // Find the port if it has one.
                    tmp = tokenizer.nextToken();
                    if (tmp.equals(":")) {
                        port = tokenizer.nextToken();
                        // Skip the '/' character
                        if (tokenizer.hasMoreTokens())
                            tmp = tokenizer.nextToken();
                    }

                    if( tmp.equals("/") ) {
                        // find the database name
                        database = tokenizer.nextToken();
                        if (tokenizer.hasMoreTokens()) {
                            tmp = tokenizer.nextToken();
                        }
                    }

                    // XXX The next loop is a bit too permisive.
                    while (tmp.equals(";")) {
                        // Extract the additional attribute.
                        String extra = tokenizer.nextToken();
                        StringTokenizer tok2 = new StringTokenizer(extra, "=", false);
                        String key = tok2.nextToken().toUpperCase();
                        if (tok2.hasMoreTokens()) {
                            result.put(key, tok2.nextToken());
                        }

                        if (tokenizer.hasMoreTokens()) {
                            tmp = tokenizer.nextToken();
                        }
                        else {
                            break;
                        }
                    }
                }

                // if there are anymore tokens then don't recognoze this URL
                if ((!tokenizer.hasMoreTokens())
                         && isValidHostname(host)
                         && database != null) {
                    result.put(Tds.PROP_HOST, host);
                    result.put(Tds.PROP_SERVERTYPE, "" + serverType);
                    result.put(Tds.PROP_PORT, port);
                    result.put(Tds.PROP_DBNAME, database);
                }
                else {
                    return false;
                }
            }
            catch (NoSuchElementException e) {
                return false;
            }
        }
        else {
            return false;
        }

        return true;
    }

    private boolean isValidHostname(String host)
    {
        return true;
        // XXX
    }

    //
    // Register ourselves with the DriverManager
    //
    static {
        try {
            java.sql.DriverManager.registerDriver(new Driver());
        }
        catch (SQLException E) {
            E.printStackTrace();
        }
    }

}
