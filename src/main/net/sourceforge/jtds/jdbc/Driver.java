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
import java.util.*;

/**
 * @author     Craig Spannring
 * @author     Igor Petrovski
 * @author     Alin Sinpalean
 * @created    March 16, 2001
 * @version    $Id: Driver.java,v 1.2 2004-01-22 23:49:35 alin_sinpalean Exp $
 * @see        Connection
 */
public class Driver implements java.sql.Driver
{
    public final static String cvsVersion = "$Id: Driver.java,v 1.2 2004-01-22 23:49:35 alin_sinpalean Exp $";

    final static boolean debug = false;
    final static String oldSQLServerUrlPrefix = "jdbc:jtds://";
    final static String newSQLServerUrlPrefix = "jdbc:jtds:sqlserver://";
    final static String sybaseUrlPrefix = "jdbc:jtds:sybase://";
    final static String defaultSQLServerPort = "1433";
    final static String defaultSybasePort = "7100";

    public Driver() throws SQLException
    {
    }

    /** @todo Implement this method. */
    public DriverPropertyInfo[] getPropertyInfo(String Url, Properties Info) throws SQLException
    {
        DriverPropertyInfo result[] = new DriverPropertyInfo[0];
        return result;
    }

    public int getMajorVersion()
    {
        return DriverVersion.getDriverMajorVersion();
    }

    public int getMinorVersion()
    {
        return DriverVersion.getDriverMinorVersion();
    }

    public java.sql.Connection connect(String Url, Properties info) throws SQLException
    {
        if( !parseUrl(Url, info) )
            return null;
        else
            try
            {
                info = processProperties(info);
                return new TdsConnection(info);
            }
            catch (NumberFormatException e)
            {
                throw new SQLException("NumberFormatException converting port number.");
            }
            catch (net.sourceforge.jtds.jdbc.TdsException e)
            {
                throw new SQLException(e.getMessage());
            }
    }

    public boolean acceptsURL(String url) throws SQLException
    {
        return parseUrl(url, new Properties());
    }

    public boolean jdbcCompliant()
    {
        // :-(  MS SQLServer 6.5 doesn't provide what JDBC wants.
        // See DatabaseMetaData.nullPlusNonNullIsNull() for more details.
        // XXX Need to check if Sybase could be jdbcCompliant
        return false;
    }

    protected boolean parseUrl(String url, Properties result)
    {
        String tmpUrl = url;
        int serverType = -1;

        if( tmpUrl.startsWith(oldSQLServerUrlPrefix) ||
            tmpUrl.startsWith(newSQLServerUrlPrefix) ||
            tmpUrl.startsWith(sybaseUrlPrefix))
        {
            if( tmpUrl.startsWith(oldSQLServerUrlPrefix) )
            {
                serverType = Tds.SQLSERVER;
                tmpUrl = tmpUrl.substring(oldSQLServerUrlPrefix.length());
            }
            else if( tmpUrl.startsWith(newSQLServerUrlPrefix) )
            {
                serverType = Tds.SQLSERVER;
                tmpUrl = tmpUrl.substring(newSQLServerUrlPrefix.length());
            }
            else if( tmpUrl.startsWith(sybaseUrlPrefix) )
            {
                serverType = Tds.SYBASE;
                tmpUrl = url.substring(sybaseUrlPrefix.length());
            }

            try
            {
                StringTokenizer tokenizer = new StringTokenizer(tmpUrl, ":/;", true);
                String tmp;
                String host = null;
                String port = serverType==Tds.SYBASE ? defaultSybasePort : defaultSQLServerPort;
                String database = "";

                // Get the hostname
                host = tokenizer.nextToken();

                if( tokenizer.hasMoreTokens() )
                {
                    // Find the port if it has one.
                    tmp = tokenizer.nextToken();
                    if( tmp.equals(":") )
                    {
                        port = tokenizer.nextToken();
                        // Skip the '/' character
                        if( tokenizer.hasMoreTokens() )
                            tmp = tokenizer.nextToken();
                    }

                    if( tokenizer.hasMoreTokens() && tmp.equals("/") )
                    {
                        // find the database name
                        database = tokenizer.nextToken();
                        if( tokenizer.hasMoreTokens() )
                            tmp = tokenizer.nextToken();
                    }

                    // XXX The next loop is a bit too permisive.
                    while( tmp.equals(";") )
                    {
                        // Extract the additional attribute.
                        String extra = tokenizer.nextToken();
                        StringTokenizer tok2 = new StringTokenizer(extra, "=", false);
                        String key = tok2.nextToken().toUpperCase();

                        if( tok2.hasMoreTokens() )
                            result.put(key, tok2.nextToken());

                        if( tokenizer.hasMoreTokens() )
                            tmp = tokenizer.nextToken();
                        else
                            break;
                    }
                }

                // if there are anymore tokens then don't recognoze this URL
                if( !tokenizer.hasMoreTokens() && isValidHostname(host) )
                {
                    result.put(Tds.PROP_HOST, host);
                    result.put(Tds.PROP_SERVERTYPE, ""+serverType);
                    result.put(Tds.PROP_PORT, port);
                    result.put(Tds.PROP_DBNAME, database);
                }
                else
                    return false;

                return true;
            }
            catch (NoSuchElementException e)
            {
                return false;
            }
        }
        else
            return false;
    }

    private boolean isValidHostname(String host)
    {
        return host != null;
    }

    /**
     * Returns a <code>Properties</code> instance with the keys "uppercased".
     * The idea is to make them easier to read by the inner classes.
     *
     * @param props the input <code>Properties</code>
     * @return      the "same" <code>Properties</code> with uppercase keys
     */
    static Properties processProperties(Properties props)
    {
        Properties res = null;

        if( props != null )
        {
            res = new Properties();

            for( Enumeration e=props.keys(); e.hasMoreElements(); )
            {
                String key = e.nextElement().toString();
                res.setProperty(key.toUpperCase(), props.getProperty(key));
            }
        }

        return res;
    }

    // Register ourselves with the DriverManager
    static
    {
        try
        {
            java.sql.DriverManager.registerDriver(new Driver());
        }
        catch( SQLException ex )
        {
        }
    }
}
