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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Properties;

/**
 * @author     Craig Spannring
 * @author     Igor Petrovski
 * @author     Alin Sinpalean
 * @created    March 16, 2001
 * @version    $Id: Driver.java,v 1.4 2004-01-31 00:00:15 bheineman Exp $
 * @see        Connection
 */
public class Driver implements java.sql.Driver {
    public final static String cvsVersion = "$Id: Driver.java,v 1.4 2004-01-31 00:00:15 bheineman Exp $";

    private final static String DEFAULT_SQL_SERVER_PORT = "1433";
    private final static String DEFAULT_SYBASE_PORT = "7100";

    // Register ourselves with the DriverManager
    static {
        try {
            DriverManager.registerDriver(new Driver());
        } catch (SQLException e) {
        }
    }

    /**
     * Constructs a new driver instance.
     */
    public Driver() throws SQLException {
    }

    /**
     * Returns <code>true</code> if this driver can connect to the url specified;
     * returns <code>false</code> otherwise.
     * 
     * @param url the JDBC url to used to determine if a connection can be made
     */
    public boolean acceptsURL(String url) throws SQLException {
        return parseUrl(url, null);
    }

    /**
     * Attempts to establish a database connection with the url and info specified.
     */
    public Connection connect(String url, Properties info) throws SQLException {
        if (info == null) {
            info = new Properties();
        } else {
            info = processProperties(info);
        }

        if (!parseUrl(url, info)) {
            return null;
        } else {
            try {
                return new TdsConnection(info);
            } catch (NumberFormatException e) {
                throw new SQLException("NumberFormatException converting port number.");
            } catch (TdsException e) {
                throw new SQLException(e.getMessage());
            }
        }
    }

    /**
     * Returns the drivers Major version.
     */
    public int getMajorVersion() {
        return DriverVersion.getDriverMajorVersion();
    }

    /**
     * Returns the drivers Minor version.
     */
    public int getMinorVersion() {
        return DriverVersion.getDriverMinorVersion();
    }

    /**
     * Returns an array of driver properties.
     */
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
    throws SQLException {
        DriverPropertyInfo[] dpi = new DriverPropertyInfo[] {
            new DriverPropertyInfo(Tds.PROP_SERVERTYPE, null),
            new DriverPropertyInfo(Tds.PROP_HOST, null),
            new DriverPropertyInfo(Tds.PROP_PORT, null),
            new DriverPropertyInfo(Tds.PROP_DBNAME, null),
            new DriverPropertyInfo(Tds.PROP_USER, null),
            new DriverPropertyInfo(Tds.PROP_PASSWORD, null),
            new DriverPropertyInfo(Tds.PROP_TDS, null),
            new DriverPropertyInfo(Tds.PROP_DOMAIN, null),
            new DriverPropertyInfo(Tds.PROP_INSTANCE, null),
            new DriverPropertyInfo(Tds.PROP_USEUNICODE, null)
        };

        if (info == null) {
            info = new Properties();
        } else {
            info = processProperties(info);
        }

        // Populate the properties.
        parseUrl(url, info);

        for (int i = 0; i < dpi.length; i++) {
            String name = dpi[i].name;
            Object tmpValue = info.get(name);

            if (tmpValue != null) {
                dpi[i].value = tmpValue.toString();
            }

            if (name.equals(Tds.PROP_SERVERTYPE)) {
                dpi[i].description = "The type of database.";
                dpi[i].required = true;
                dpi[i].choices = new String[] {
                    String.valueOf(Tds.SQLSERVER),
                    String.valueOf(Tds.SYBASE)
                };

                if (dpi[i].value == null) {
                    dpi[i].value = dpi[i].choices[0]; // Tds.SQLSERVER
                }
            } else if (name.equals(Tds.PROP_HOST)) {
                dpi[i].description = "The database hostname.";
                dpi[i].required = true;
            } else if (name.equals(Tds.PROP_PORT)) {
                dpi[i].description = "The database port.";

                if (dpi[i].value == null) {
                    if (String.valueOf(Tds.SYBASE).equalsIgnoreCase(String.valueOf(info.get(Tds.PROP_SERVERTYPE)))) {
                        dpi[i].value = DEFAULT_SYBASE_PORT;
                    } else {
                        dpi[i].value = DEFAULT_SQL_SERVER_PORT;
                    }
                }
            } else if (name.equals(Tds.PROP_DBNAME)) {
                dpi[i].description = "The database name.";

                if (dpi[i].value == null) {
                    dpi[i].value = "master";
                }
            } else if (name.equals(Tds.PROP_USER)) {
                dpi[i].description = "The database user.";
            } else if (name.equals(Tds.PROP_PASSWORD)) {
                dpi[i].description = "The database password.";
            } else if (name.equals(Tds.PROP_TDS)) {
                dpi[i].description = "The database protocol.";
                dpi[i].choices = new String[] {
                    String.valueOf(Tds.TDS42),
                    String.valueOf(Tds.TDS50),
                    String.valueOf(Tds.TDS70)
                };

                if (dpi[i].value == null) {
                    dpi[i].value = dpi[i].choices[2]; // Tds.TDS70
                }
            } else if (name.equals(Tds.PROP_DOMAIN)) {
                dpi[i].description = "The domain used for authentication.";
            } else if (name.equals(Tds.PROP_INSTANCE)) {
                dpi[i].description = "The database instance.";
            } else if (name.equals(Tds.PROP_USEUNICODE)) {
                dpi[i].description = "If strings should be sent as unicode values.";
                dpi[i].choices = new String[] {"true","false"};

                if (dpi[i].value == null) {
                    dpi[i].value = dpi[i].choices[0]; // true
                }
            }
        }

        return dpi;
    }

    /**
     * Returns <code>true</code> if the driver is JDBC 1.0 compliant and provides
     * full support for SQL-92 Entry Level; returns <code>false</code> otherwise.
     */
    public boolean jdbcCompliant() {
        // :-(MS SQLServer 6.5 doesn't provide what JDBC wants.
        // See DatabaseMetaData.nullPlusNonNullIsNull() for more details.
        // XXX Need to check if Sybase could be jdbcCompliant
        return false;
    }

    /**
     * Parses the specified URL into <code>result</code>.
     * 
     * @param url the url to parse
     * @param result reference used to write the results to
     * @return <code>true</code> if the url was valid; <code>false</code> otherwise
     */
    protected boolean parseUrl(String url, Properties result) {
        if (url == null) {
            return false;
        }

        String tmpUrl = url;
        int serverType = Tds.SQLSERVER;
        int pos = tmpUrl.indexOf(':');

        // Expected "jdbc:"
        if (pos == -1) {
            return false;
        }

        pos = tmpUrl.indexOf(':', pos + 1);

        // Expected "jdbc:jtds:"
        if (pos == -1) {
            return false;
        }

        // Expected "jdbc:jtds:"
        if (!tmpUrl.substring(0, pos + 1).equalsIgnoreCase("jdbc:jtds:")) {
            return false;
        }

        int endPos = tmpUrl.indexOf("//", ++pos);

        // Expected at least "jdbc:jtds://"
        if (endPos == -1) {
            return false;
        }

        if (pos != endPos) {
            String server = tmpUrl.substring(pos, endPos).toLowerCase();

            if (server.equals("sqlserver:")) {
                // Already set to SQL Server...
            } else if (server.equals("sybase:")) {
                serverType = Tds.SYBASE;
            } else {
                return false;
            }
        }

        // Set here to improve getPropertyInfo() support
        if (result != null) {
            result.put(Tds.PROP_SERVERTYPE, String.valueOf(serverType));
        }

        tmpUrl = tmpUrl.substring(endPos + 2);

        int portStart = tmpUrl.indexOf(':');
        int databaseStart = tmpUrl.indexOf('/');
        int attributesStart = tmpUrl.indexOf(';');

        if (portStart != -1) {
            endPos = portStart;
        } else if (databaseStart != -1) {
            endPos = databaseStart;
        } else if (attributesStart != -1) {
            endPos = attributesStart;
        } else {
            endPos = tmpUrl.length();
        }

        // Was expecting some kind of host to be specified...
        if (endPos == 0) {
            return false;
        }

        String host = tmpUrl.substring(0, endPos);
        String port;
        String database;

        // Set here to improve getPropertyInfo() support
        if (result != null) {
            result.put(Tds.PROP_HOST, host);
        }

        // Determine if a port is specified
        if (portStart == -1) {
            port = (serverType == Tds.SYBASE) ? DEFAULT_SYBASE_PORT : DEFAULT_SQL_SERVER_PORT;
        } else {
            if (databaseStart != -1) {
                endPos = databaseStart;
            } else if (attributesStart != -1) {
                endPos = attributesStart;
            } else {
                endPos = tmpUrl.length();
            }

            // All other start positions should be less than the port start otherwise
            // there is a problem with the order of the settings.
            // If the end position equals the start position, then no port was entered
            // after the delimiter so consider this a failure and do not default the
            // value.
            if (endPos <= portStart) {
                return false;
            }

            port = tmpUrl.substring(portStart + 1, endPos);
        }

        // Set here to improve getPropertyInfo() support
        if (result != null) {
            result.put(Tds.PROP_PORT, port);
        }

        // Determine if a database is specified
        if (databaseStart == -1) {
            database = "";
        } else {
            if (attributesStart != -1) {
                endPos = attributesStart;
            } else {
                endPos = tmpUrl.length();
            }

            // All other start positions should be less than the port start otherwise
            // there is a problem with the order of the settings.  If the values are
            // equal the database delimiter was found but no database name was entered.
            if (endPos <= portStart) {
                return false;
            }

            database = tmpUrl.substring(databaseStart + 1, endPos);
        }

        // Set here to improve getPropertyInfo() support
        if (result != null) {
            result.put(Tds.PROP_DBNAME, database);
        }

        // Process all attributes that may be specified.
        while (attributesStart != -1) {
            pos = attributesStart + 1;
            int assignment = tmpUrl.indexOf('=', pos);

            // If there is an attribute, there should be an assignement operator;
            // the assignment operation should not have been discovered at the start
            // position or else there is no attribute name defined.
            if (assignment == -1 || pos == assignment) {
                return false;
            }

            attributesStart = tmpUrl.indexOf(';', pos);

            if (attributesStart == -1) {
                endPos = tmpUrl.length();
            } else {
                // Make sure an assignment operator was not found for a subsequent
                // attribute
                if (assignment > attributesStart) {
                    return false;
                }

                endPos = attributesStart;
            }

            // Should empty values be allowed???
            if (result != null) {
                result.put(tmpUrl.substring(pos, assignment).toUpperCase(),
                           tmpUrl.substring(assignment + 1, endPos));
            }
        }

        return true;
    }

    /**
     * Returns a <code>Properties</code> instance with the keys "uppercased".
     * The idea is to make them easier to read by the inner classes.
     *
     * @param props the input <code>Properties</code>
     * @return the "same" <code>Properties</code> with uppercase keys
     */
    private Properties processProperties(Properties props) {
        Properties res = props;

        for (Enumeration e = props.keys(); e.hasMoreElements();) {
            String key = e.nextElement().toString();

            res.setProperty(key.toUpperCase(), props.getProperty(key));
        }

        return res;
    }
}
