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

import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.sql.DriverManager;
import java.util.Enumeration;

/**
 * jTDS implementation of the java.sql.Driver interface.
 * <p>
 * Implementation note:
 * <ol>
 * <li>Property text names and descriptions are loaded from an external file resource.
 *     This allows the actual names and descriptions to be changed or localised without
 *     impacting this code.
 * <li>The way in which the URL is parsed and converted to properties is rather
 *     different from the original jTDS Driver class.
 *     See parseURL and Connection.unpackProperties methods for more detail.
 * </ol>
 * @see java.sql.Driver
 * @author Brian Heineman
 * @author Mike Hutchinson
 * @author Alin Sinpalean
 * @version $Id: Driver.java,v 1.26 2004-08-04 01:58:39 ddkilzer Exp $
 */
public class Driver implements java.sql.Driver {
    private static String driverPrefix = "jdbc:jtds:";
    static final int MAJOR_VERSION = 0;
    static final int MINOR_VERSION = 9;
    public static final boolean JDBC3 =
            "1.4".compareTo(System.getProperty("java.specification.version")) <= 0;
    /** TDS 4.2 protocol. */
    public static final int TDS42 = 1;
    /** TDS 5.0 protocol. */
    public static final int TDS50 = 2;
    /** TDS 7.0 protocol. */
    public static final int TDS70 = 3;
    /** TDS 8.0 protocol. */
    public static final int TDS80 = 4;
    /** Microsoft SQL Server. */
    public static final int SQLSERVER = 1;
    /** Sybase ASE. */
    public static final int SYBASE = 2;

    static {
        try {
            // Register this with the DriverManager
            DriverManager.registerDriver(new Driver());
        } catch (SQLException e) {
        }
    }

    public int getMajorVersion() {
        return MAJOR_VERSION;
    }

    public int getMinorVersion() {
        return MINOR_VERSION;
    }

    public boolean jdbcCompliant() {
        return false;
    }

    public boolean acceptsURL(String url) throws SQLException {
        if (url == null) {
            return false;
        }
        
        return url.toLowerCase().startsWith(driverPrefix);
    }

    public Connection connect(String url, Properties info)
        throws SQLException  {
        if (url == null || !url.toLowerCase().startsWith(driverPrefix)) {
            return null;
        }

        Properties props = parseURL(url, info);

        if (props == null) {
            throw new SQLException(Support.getMessage("error.driver.badurl", url), "08001");
        }

        if (props.getProperty(Support.getMessage("prop.logintimeout")) == null) {
            props.setProperty(Support.getMessage("prop.logintimeout"), Integer.toString(DriverManager.getLoginTimeout()));
        }

        if (JDBC3) {
            return new ConnectionJDBC3(url, props);
        }

        return new ConnectionJDBC2(url, props);
    }

    public DriverPropertyInfo[] getPropertyInfo(String url, Properties props)
            throws SQLException {
        DriverPropertyInfo[] dpi = new DriverPropertyInfo[] {
            new DriverPropertyInfo(Support.getMessage("prop.servertype"), null),
            new DriverPropertyInfo(Support.getMessage("prop.servername"), null),
            new DriverPropertyInfo(Support.getMessage("prop.portnumber"), null),
            new DriverPropertyInfo(Support.getMessage("prop.databasename"), null),
            new DriverPropertyInfo(Support.getMessage("prop.user"), null),
            new DriverPropertyInfo(Support.getMessage("prop.password"), null),
            new DriverPropertyInfo(Support.getMessage("prop.charset"), null),
            new DriverPropertyInfo(Support.getMessage("prop.tds"), null),
            new DriverPropertyInfo(Support.getMessage("prop.domain"), null),
            new DriverPropertyInfo(Support.getMessage("prop.instance"), null),
            new DriverPropertyInfo(Support.getMessage("prop.language"), null),
            new DriverPropertyInfo(Support.getMessage("prop.lastupdatecount"), null),
            new DriverPropertyInfo(Support.getMessage("prop.logintimeout"), null),
            new DriverPropertyInfo(Support.getMessage("prop.useunicode"), null),
            new DriverPropertyInfo(Support.getMessage("prop.namedpipe"), null),
            new DriverPropertyInfo(Support.getMessage("prop.macaddress"), null),
            new DriverPropertyInfo(Support.getMessage("prop.packetsize"), null),
            new DriverPropertyInfo(Support.getMessage("prop.preparesql"), null),
            new DriverPropertyInfo(Support.getMessage("prop.lobbuffer"), null)
        };

        Properties info = parseURL(url, (props == null ? new Properties() : props));

        if (info == null) {
            throw new SQLException(
                        Support.getMessage("error.driver.badurl", url), "08001");
        }

        for (int i = 0; i < dpi.length; i++) {
            String name = dpi[i].name;
            String value = info.getProperty(name);

            if (value != null) {
                dpi[i].value = value;
            }

            if (name.equals(Support.getMessage("prop.servertype"))) {
                dpi[i].description = Support.getMessage("prop.desc.servertype");
                dpi[i].required = true;
                dpi[i].choices = new String[] {
                    String.valueOf(SQLSERVER),
                    String.valueOf(SYBASE)
                };

                if (dpi[i].value == null) {
                    dpi[i].value = dpi[i].choices[0]; // Driver.SQLSERVER
                }
            } else if (name.equals(Support.getMessage("prop.servername"))) {
                dpi[i].description = Support.getMessage("prop.desc.servername");
                dpi[i].required = true;
            } else if (name.equals(Support.getMessage("prop.portnumber"))) {
                dpi[i].description = Support.getMessage("prop.desc.portnumber");

                if (dpi[i].value == null) {
                    if (String.valueOf(SYBASE).equalsIgnoreCase(
                            String.valueOf(info.get(Support.getMessage("prop.servertype"))))) {
                        dpi[i].value = Integer.toString(Settings.DEFAULT_PORT_NUMBER_SYBASE);
                    } else {
                        dpi[i].value = Integer.toString(Settings.DEFAULT_PORT_NUMBER_SQLSERVER);
                    }
                }
            } else if (name.equals(Support.getMessage("prop.databasename"))) {
                dpi[i].description = Support.getMessage("prop.desc.databasename");

                if (dpi[i].value == null) {
                    dpi[i].value = "master";
                }
            } else if (name.equals(Support.getMessage("prop.user"))) {
                dpi[i].description = Support.getMessage("prop.desc.user");
            } else if (name.equals(Support.getMessage("prop.password"))) {
                dpi[i].description = Support.getMessage("prop.desc.password");
            } else if (name.equals(Support.getMessage("prop.charset"))) {
                dpi[i].description = Support.getMessage("prop.desc.charset");
            } else if (name.equals(Support.getMessage("prop.language"))) {
                dpi[i].description = Support.getMessage("prop.desc.language");
            } else if (name.equals(Support.getMessage("prop.tds"))) {
                dpi[i].description = Support.getMessage("prop.desc.tds");
                dpi[i].choices = new String[] {
                    "4.2", "5.0", "7.0", "8.0"
                };

                if (dpi[i].value == null) {
                    dpi[i].value = dpi[i].choices[2]; // TdsCore.TDS70
                }
            } else if (name.equals(Support.getMessage("prop.domain"))) {
                dpi[i].description = Support.getMessage("prop.desc.domain");
            } else if (name.equals(Support.getMessage("prop.instance"))) {
                dpi[i].description = Support.getMessage("prop.desc.instance");
            } else if (name.equals(Support.getMessage("prop.lastupdatecount"))) {
                dpi[i].description = Support.getMessage("prop.desc.lastupdatecount");
                dpi[i].choices = new String[] {"true", "false"};

                if (dpi[i].value == null) {
                    dpi[i].value = dpi[i].choices[1]; // false
                }
            } else if (name.equals(Support.getMessage("prop.logintimeout"))) {
                dpi[i].description = Support.getMessage("prop.desc.logintimeout");

                if (dpi[i].value == null) {
                    dpi[i].value = "0";
                }
            } else if (name.equals(Support.getMessage("prop.useunicode"))) {
                dpi[i].description = Support.getMessage("prop.desc.useunicode");
                dpi[i].choices = new String[] {"true","false"};

                if (dpi[i].value == null) {
                    dpi[i].value = dpi[i].choices[0]; // true
                }
            } else if (name.equals(Support.getMessage("prop.namedpipe"))) {
                dpi[i].description = Support.getMessage("prop.desc.namedpipe");
                dpi[i].choices = new String[] {"true","false"};

                if (dpi[i].value == null) {
                    dpi[i].value = dpi[i].choices[1]; // false
                }
            } else if (name.equals(Support.getMessage("prop.macaddress"))) {
                dpi[i].description = Support.getMessage("prop.desc.macaddress");
            } else if (name.equals(Support.getMessage("prop.packetsize"))) {
                dpi[i].description = Support.getMessage("prop.desc.packetsize");
                
                if (dpi[i].value == null) {
                    dpi[i].value = String.valueOf(TdsCore.MIN_PKT_SIZE);
                }
            } else if (name.equals(Support.getMessage("prop.preparesql"))) {
                dpi[i].description = Support.getMessage("prop.desc.preparesql");
                dpi[i].choices = new String[] {"true", "false"};

                if (dpi[i].value == null) {
                    dpi[i].value = dpi[i].choices[0]; // true
                }
            } else if (name.equals(Support.getMessage("prop.lobbuffer"))) {
                dpi[i].description = Support.getMessage("prop.desc.lobbuffer");
    
                if (dpi[i].value == null) {
                    dpi[i].value = String.valueOf(Settings.DEFAULT_LOB_BUFFER_SIZE);
                }
            }
        }

        return dpi;
    }

    /**
     * Parse the driver URL and extract the properties.
     *
     * @param url The URL to parse.
     * @param info Any existing properties already loaded in a Properties object.
     * @return The URL properties as a <code>Properties</code> object.
     */
    private static Properties parseURL(String url, Properties info) {
        Properties props = new Properties();

        // Take local copy of existing properties
        for (Enumeration e = info.keys(); e.hasMoreElements();) {
            String key = (String) e.nextElement();
            String value = info.getProperty(key);
            
            if (value != null) {
                props.setProperty(key.toUpperCase(), value);
            }
        }

        StringBuffer token = new StringBuffer(16);
        int pos = 0;

        pos = nextToken(url, pos, token); // Skip jdbc

        if (!token.toString().equalsIgnoreCase("jdbc")) {
            return null; // jdbc: missing
        }

        pos = nextToken(url, pos, token); // Skip jtds
        
        if (!token.toString().equalsIgnoreCase("jtds")) {
            return null; // jtds: missing
        }

        pos = nextToken(url, pos, token); // Get server type
        String type = token.toString().toLowerCase();

        if (type.equals("sqlserver")) {
            props.setProperty(Support.getMessage("prop.servertype"),
                              String.valueOf(SQLSERVER));
        } else if (type.equals("sybase")) {
            props.setProperty(Support.getMessage("prop.servertype"),
                              String.valueOf(SYBASE));
        } else {
            return null; // Bad server type
        }

        pos = nextToken(url, pos, token); // Null token between : and //

        if (token.length() > 0) {
            return null; // There should not be one!
        }

        pos = nextToken(url, pos, token); // Get server name
        String host = token.toString();

        if (host.length() == 0 &&
            props.getProperty(Support.getMessage("prop.servername")) == null) {
            return null; // Server name missing
        }

        props.setProperty(Support.getMessage("prop.servername"), host);

        if (url.charAt(pos - 1) == ':' && pos < url.length()) {
            pos = nextToken(url, pos, token); // Get port number

            try {
                int port = Integer.parseInt(token.toString());
                props.setProperty(Support.getMessage("prop.portnumber"), Integer.toString(port));
            } catch(NumberFormatException e) {
                return null; // Bad port number
            }
        }

        if (url.charAt(pos - 1) == '/' && pos < url.length()) {
            pos = nextToken(url, pos, token); // Get database name
            props.setProperty(Support.getMessage("prop.databasename"), token.toString());
        }

        //
        // Process any additional properties in URL
        //
        while (url.charAt(pos - 1) == ';' && pos < url.length()) {
            pos = nextToken(url, pos, token);
            String tmp = token.toString();
            int index = tmp.indexOf('=');

            if (index > 0 && index < tmp.length() - 1) {
                props.setProperty(tmp.substring(0, index).toUpperCase(), tmp.substring(index + 1));
            } else {
                props.setProperty(tmp.toUpperCase(), "");
            }
        }

        //
        // Set default properties
        //
        props = Settings.addDefaultProperties(props);

        return props;
    }

    /**
     * Extract the next lexical token from the URL.
     *
     * @param url The URL being parsed
     * @param pos The current position in the URL string.
     * @param token The buffer containing the extracted token.
     * @return The updated position as an <code>int</code>.
     */
    private static int nextToken(String url, int pos, StringBuffer token) {
        token.setLength(0);

        while (pos < url.length()) {
            char ch = url.charAt(pos++);

            if (ch == ':' || ch == ';') {
                break;
            }

            if (ch == '/') {
                if (pos < url.length() && url.charAt(pos) == '/') {
                    pos++;
                }

                break;
            }

            token.append(ch);
        }

        return pos;
    }
}
