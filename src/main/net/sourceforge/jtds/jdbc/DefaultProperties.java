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

import java.util.Properties;
import java.util.Map;
import java.util.HashMap;



/**
 * Container for default property constants.
 * <p/>
 * This class also provides static utility methods for 
 * {@link Properties} and <code>Settings</code> objects.
 * <p/>
 * To add new properties to the jTDS driver, do the following:
 * <ol>
 * <li>Add <code>prop.<em>foo</em></code> and <code>prop.desc.<em>foo</em></code>
 *     properties to <code>Messages.properties</code>.</li>
 * <li>Add a <code>static final</code> default field to {@link DefaultProperties}.</li>
 * <li>Update {@link #addDefaultProperties(java.util.Properties)} to set the default.</li>
 * <li>Update {@link Driver#createChoicesMap()} and 
 *     <code>DriverUnitTest.test_getPropertyInfo_Choices()</code> if the property
 *     has a specific set of inputs, e.g., "true" and "false", or "1" and "2".</li>
 * <li>Update {@link Driver#createRequiredTrueMap()} and
 *     <code>DriverUnitTest.test_getPropertyInfo_Required()</code> if the property
 *     is required.</li>
 * <li>Add a new test to <code>DefaultPropertiesTestLibrary</code> for the new
 *     property.</li>
 * </ol>
 * 
 * @author David D. Kilzer
 * @version $Id: DefaultProperties.java,v 1.8 2004-08-16 18:43:00 ddkilzer Exp $
 */
public final class DefaultProperties {

    /** Default <code>appName</code> property. */
    public static final String APP_NAME = "jTDS";
    /** Default <code>databaseName</code> property. */
    public static final String DATABASE_NAME = "";
    /** Default <code>lastUpdateCount</code> property. */
    public static final boolean LAST_UPDATE_COUNT = true;
    /** Default <code>lobBufferSize</code> property. */
    public static final int LOB_BUFFER_SIZE = 32768;
    /** Default <code>loginTimeout</code> property. */
    public static final int LOGIN_TIMEOUT = 0;
    /** Default <code>macAddress</code> property. */
    public static final String MAC_ADDRESS = "000000000000";
    /** Default <code>namedPipe</code> property. */
    public static final boolean NAMED_PIPE = false;
    /** Default <code>namedPipePath</code> property for SQL Server. */
    public static final String NAMED_PIPE_PATH_SQLSERVER = "/sql/query";
    /** Default <code>namedPipePath</code> property for Sybase. */
    public static final String NAMED_PIPE_PATH_SYBASE = "/sybase/query";
    /** Default <code>packetSize</code> property for TDS 4.2 and TDS 5.0. */
    public static final int PACKET_SIZE_42_50 = TdsCore.MIN_PKT_SIZE;
    /** Default <code>packetSize</code> property for TDS 7.0 and TDS 8.0. */
    public static final int PACKET_SIZE_70_80 = 0; // server sets packet size
    /** Default <code>portNumber</code> property for SQL Server. */
    public static final int PORT_NUMBER_SQLSERVER = 1433;
    /** Default <code>portNumber</code> property for Sybase. */
    public static final int PORT_NUMBER_SYBASE = 7100;
    /** Default <code>prepareSql</code> property. */
    public static final int PREPARE_SQL = TdsCore.TEMPORARY_STORED_PROCEDURES;
    /** Default <code>progName</code> property. */
    public static final String PROG_NAME = "jTDS";
    /** Default <code>sendStringParametersAsUnicode</code> property. */
    public static final boolean USE_UNICODE = true;

    /** Default <code>serverType</code> property for SQL Server. */
    public static final String SERVER_TYPE_SQLSERVER = "sqlserver";
    /** Default <code>serverType</code> property for Sybase. */
    public static final String SERVER_TYPE_SYBASE = "sybase";

    /** Default <code>tds</code> property for TDS 4.2. */
    public static final String TDS_VERSION_42 = "4.2";
    /** Default <code>tds</code> property for TDS 5.0. */
    public static final String TDS_VERSION_50 = "5.0";
    /** Default <code>tds</code> property for TDS 7.0. */
    public static final String TDS_VERSION_70 = "7.0";
    /** Default <code>tds</code> property for TDS 8.0. */
    public static final String TDS_VERSION_80 = "8.0";


    /**
     * Add default properties to the <code>props</code> properties object.
     * 
     * @param props The properties object.
     * @return The updated <code>props</code> object, or <code>null</code>
     *         if the <code>serverType</code> property is not set.
     */ 
    public static Properties addDefaultProperties(final Properties props) {

        final String serverType = props.getProperty(Messages.get("prop.servertype"));
        if (serverType == null) {
            return null;
        }

        final HashMap tdsDefaults = new HashMap(2);
        tdsDefaults.put(String.valueOf(Driver.SQLSERVER), TDS_VERSION_70);
        tdsDefaults.put(String.valueOf(Driver.SYBASE), TDS_VERSION_50);
        addDefaultPropertyIfNotSet(props, "prop.tds", "prop.servertype", tdsDefaults);

        final HashMap portNumberDefaults = new HashMap(2);
        portNumberDefaults.put(String.valueOf(Driver.SQLSERVER), String.valueOf(PORT_NUMBER_SQLSERVER));
        portNumberDefaults.put(String.valueOf(Driver.SYBASE), String.valueOf(PORT_NUMBER_SYBASE));
        addDefaultPropertyIfNotSet(props, "prop.portnumber", "prop.servertype", portNumberDefaults);

        addDefaultPropertyIfNotSet(props, "prop.databasename", DATABASE_NAME);
        addDefaultPropertyIfNotSet(props, "prop.appname", APP_NAME);
        addDefaultPropertyIfNotSet(props, "prop.lastupdatecount", String.valueOf(LAST_UPDATE_COUNT));
        addDefaultPropertyIfNotSet(props, "prop.lobbuffer", String.valueOf(LOB_BUFFER_SIZE));
        addDefaultPropertyIfNotSet(props, "prop.logintimeout", String.valueOf(LOGIN_TIMEOUT));
        addDefaultPropertyIfNotSet(props, "prop.macaddress", MAC_ADDRESS);
        addDefaultPropertyIfNotSet(props, "prop.namedpipe", String.valueOf(NAMED_PIPE));

        final HashMap packetSizeDefaults = new HashMap(4);
        packetSizeDefaults.put(TDS_VERSION_42, String.valueOf(PACKET_SIZE_42_50));
        packetSizeDefaults.put(TDS_VERSION_50, String.valueOf(PACKET_SIZE_42_50));
        packetSizeDefaults.put(TDS_VERSION_70, String.valueOf(PACKET_SIZE_70_80));
        packetSizeDefaults.put(TDS_VERSION_80, String.valueOf(PACKET_SIZE_70_80));
        addDefaultPropertyIfNotSet(props, "prop.packetsize", "prop.tds", packetSizeDefaults);

        addDefaultPropertyIfNotSet(props, "prop.preparesql", String.valueOf(PREPARE_SQL));
        addDefaultPropertyIfNotSet(props, "prop.progname", PROG_NAME);
        addDefaultPropertyIfNotSet(props, "prop.useunicode", String.valueOf(USE_UNICODE));

        return props;
    }


    /**
     * Sets a default property if the property is not already set.
     * 
     * @param props The properties object.
     * @param key The message key to set.
     * @param defaultValue The default value to set.
     */ 
    private static void addDefaultPropertyIfNotSet(
            final Properties props, final String key, final String defaultValue) {
        final String messageKey = Messages.get(key);
        
        if (props.getProperty(messageKey) == null) {
            props.setProperty(messageKey, defaultValue);
        }
    }


    /**
     * Sets a default property if the property is not already set, using
     * the <code>defaultKey</code> and the <code>defaults</code> map to
     * determine the correct value.
     * 
     * @param props The properties object.
     * @param key The message key to set.
     * @param defaultKey The key whose value determines which default
     *        value to set from <code>defaults</code>.
     * @param defaults The mapping of <code>defaultKey</code> values to
     *        the correct <code>key</code> value to set.
     */ 
    private static void addDefaultPropertyIfNotSet(
            final Properties props, final String key, final String defaultKey, final Map defaults) {
        final String defaultKeyValue = props.getProperty(Messages.get(defaultKey));
        
        if (defaultKeyValue == null) {
            return;
        }

        final String messageKey = Messages.get(key);
        
        if (props.getProperty(messageKey) == null) {
            final Object defaultValue = defaults.get(defaultKeyValue);
            
            if (defaultValue != null) {
                props.setProperty(messageKey, String.valueOf(defaultValue));
            }
        }
    }


    /**
     * Converts an integer server type to its string representation.
     * 
     * @param serverType The server type as an integer.
     * @return The server type as a string if known, or <code>null</code> if unknown.
     */
    public static String getServerType(int serverType) {
        if (serverType == Driver.SQLSERVER) {
            return SERVER_TYPE_SQLSERVER;
        }
        else if (serverType == Driver.SYBASE) {
            return SERVER_TYPE_SYBASE;
        }
        return null;
    }


    /**
     * Converts a string server type to its integer representation.
     * 
     * @param serverType The server type as a string.
     * @return The server type as an integer if known, or <code>null</code> if unknown.
     */
    public static Integer getServerType(String serverType) {
        if (DefaultProperties.SERVER_TYPE_SQLSERVER.equals(serverType)) {
            return new Integer(Driver.SQLSERVER);
        }
        else if (DefaultProperties.SERVER_TYPE_SYBASE.equals(serverType)) {
            return new Integer(Driver.SYBASE);
        }
        return null;
    }


    /**
     * Converts a string TDS version to its integer representation.
     * 
     * @param tdsVersion The TDS version as a string.
     * @return The TDS version as an integer if known, or <code>null</code> if unknown.
     */
    public static Integer getTdsVersion(String tdsVersion) {
        if (DefaultProperties.TDS_VERSION_42.equals(tdsVersion)) {
            return new Integer(Driver.TDS42);
        }
        else if (DefaultProperties.TDS_VERSION_50.equals(tdsVersion)) {
            return new Integer(Driver.TDS50);
        }
        else if (DefaultProperties.TDS_VERSION_70.equals(tdsVersion)) {
            return new Integer(Driver.TDS70);
        }
        else if (DefaultProperties.TDS_VERSION_80.equals(tdsVersion)) {
            return new Integer(Driver.TDS80);
        }
        return null;
    }

}
