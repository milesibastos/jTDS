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
 * <li>Add a <code>static final</code> default field to this class.</li>
 * <li>Update {@link #addDefaultProperties(java.util.Properties)} to set the default.</li>
 * </ol>
 * 
 * @author David D. Kilzer.
 * @version $Id: DefaultProperties.java,v 1.1 2004-08-04 15:23:30 ddkilzer Exp $
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
    public static final boolean PREPARE_SQL = true;
    /** Default <code>progName</code> property. */
    public static final String PROG_NAME = "jTDS";
    /** Default <code>sendStringParametersAsUnicode</code> property. */
    public static final boolean USE_UNICODE = true;

    /** Default <code>serverType</code> property for SQL Server. */
    public static final String SERVER_TYPE_SQLSERVER = "sqlserver";
    /** Default <code>serverType</code> property for Sybase. */
    public static final String SERVER_TYPE_SYBASE = "sybase";

    /** Default <code>tdsVersion</code> property for TDS 4.2. */
    public static final String TDS_VERSION_42 = "4.2";
    /** Default <code>tdsVersion</code> property for TDS 5.0. */
    public static final String TDS_VERSION_50 = "5.0";
    /** Default <code>tdsVersion</code> property for TDS 7.0. */
    public static final String TDS_VERSION_70 = "7.0";
    /** Default <code>tdsVersion</code> property for TDS 8.0. */
    public static final String TDS_VERSION_80 = "8.0";


    /**
     * Add default properties to the <code>props</code> properties object.
     * 
     * @param props The properties object.
     * @return The updated <code>props</code> object, or <code>null</code>
     *         if the <code>serverType</code> property is not set.
     */ 
    public static Properties addDefaultProperties(Properties props) {

        String serverType = props.getProperty(Support.getMessage("prop.servertype"));
        if (serverType == null) {
            return null;
        }

        String messageKey = Support.getMessage("prop.tds");
        if (props.getProperty(messageKey) == null) {
            if (serverType.equals(String.valueOf(Driver.SQLSERVER))) {
                props.setProperty(messageKey, TDS_VERSION_70);
            } else if (serverType.equals(String.valueOf(Driver.SYBASE))) {
                props.setProperty(messageKey, TDS_VERSION_50);
            }
        }

        messageKey = Support.getMessage("prop.portnumber");
        if (props.getProperty(messageKey) == null) {
            if (serverType.equals(String.valueOf(Driver.SQLSERVER))) {
                props.setProperty(messageKey, String.valueOf(PORT_NUMBER_SQLSERVER));
            }
            else if (serverType.equals(String.valueOf(Driver.SYBASE))) {
                props.setProperty(messageKey, String.valueOf(PORT_NUMBER_SYBASE));
            }
        }

        messageKey = Support.getMessage("prop.databasename");
        if (props.getProperty(messageKey) == null) {
            props.setProperty(messageKey, DATABASE_NAME);
        }

        messageKey = Support.getMessage("prop.appname");
        if (props.getProperty(messageKey) == null) {
            props.setProperty(messageKey, APP_NAME);
        }

        messageKey = Support.getMessage("prop.lastupdatecount");
        if (props.getProperty(messageKey) == null) {
            props.setProperty(messageKey, String.valueOf(LAST_UPDATE_COUNT));
        }

        messageKey = Support.getMessage("prop.lobbuffer");
        if (props.getProperty(messageKey) == null) {
            props.setProperty(messageKey, String.valueOf(LOB_BUFFER_SIZE));
        }

        messageKey = Support.getMessage("prop.logintimeout");
        if (props.getProperty(messageKey) == null) {
            props.setProperty(messageKey, String.valueOf(LOGIN_TIMEOUT));
        }

        messageKey = Support.getMessage("prop.macaddress");
        if (props.getProperty(messageKey) == null) {
            props.setProperty(messageKey, MAC_ADDRESS);
        }

        messageKey = Support.getMessage("prop.namedpipe");
        if (props.getProperty(messageKey) == null) {
            props.setProperty(messageKey, String.valueOf(NAMED_PIPE));
        }
        
        messageKey = Support.getMessage("prop.packetsize");
        if (props.getProperty(messageKey) == null) {

            String tdsVersion = props.getProperty(Support.getMessage("prop.tds"));

            if (tdsVersion.equals(String.valueOf(TDS_VERSION_42)) ||
                tdsVersion.equals(String.valueOf(TDS_VERSION_50))) {

                props.setProperty(messageKey, String.valueOf(PACKET_SIZE_42_50));
            }
            else if (tdsVersion.equals(String.valueOf(TDS_VERSION_70)) ||
                     tdsVersion.equals(String.valueOf(TDS_VERSION_80))) {

                props.setProperty(messageKey, String.valueOf(PACKET_SIZE_70_80));
            }
        }

        messageKey = Support.getMessage("prop.preparesql");
        if (props.getProperty(messageKey) == null) {
            props.setProperty(messageKey, String.valueOf(PREPARE_SQL));
        }

        messageKey = Support.getMessage("prop.progname");
        if (props.getProperty(messageKey) == null) {
            props.setProperty(messageKey, PROG_NAME);
        }

        messageKey = Support.getMessage("prop.useunicode");
        if (props.getProperty(messageKey) == null) {
            props.setProperty(messageKey, String.valueOf(USE_UNICODE));
        }

        return props;
    }

}
