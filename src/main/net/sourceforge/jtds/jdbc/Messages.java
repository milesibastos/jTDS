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

import java.util.ResourceBundle;
import java.text.MessageFormat;



/**
 * Support class for <code>Messages.properties</code>.
 * 
 * @author David D. Kilzer
 * @author Mike Hutchinson
 * @version $Id: Messages.java,v 1.2 2004-08-05 23:49:11 ddkilzer Exp $
 */
public final class Messages {

    /**
     * Default name for resource bundle containing the messages
     */
    private static final String DEFAULT_RESOURCE = "net.sourceforge.jtds.jdbc.Messages";


    /**
     * Default constructor.  Private to prevent instantiation.
     */
    private Messages() {
    }


    /**
     * Get runtime message using supplied key.
     *
     * @param key The key of the message in Messages.properties
     * @return The selected message as a <code>String</code>.
     */
    public static String get(String key) {
        return get(key, null);
    }


    /**
     * Get runtime message using supplied key and substitute parameter
     * into message.
     *
     * @param key The key of the message in Messages.properties
     * @param param1 The object to insert into message.
     * @return The selected message as a <code>String</code>.
     */
    static String get(String key, Object param1) {
        Object args[] = {param1};

        return get(key, args);
    }


    /**
     * Get runtime message using supplied key and substitute parameters
     * into message.
     *
     * @param key The key of the message in Messages.properties
     * @param param1 The object to insert into message.
     * @param param2 The object to insert into message.
     * @return The selected message as a <code>String</code>.
     */
    static String get(String key, Object param1, Object param2) {
        Object args[] = {param1, param2};

        return get(key, args);
    }


    /**
     * Get runtime error using supplied key and substitute parameters
     * into message.
     *
     * @param key The key of the error message in Messages.properties
     * @param arguments The objects to insert into the message.
     * @return The selected error message as a <code>String</code>.
     */
    private static String get(String key, Object[] arguments) {

        ResourceBundle rb = loadResourceBundle();

        String formatString;

        try {
            formatString = rb.getString(key);
        } catch (java.util.MissingResourceException mre) {
            throw new RuntimeException("no message resource found for message property "
                                       + key);
        }

        MessageFormat formatter = new MessageFormat(formatString);

        return formatter.format(arguments);
    }


    /**
     * Load the {@link #DEFAULT_RESOURCE} resource bundle.
     * <p/>
     * ResourceBundle does caching so performance should be OK.
     * (Comment copied from {@link #get(String, Object[])}
     * in previous revision.)
     * 
     * @return The resource bundle.
     */
    private static ResourceBundle loadResourceBundle() {
        return ResourceBundle.getBundle(DEFAULT_RESOURCE);
    }

}
