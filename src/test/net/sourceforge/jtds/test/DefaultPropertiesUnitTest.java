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
package net.sourceforge.jtds.test;

import net.sourceforge.jtds.jdbc.DefaultProperties;
import net.sourceforge.jtds.jdbc.Support;
import net.sourceforge.jtds.jdbc.Messages;
import java.util.Properties;
import java.util.Map;
import java.util.HashMap;



/**
 * Unit tests for the {@link net.sourceforge.jtds.jdbc.DefaultProperties} class.
 * 
 * @author David D. Kilzer
 * @version $Id: DefaultPropertiesUnitTest.java,v 1.2 2004-08-05 01:45:23 ddkilzer Exp $
 */
public class DefaultPropertiesUnitTest extends UnitTestBase {

    /**
     * Constructor.
     * 
     * @param name The name of the test.
     */
    public DefaultPropertiesUnitTest(String name) {
        super(name);
    }


    /**
     * Tests that
     * {@link DefaultProperties#addDefaultPropertyIfNotSet(java.util.Properties, java.lang.String, java.lang.String)}
     * sets a default property if the property is not already set.
     */ 
    public void test_addDefaultPropertyIfNotSet_PropertyNotSet() {
        final Properties properties = new Properties();
        final String key = "prop.databasename";
        final String defaultValue = "foobar";
        invokeStaticMethod(
                DefaultProperties.class, "addDefaultPropertyIfNotSet",
                new Class[]{Properties.class, String.class, String.class},
                new Object[]{properties, key, defaultValue});
        assertEquals(defaultValue, properties.get(Messages.get(key)));
    }


    /**
     * Tests that
     * {@link DefaultProperties#addDefaultPropertyIfNotSet(java.util.Properties, java.lang.String, java.lang.String)}
     * does <em>not</em> set a default property if the property is already set.
     */ 
    public void test_addDefaultPropertyIfNotSet_PropertyAlreadySet() {
        final Properties properties = new Properties();
        final String key = "prop.databasename";
        final String presetValue = "barbaz";
        final String defaultValue = "foobar";
        properties.setProperty(Messages.get(key), presetValue);
        invokeStaticMethod(DefaultProperties.class, "addDefaultPropertyIfNotSet",
                           new Class[]{Properties.class, String.class, String.class},
                           new Object[]{properties, key, defaultValue});
        assertEquals(presetValue, properties.get(Messages.get(key)));
    }


    /**
     * Tests that
     * {@link DefaultProperties#addDefaultPropertyIfNotSet(java.util.Properties, java.lang.String, java.lang.String, java.util.Map)}
     * does <em>not</em> set a default property if the <code>defaultKey</code> is not set.
     */ 
    public void test_addDefaultPropertyIfNotSet_DefaultKeyNotSet() {
        final Properties properties = new Properties();
        final String defaultKey = "prop.servertype";
        final String key = "prop.portnumber";
        final HashMap defaults = new HashMap();
        invokeStaticMethod(DefaultProperties.class, "addDefaultPropertyIfNotSet",
                           new Class[]{Properties.class, String.class, String.class, Map.class},
                           new Object[]{properties, key, defaultKey, defaults});
        assertEquals(0, properties.size());
    }


    /**
     * Tests that
     * {@link DefaultProperties#addDefaultPropertyIfNotSet(java.util.Properties, java.lang.String, java.lang.String, java.util.Map)}
     * sets a default property if the property is not already set.
     */ 
    public void test_addDefaultPropertyIfNotSet_DefaultKeySet_PropertyNotSet() {
        final Properties properties = new Properties();
        final String defaultKey = "prop.servertype";
        final String defaultKeyValue = "foobar";
        properties.put(Messages.get(defaultKey), defaultKeyValue);
        final String key = "prop.portnumber";
        final String defaultValue = "2004";
        final HashMap defaults = new HashMap();
        defaults.put(defaultKeyValue, defaultValue);
        invokeStaticMethod(DefaultProperties.class, "addDefaultPropertyIfNotSet",
                           new Class[]{Properties.class, String.class, String.class, Map.class},
                           new Object[]{properties, key, defaultKey, defaults});
        assertEquals(defaultValue, properties.get(Messages.get(key)));
    }


    /**
     * Tests that
     * {@link DefaultProperties#addDefaultPropertyIfNotSet(java.util.Properties, java.lang.String, java.lang.String, java.util.Map)}
     * does <em>not</em> set a default property if the property is already set.
     */ 
    public void test_addDefaultPropertyIfNotSet_DefaultKeySet_PropertyAlreadySet() {
        final Properties properties = new Properties();
        final String defaultKey = "prop.servertype";
        final String defaultKeyValue = "foobar";
        properties.put(Messages.get(defaultKey), defaultKeyValue);
        final String key = "prop.portnumber";
        final String presetValue = "2020";
        properties.put(Messages.get(key), presetValue);
        final String defaultValue = "2004";
        final HashMap defaults = new HashMap();
        defaults.put(defaultKeyValue, defaultValue);
        invokeStaticMethod(DefaultProperties.class, "addDefaultPropertyIfNotSet",
                           new Class[]{Properties.class, String.class, String.class, Map.class},
                           new Object[]{properties, key, defaultKey, defaults});
        assertEquals(presetValue, properties.get(Messages.get(key)));
    }

}
