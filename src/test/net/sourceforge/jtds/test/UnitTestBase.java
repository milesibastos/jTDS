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

import junit.framework.TestCase;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Constructor;


/**
 * Base class for unit tests which do not connect to a database.
 * 
 * @author David D. Kilzer
 * @version $Id: UnitTestBase.java,v 1.4 2004-08-05 01:45:23 ddkilzer Exp $
 */ 
public abstract class UnitTestBase extends TestCase {

    /**
     * Constructor.
     * 
     * @param name The name of the test.
     */ 
    public UnitTestBase(final String name) {
        super(name);
    }


    /**
     * Invoke a constructor on a class using reflection.
     * 
     * @param klass The class.
     * @param classes The classes in the parameter list.
     * @param objects The objects to be used as parameters.
     * @return The object constructed.
     */ 
    public static Object invokeConstructor(final Class klass, final Class[] classes, final Object[] objects) {
        try {
            Constructor constructor;
            try {
                constructor = klass.getDeclaredConstructor(classes);
            }
            catch (NoSuchMethodException e) {
                constructor = klass.getConstructor(classes);
            }
            constructor.setAccessible(true);
            return constructor.newInstance(objects);
        }
        catch (NoSuchMethodException e) {
            throw new RuntimeException(e.getMessage());
        }
        catch (InstantiationException e) {
            throw new RuntimeException(e.getMessage());
        }
        catch (IllegalAccessException e) {
            throw new RuntimeException(e.getMessage());
        }
        catch (InvocationTargetException e) {
            throw new RuntimeException(e.getMessage());
        }
    }


    /**
     * Invoke an instance method on an object using reflection.
     * 
     * @param instance The instance of the object.
     * @param methodName The name of the method.
     * @param classes The classes in the parameter list.
     * @param objects The objects to be used as parameters.
     * @return The object returned by invoking the method.
     */ 
    public static Object invokeInstanceMethod(
            final Object instance, final String methodName, final Class[] classes, final Object[] objects) {

        try {
            Method method;
            try {
                method = instance.getClass().getDeclaredMethod(methodName, classes);
            }
            catch (NoSuchMethodException e) {
                method = instance.getClass().getMethod(methodName, classes);
            }
            method.setAccessible(true);
            return method.invoke(instance, objects);
        }
        catch (NoSuchMethodException e) {
            throw new RuntimeException(e.getMessage());
        }
        catch (IllegalAccessException e) {
            throw new RuntimeException(e.getMessage());
        }
        catch (InvocationTargetException e) {
            throw new RuntimeException(e.getMessage());
        }
    }


    /**
     * Invoke a static method on a class using reflection.
     * 
     * @param klass The class.
     * @param methodName The name of the method.
     * @param classes The classes in the parameter list.
     * @param objects The objects to be used as parameters.
     * @return The object returned by invoking the method.
     */ 
    public static Object invokeStaticMethod(
            final Class klass, final String methodName, final Class[] classes, final Object[] objects) {

        try {
            Method method;
            try {
                method = klass.getDeclaredMethod(methodName, classes);
            }
            catch (NoSuchMethodException e) {
                method = klass.getMethod(methodName, classes);
            }
            method.setAccessible(true);
            return method.invoke(klass, objects);
        }
        catch (NoSuchMethodException e) {
            throw new RuntimeException(e.getMessage());
        }
        catch (IllegalAccessException e) {
            throw new RuntimeException(e.getMessage());
        }
        catch (InvocationTargetException e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}
