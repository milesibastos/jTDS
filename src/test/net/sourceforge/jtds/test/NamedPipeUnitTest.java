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
import net.sourceforge.jtds.jdbc.SharedNamedPipe;
import net.sourceforge.jtds.jdbc.TdsCore;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Constructor;



public class NamedPipeUnitTest extends TestCase {


    public void testCalculateBufferSize_TDS42() {
        int length = invoke_calculateBufferSize(TdsCore.TDS42, 0);
        assertEquals(TdsCore.MIN_PKT_SIZE, length);
    }


    public void testCalculateBufferSize_TDS50() {
        int length = invoke_calculateBufferSize(TdsCore.TDS50, 0);
        assertEquals(TdsCore.MIN_PKT_SIZE, length);
    }


    public void testCalculateBufferSize_TDS70() {
        int length = invoke_calculateBufferSize(TdsCore.TDS70, 0);
        assertEquals(TdsCore.DEFAULT_MIN_PKT_SIZE_TDS70, length);
    }


    public void testCalculateBufferSize_TDS80() {
        int length = invoke_calculateBufferSize(TdsCore.TDS80, 0);
        assertEquals(TdsCore.DEFAULT_MIN_PKT_SIZE_TDS70, length);
    }


    private int invoke_calculateBufferSize(int tdsVersion, int packetSize) {

        try {
            Class klass = SharedNamedPipe.class;
            Constructor constructor = klass.getDeclaredConstructor(new Class[]{});
            constructor.setAccessible(true);
            Object newInstance = constructor.newInstance(new Object[]{});
            Method method =
                    klass.getDeclaredMethod(
                            "calculateBufferSize",
                            new Class[]{int.class, int.class});
            method.setAccessible(true);
            Object[] args =
                    new Object[]{
                        new Integer(tdsVersion),
                        new Integer(packetSize)};
            return ((Integer) method.invoke(newInstance, args)).intValue();
        }
        catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
        catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        }
        catch (InstantiationException e) {
            throw new RuntimeException(e);
        }
    }
}
