/*    
 * jTDS JDBC Driver for Microsoft SQL Server and Sybase
 * Copyright (C) 2011  Rainer Schwarze, admaDIC
 * e-mail: info@admadic.de
 * post  : admaDIC / Attn: Rainer Schwarze / An der Roda 7 / 
 *         07646 Laasdorf / Germany
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package net.sourceforge.jtds.jdbc;

import junit.framework.TestCase;
import net.sourceforge.jtds.util.MD4Digest;

/**
 * Tests the MD4 implementation against a known set of inputs and outputs.
 * 
 * @author Rainer Schwarze / admaDIC
 */
public class MD4DigestTest extends TestCase {

    /**
     * Test MD4.
     * @throws Exception
     */
    public void testSet1() throws Exception {
        byte [] dataInp = {
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 

                0x02, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 
                0x03, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 
                0x04, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 
                0x05, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 
                };
        byte [] dataExp = {
                /*@0x0:*/ (byte)0x20, (byte)0x48, (byte)0x28, (byte)0x02, (byte)0x4a, (byte)0x86, (byte)0x06, (byte)0x93, 
                /*@0x8:*/ (byte)0x30, (byte)0x09, (byte)0x6f, (byte)0x24, (byte)0x0d, (byte)0x37, (byte)0x8e, (byte)0x57, 
                };
        byte [] dataDig = new byte[16];
        // test digest:
        {
            MD4Digest md4Digest = new MD4Digest();
            md4Digest.update(dataInp, 0, dataInp.length);
            md4Digest.doFinal(dataDig, 0);
            String strAct = Helper.dumpCodeBytes(dataDig);
            String strExp = Helper.dumpCodeBytes(dataExp);
            assertEquals("digest failed", strExp, strAct);
        }
    }
}
