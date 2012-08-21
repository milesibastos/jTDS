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
import net.sourceforge.jtds.util.MD5Digest;

/**
 * Tests the MD5 implementation against a known set of inputs and outputs.
 * 
 * @author Rainer Schwarze / admaDIC
 */
public class MD5DigestTest extends TestCase {

    /**
     * Test MD5.
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
                /*@0x0:*/ (byte)0xfd, (byte)0xa3, (byte)0xbb, (byte)0xd5, (byte)0xd5, (byte)0x0d, (byte)0x54, (byte)0xbe, 
                /*@0x8:*/ (byte)0x4a, (byte)0x34, (byte)0x43, (byte)0xd5, (byte)0x9f, (byte)0xe3, (byte)0x95, (byte)0x70, 
                };
        byte [] dataDig = new byte[16];
        // test digest:
        {
            MD5Digest md5Digest = new MD5Digest();
            md5Digest.update(dataInp, 0, dataInp.length);
            md5Digest.doFinal(dataDig, 0);
            String strAct = Helper.dumpCodeBytes(dataDig);
            String strExp = Helper.dumpCodeBytes(dataExp);
            assertEquals("digest failed", strExp, strAct);
        }
    }
}
