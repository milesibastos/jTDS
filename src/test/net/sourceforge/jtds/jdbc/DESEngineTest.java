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

import net.sourceforge.jtds.util.DESEngine;
import junit.framework.TestCase;

/**
 * Tests the DESEngine against a known set of inputs and outputs.
 * 
 * @author Rainer Schwarze / admaDIC
 */
public class DESEngineTest extends TestCase {

    /**
     * Test DES, input data is a multiple of block size (no padding needed).
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
        byte [] dataKey = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};
        byte [] dataExp = {
                (byte)0x77, (byte)0xa7, (byte)0xd6, (byte)0xbc, (byte)0xf5, (byte)0x79, (byte)0x62, (byte)0xb9, 
                (byte)0x77, (byte)0xa7, (byte)0xd6, (byte)0xbc, (byte)0xf5, (byte)0x79, (byte)0x62, (byte)0xb9, 
                (byte)0x77, (byte)0xa7, (byte)0xd6, (byte)0xbc, (byte)0xf5, (byte)0x79, (byte)0x62, (byte)0xb9, 
                (byte)0x77, (byte)0xa7, (byte)0xd6, (byte)0xbc, (byte)0xf5, (byte)0x79, (byte)0x62, (byte)0xb9, 

                (byte)0xca, (byte)0x48, (byte)0x26, (byte)0x21, (byte)0x75, (byte)0x95, (byte)0xee, (byte)0x3b, 
                (byte)0xcc, (byte)0x1a, (byte)0x27, (byte)0x1c, (byte)0xcd, (byte)0x29, (byte)0xfe, (byte)0x26, 
                (byte)0x0d, (byte)0x40, (byte)0x98, (byte)0x42, (byte)0xdd, (byte)0xa5, (byte)0xf8, (byte)0xbc, 
                (byte)0x7d, (byte)0x76, (byte)0x12, (byte)0x5b, (byte)0x63, (byte)0x00, (byte)0x5f, (byte)0x88, 
                };
        byte [] dataEnc = new byte[dataInp.length];
        byte [] dataDec = new byte[dataInp.length];
        // test encryption:
        {
            DESEngine desEngine = new DESEngine(true, dataKey);
            for (int i=0; i<dataInp.length; i += 8) {
                desEngine.processBlock(dataInp, i, dataEnc, i);
            }
            String strAct = Helper.dumpCodeBytes(dataEnc);
            String strExp = Helper.dumpCodeBytes(dataExp);
            assertEquals("encryption failed", strExp, strAct);
        }

        // test decryption:
        {
            DESEngine desEngine = new DESEngine(false, dataKey);
            for (int i=0; i<dataInp.length; i += 8) {
                desEngine.processBlock(dataEnc, i, dataDec, i);
            }
            String strAct = Helper.dumpCodeBytes(dataDec);
            String strExp = Helper.dumpCodeBytes(dataInp);
            assertEquals("decryption failed", strExp, strAct);
        }
    }
}
