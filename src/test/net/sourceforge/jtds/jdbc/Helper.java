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

/**
 * Provides common helper functions.
 * 
 * @author Rainer Schwarze / admaDIC
 */
public class Helper {

    /**
     * Creates a String which represents the given byte array as if it was 
     * declared as Java code.
     * 
     * @param data The byte array to convert to a String.
     * @return A String built from the byte array.
     */
    static String dumpCodeBytes(byte[] data) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        for (int blockOfs = 0; blockOfs<data.length; blockOfs += 8) {
            sb.append("/*@0x");
            sb.append(Integer.toHexString(blockOfs));
            sb.append(":*/ ");
            for (int i=0; i<8; i++) {
                if (blockOfs+i > data.length) 
                    break;
                int b = data[blockOfs + i];
                b &= 0x0ff;
                sb.append("(byte)0x");
                if (b>=0 && b<16) {
                    sb.append('0');
                }
                sb.append(Integer.toHexString(b));
                sb.append(", ");
            }
            sb.append("\n");
        }
        sb.append("};\n");
        sb.trimToSize();
        return sb.toString();
    }
}
