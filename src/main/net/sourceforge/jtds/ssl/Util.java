// jTDS JDBC Driver for Microsoft SQL Server and Sybase
//Copyright (C) 2004 The jTDS Project
//
//This library is free software; you can redistribute it and/or
//modify it under the terms of the GNU Lesser General Public
//License as published by the Free Software Foundation; either
//version 2.1 of the License, or (at your option) any later version.
//
//This library is distributed in the hope that it will be useful,
//but WITHOUT ANY WARRANTY; without even the implied warranty of
//MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
//Lesser General Public License for more details.
//
//You should have received a copy of the GNU Lesser General Public
//License along with this library; if not, write to the Free Software
//Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
//
package net.sourceforge.jtds.ssl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Useful methods for the <code>net.sourceforge.jtds.ssl</code> package.
 *
 * @author Rob Worsnop
 * @version $Id: Util.java,v 1.1 2005-01-04 17:13:04 alin_sinpalean Exp $
 */
class Util {
    static void putPacket(OutputStream out, byte[] data) throws IOException {
        short length = (short) (data.length + TdsPacket.HEADER_SIZE);
        ByteArrayOutputStream bos = new ByteArrayOutputStream(length);
        new TdsPacket((byte) 0x12, (byte) 0x01, length, (short) 0, (byte) 0,
                (byte) 0, data).write(bos);
        out.write(bos.toByteArray());
    }
}
