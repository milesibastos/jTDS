//jTDS JDBC Driver for Microsoft SQL Server and Sybase
//Copyright (C) 2004 The jTDS Project
//
//This library is free software; you can redistribute it and/or
//modify it under the terms of the GNU Lesser General Public
//License as published by the Free Software Foundation; either
//version 2.1 of the License, or (at your option) any later version.
//
//This library is distributed in the hope that it will be useful,
//but WITHOUT ANY WARRANTY; without even the implied warranty of
//MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
//Lesser General Public License for more details.
//
//You should have received a copy of the GNU Lesser General Public
//License along with this library; if not, write to the Free Software
//Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
//
package net.sourceforge.jtds.ssl;

import java.io.IOException;
import java.io.OutputStream;

import net.sourceforge.jtds.util.BytesView;

/**
 * Base class for all SSL (and TLS) records.
 *
 * @author Rob Worsnop
 * @version $Id: Record.java,v 1.1 2005-01-04 17:13:04 alin_sinpalean Exp $
 */
abstract class Record {
    private BytesView data;

    protected Record(BytesView data) {
        this.data = data;
    }

    /**
     * Writes the record out to an output stream.
     *
     * @param out the output stream
     */
    public void write(OutputStream out) throws IOException {
        data.write(out);
    }

    /**
     * Detaches the data so the underlying bytes cannot be modified elsewhere.
     */
    public void detachData() {
        data = data.deepClone();
    }

    /**
     * Returns the length of the record.
     *
     * @return the length of the record.
     */
    public int getLength() {
        return data.getLength();
    }
}
