// jTDS JDBC Driver for Microsoft SQL Server and Sybase
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

/**
 * This class encapsualtes the SQL Server Text Pointer object that
 * describes the location of text and image column data.
 *
 * @author Mike Hutchinson
 * @version $Id: TextPtr.java,v 1.4 2004-10-27 14:57:45 alin_sinpalean Exp $
 */
public class TextPtr {
    /** The 16 byte Text Pointer. */
    byte[] ptr = new byte[16];
    /** The 8 byte timestamp. */
    byte[] ts = new byte[8];
    /** The length of the text or image data. */
    int len;
    /** True if the data has actually been read from the server. */
//    boolean isRead; // Not used
    /** The actual String or byte[] value. */
    Object value;

    /**
     * Construct an empty TextPtr object.
     */
    TextPtr() {
    }
}
