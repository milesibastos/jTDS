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
package net.sourceforge.jtds.jdbc;

/**
 * This class encapsulates a column data value.
 *
 * @author Mike Hutchinson
 * @version $Id: ColData.java,v 1.4 2004-07-29 00:14:53 ddkilzer Exp $
 */
public class ColData {
    /** The column value or null */
    private Object value = null;
    /** True if the column has been updated. */
    private boolean updated = false;
    /** The length of stream, String or byte[] values. */
    private int length = 0;

    /**
     * Construct a column data item with an initial value.
     *
     * @param valueIn The initial data value for this column.
     */
    ColData(Object valueIn, int tdsVersion) {
        value  = valueIn;

        if (value instanceof String) {
            length = ((String) value).length();

            if (tdsVersion < Driver.TDS70 && ((String) value).equals(" ")){
                value = "";
                length = 0;
            }
        } else if (this.value instanceof byte[]) {
            length = ((byte[]) this.value).length;
        }
    }

    /**
     * Construct a column data item with a null value.
     */
    ColData() {
    }

    /**
     * Convert the column value to a String literal.
     *
     * @return The column value as a <code>String</code>.
     */
    public String toString() {
        if (value == null) {
            return null;
        }

        if (value instanceof byte[]) {
            return Support.toHex((byte[]) value);
        } else if (value instanceof Boolean) {
            return((Boolean) value).booleanValue() ? "1" : "0";
        }

        return value.toString();
    }

    /**
     * Get the null status of this column.
     *
     * @return <code>boolean</code> true if column is null.
     */
    boolean isNull() {
        return value == null;
    }

    /**
     * Get the update status of this column.
     *
     * @return <code>boolean</code> true if column has been updated.
     */
    boolean isUpdated() {
        return this.updated;
    }

    /**
     * Set this column's value.
     *
     * @param value The new column value.
     */
    void setValue(Object value) {
        this.value = value;

        if (this.value instanceof String) {
            length = ((String) this.value).length();
        } else if (this.value instanceof byte[]) {
            length = ((byte[]) this.value).length;
        } else {
            length = 0;
        }

        this.updated = true;
    }

    /**
     * Retrieve this column's value.
     *
     * @return The column value as an <code>Object</code>.
     */
    Object getValue() {
        return this.value;
    }

    /**
     * Set the length of the column data.
     *
     * @param length The length value.
     */
    void setLength(int length) {
        this.length = length;
    }

    /**
     * Retrieve the length of this columns data.
     *
     * @return The column data length as a <code>int</code>.
     */
    int getLength() {
        return this.length;
    }
}
