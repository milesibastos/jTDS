//
// Copyright 1998 CDS Networks, Inc., Medford Oregon
//
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
// 1. Redistributions of source code must retain the above copyright
//    notice, this list of conditions and the following disclaimer.
// 2. Redistributions in binary form must reproduce the above copyright
//    notice, this list of conditions and the following disclaimer in the
//    documentation and/or other materials provided with the distribution.
// 3. All advertising materials mentioning features or use of this software
//    must display the following acknowledgement:
//      This product includes software developed by CDS Networks, Inc.
// 4. The name of CDS Networks, Inc.  may not be used to endorse or promote
//    products derived from this software without specific prior
//    written permission.
//
// THIS SOFTWARE IS PROVIDED BY CDS NETWORKS, INC. ``AS IS'' AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED.  IN NO EVENT SHALL CDS NETWORKS, INC. BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
// OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
// HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
// LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
// OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
// SUCH DAMAGE.
//


package net.sourceforge.jtds.jdbc;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Vector;

/**
 * Encapsulate the information from one row of a result set.
 *
 * @author     Craig Spannring
 * @created    17 March 2001
 */
public class PacketRowResult extends PacketResult {

    Vector row = null;
    Context context;

    boolean wasNull = false;
    /**
     *  /** @todo Description of the Field
     */
    public final static String cvsVersion = "$Id: PacketRowResult.java,v 1.10 2004-05-02 04:08:08 bheineman Exp $";

    public PacketRowResult(Context context) {
        super(TdsDefinitions.TDS_ROW);
        this.context = context;
        row = new Vector(realColumnCount());
        row.setSize(realColumnCount());
    }

    public Context getContext() {
        return context;
    }

    /**
     * Sets the component at the specified index of this vector to be the
     * specified object. The previous component at that position is discarded.
     *
     * <UL>Note- <\UL>Unlike the vector class this class starts the index with
     * 1, not 0.
     *
     * @param  obj               The object to store
     * @param  index             Index to store the element at. First element is
     *      at index 1
     */
    public void setElementAt(int index, Object obj)
            throws SQLException {
        if (index < 1 || index > realColumnCount()) {
            throw new SQLException("Bad index " + index);
        }

        if (obj != null) {
            int colType = context.getColumnInfo().getNativeType(index);

            switch (colType) {
            case Tds.SYBINTN:
            case Tds.SYBINT1:
            case Tds.SYBINT2:
            case Tds.SYBINT4:
                if (!(obj instanceof Number)) {
                    obj = Integer.valueOf(obj.toString());
                }
                break;

            case Tds.SYBIMAGE:
            case Tds.SYBVARBINARY:
            case Tds.SYBBINARY:
                if (!(obj instanceof byte[])) {
                    throw new SQLException(
                            "Only byte[] accepted for IMAGE/BINARY fields.");
                }
                break;

            case Tds.SYBTEXT:
            case Tds.SYBNTEXT:
            case Tds.SYBCHAR:
            case Tds.SYBVARCHAR:
            case Tds.SYBNCHAR:
            case Tds.SYBNVARCHAR:
            case Tds.SYBUNIQUEID:
                // Anything goes here
                break;

            case Tds.SYBREAL:
            case Tds.SYBFLT8:
            case Tds.SYBFLTN:
                if (!(obj instanceof Number)) {
                    obj = Double.valueOf(obj.toString());
                }
                break;

            case Tds.SYBSMALLMONEY:
            case Tds.SYBMONEY:
            case Tds.SYBMONEYN:
            case Tds.SYBNUMERIC:
            case Tds.SYBDECIMAL:
                if (!(obj instanceof Number)) {
                    obj = new BigDecimal(obj.toString());
                }
                break;

            case Tds.SYBDATETIME4:
            case Tds.SYBDATETIMN:
            case Tds.SYBDATETIME:
                if (obj instanceof java.util.Date) {

                } else {
                    throw new SQLException(
                            "Only java.util.Date values accepted for DATETIME fields.");
                }
                break;

            case Tds.SYBBITN:
            case Tds.SYBBIT:
                if (obj instanceof Boolean) {
                    break;
                }
                if (obj instanceof Number) {
                    obj = new Boolean(((Number) obj).intValue() != 0);
                } else {
                    obj = new Boolean(!"0".equals(obj.toString()));
                }
                break;

            default:
                throw new SQLException("Don't now how to handle " +
                                       "column type 0x" +
                                       Integer.toHexString(colType));
            }
        }

        row.setElementAt(obj, index - 1);
    }

    private int realColumnCount() {
        return context.getColumnInfo().realColumnCount();
    }

    private int getColumnType(int index)
            throws SQLException {
        return context.getColumnInfo().getJdbcType(index);
    }

    /**
     * Get the element at the specified index.<p>
     *
     * <UL>Note- <\UL>Unlike the vector class this starts the index with 1, not
     * 0.
     *
     * @param  index             Index to get the element from. First element is
     *                           at index 1
     * @return                   the ElementAt value
     * @exception  SQLException  if the index is out of bounds
     */
    public Object getElementAt(int index)
            throws SQLException {
        if (index < 1 || index > realColumnCount()) {
            throw new SQLException("Bad index " + index);
        }

        Object res = row.elementAt(index - 1);
        wasNull = (res == null);
        return res;
    }

    public Object getObject(int columnIndex) throws SQLException {
        // This method is implicitly coupled to the getRow() method in the
        // Tds class.  Every type that getRow() could return must
        // be handled in this method.
        //
        // The object type returned by getRow() must correspond with the
        // jdbc SQL type in the switch statement below.
        //
        // Note-  The JDBC spec (version 1.20) does not define the type
        // of the Object returned for LONGVARCHAR data.

        // XXX-  Needs modifications for JDBC 2.0

        // SAfe Now this method must support any type that can be set with the
        //      updateXXX methods of ResultSet (after they have been filtered
        //      by setElementAt).

        return getObjectAs(columnIndex, getColumnType(columnIndex));
    }

    public Object getObjectAs(int columnIndex, int sqlType) throws SQLException {
        return ParameterUtils.getObject(sqlType,
                                        getElementAt(columnIndex),
                                        context.getEncoder());
    }

    public boolean wasNull() {
        return wasNull;
    }
}
