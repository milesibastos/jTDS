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

import java.sql.SQLException;

/**
 * Simple result set class to return single row for getGeneratedKeys() or
 * empty result sets eg in DatabaseMetaData.
 *
 * @author Mike Hutchinson.
 * @version $Id: DummyResultSet.java,v 1.1 2004-06-27 17:00:51 bheineman Exp $
 */
public class DummyResultSet extends JtdsResultSet {
    /** Single row of data to return in this result set. */
    private ColData[] dummyRow;

    /**
     * Construct a simple result set for metadata or generated keys.
     *
     * @param statement The parent statement object.
     * @param columns The array of column descriptors for the result set row.
     * @param rowData The result set data or null for empty result sets.
     * @throws SQLException
     */
    DummyResultSet(JtdsStatement statement,
                   ColInfo[] columns,
                   ColData[] rowData)
    throws SQLException {
        super(statement, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, null, false);
        this.columns = columns;
        columnCount = columns.length;
        dummyRow = rowData;
        rowsInResult = (dummyRow == null) ? 0 : 1;
    }

    public boolean next() throws SQLException {
        checkOpen();

        if (dummyRow != null) {
            // Return the single row in our fake result set
            currentRow = dummyRow;
            dummyRow = null;
            pos = 1;
        } else {
            currentRow = null;
            pos = POS_AFTER_LAST;
        }

        return currentRow != null;
    }

    public boolean isLast() throws SQLException {
        checkOpen();
        return(pos == rowsInResult) && (rowsInResult != 0);
    }

    public void close() throws SQLException {
        closed = true;
        statement = null;
    }
}
