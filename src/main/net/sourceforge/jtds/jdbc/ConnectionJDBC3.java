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

import java.util.*;
import java.sql.*;

/**
 * Implements JDBC 3.0 specific functionality. Separated from {@link
 * ConnectionJDBC2} in order to allow the same classes to run under both J2SE 1.3
 * (<code>ConnectionJDBC2</code>)and 1.4 (<code>ConnectionJDBC3</code>).
 *
 * @author Alin Sinpalean
 * @author Brian Heineman
 * @author Mike Hutchinson
 *  created    March 30, 2004
 * @version $Id: ConnectionJDBC3.java,v 1.1 2004-06-27 17:00:51 bheineman Exp $
 */
public class ConnectionJDBC3 extends ConnectionJDBC2 {
    /** The list of savepoints. */
    private ArrayList savepoints = null;
    private int savepointId = 0;

    /**
     * Create a new database connection.
     *
     * @param url The connection URL starting jdbc:jtds:.
     * @param props The additional connection properties.
     * @throws SQLException
     */
    ConnectionJDBC3(String url, Properties props) throws SQLException {
        super(url, props);
    }

    /**
     * Add a savepoint to the list maintained by this connection.
     *
     * @param savepoint The savepoint object to add.
     * @throws SQLException
     */
    private void setSavepoint(SavepointImpl savepoint) throws SQLException {
        Statement statement = null;

        try {
            statement = createStatement();
            statement.execute("SAVE TRAN jtds" + savepoint.getId());
        } finally {
            statement.close();
        }

        synchronized (this) {
            if (savepoints == null) {
                savepoints = new ArrayList();
            }

            savepoints.add(savepoint);
        }
    }

    /**
     * Releases all savepoints. Used internally when committing or rolling back
     * a transaction.
     */
    synchronized void clearSavepoints() {
        if (savepoints == null) {
            return;
        }

        savepoints.clear();
        savepointId = 0;
    }


// ------------- Methods implementing java.sql.Connection  -----------------

    public synchronized void releaseSavepoint(Savepoint savepoint)
            throws SQLException {
        checkOpen();

        if (savepoints == null) {
            throw new SQLException(
                Support.getMessage("error.connection.badsavep"), "25000");
        }

        int index = savepoints.indexOf(savepoint);

        if (index == -1) {
            throw new SQLException(
                Support.getMessage("error.connection.badsavep"), "25000");
        }

        savepoints.remove(index);
    }

    public synchronized void rollback(Savepoint savepoint) throws SQLException {
        checkOpen();

        if (savepoints == null) {
            throw new SQLException(
                Support.getMessage("error.connection.badsavep"), "25000");
        }

        int index = savepoints.indexOf(savepoint);

        if (index == -1) {
            throw new SQLException(
                Support.getMessage("error.connection.badsavep"), "25000");
        } else if (getAutoCommit()) {
            throw new SQLException(
                Support.getMessage("error.connection.savenorollback"), "25000");
        }

        Statement statement = null;

        try {
            statement = createStatement();
            statement.execute("ROLLBACK TRAN jtds" + ((SavepointImpl) savepoint).getId());
        } finally {
            statement.close();
        }

        int size = savepoints.size();

        for (int i = size - 1; i >= index; i--) {
            savepoints.remove(i);
        }
    }

    public Savepoint setSavepoint() throws SQLException {
        checkOpen();

        if (getAutoCommit()) {
            throw new SQLException(
                Support.getMessage("error.connection.savenoset"), "25000");
        }

        SavepointImpl savepoint = new SavepointImpl(getNextSavepointId());

        setSavepoint(savepoint);

        return savepoint;
    }

    public Savepoint setSavepoint(String name) throws SQLException {
        checkOpen();

        if (getAutoCommit()) {
            throw new SQLException(
                Support.getMessage("error.connection.savenoset"), "25000");
        } else if (name == null) {
            throw new SQLException(
                Support.getMessage("error.connection.savenullname", "savepoint"),
                "25000");
        }

        SavepointImpl savepoint = new SavepointImpl(getNextSavepointId(), name);

        setSavepoint(savepoint);

        return savepoint;
    }

    /**
     * Returns the next savepoint identifier.
     *
     * @return the next savepoint identifier
     */
    private synchronized int getNextSavepointId() {
        return ++savepointId;
    }
}
