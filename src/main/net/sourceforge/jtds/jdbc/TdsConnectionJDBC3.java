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
 * TdsConnection} in order to allow the same classes to run under both J2SE 1.3
 * (<code>TdsConnection</code>)and 1.4 (<code>TdsConnectionJDBC3</code>).
 *
 * @author     Alin Sinpalean
 * @author     Brian Heineman
 * @author     Mike Hutchinson
 * @created    March 30, 2004
 * @version    $Id: TdsConnectionJDBC3.java,v 1.2 2004-04-01 20:22:38 bheineman Exp $
 */
public class TdsConnectionJDBC3 extends TdsConnection {
    /**
     * CVS revision of the file.
     */
    public final static String cvsVersion = "$Id: TdsConnectionJDBC3.java,v 1.2 2004-04-01 20:22:38 bheineman Exp $";

    private ArrayList savepoints = null;

    /**
     * Create a <code>Connection</code> to a database server.
     *
     * @param  props            <code>Properties</code> of the new
     *     <code>Connection</code> instance
     * @exception  SQLException  if a database access error occurs
     * @exception  TdsException  if a network protocol error occurs
     */
    public TdsConnectionJDBC3(Properties props)
            throws SQLException, TdsException {
        super(props);
    }

    public synchronized void releaseSavepoint(Savepoint savepoint)
            throws SQLException {
        checkClosed();

        if (savepoints == null) {
            throw new SQLException("savepoint is not valid for this transaction");
        }

        int index = savepoints.indexOf(savepoint);

        if (index == -1) {
            throw new SQLException("savepoint is not valid for this transaction");
        }

        savepoints.remove(index);

        if (savepoint instanceof SavepointImpl) {
            ((SavepointImpl) savepoint).release();
        }
    }

    public synchronized void rollback(Savepoint savepoint) throws SQLException {
        checkClosed();

        if (savepoints == null) {
            throw new SQLException("savepoint is not valid for this transaction");
        }

        int index = savepoints.indexOf(savepoint);

        if (index == -1) {
            throw new SQLException("savepoint is not valid for this transaction");
        } else if (getAutoCommit()) {
            throw new SQLException("savepoints cannot be rolled back in auto-commit mode");
        }

        Statement statement = null;

        try {
            statement = createStatement();
            statement.execute("ROLLBACK TRAN s" + ((SavepointImpl) savepoint).getId());
        } finally {
            statement.close();
        }

        int size = savepoints.size();

        for (int i = size - 1; i >= index; i--) {
            savepoints.remove(i);
        }
    }

    public synchronized Savepoint setSavepoint() throws SQLException {
        checkClosed();

        if (getAutoCommit()) {
            throw new SQLException("savepoints cannot be set in auto-commit mode");
        }

        if (savepoints == null) {
            savepoints = new ArrayList();
        }

        SavepointImpl savepoint = new SavepointImpl(savepoints.size() + 1);

        setSavepoint(savepoint);

        return savepoint;
    }

    public synchronized Savepoint setSavepoint(String name) throws SQLException {
        checkClosed();

        if (getAutoCommit()) {
            throw new SQLException("savepoints cannot be set in auto-commit mode");
        } else if (name == null) {
            throw new SQLException("savepoint name cannot be null");
        }

        if (savepoints == null) {
            savepoints = new ArrayList();
        }

        SavepointImpl savepoint = new SavepointImpl(savepoints.size() + 1, name);

        setSavepoint(savepoint);

        return savepoint;
    }

    private void setSavepoint(SavepointImpl savepoint) throws SQLException {
        Statement statement = null;

        try {
            statement = createStatement();
            statement.execute("SAVE TRAN s" + savepoint.getId());
        } finally {
            statement.close();
        }

        savepoints.add(savepoint);
    }

    /**
     * Releases all savepoints. Used internally when committing or rolling back
     * a transaction.
     */
    synchronized void clearSavepoints() {
        if (savepoints == null) {
            return;
        }

        for (Iterator iterator = savepoints.iterator(); iterator.hasNext();) {
            SavepointImpl savepoint = (SavepointImpl) iterator.next();

            savepoint.release();
        }

        savepoints.clear();
    }
}
