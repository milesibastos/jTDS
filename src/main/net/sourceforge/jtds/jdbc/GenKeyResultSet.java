/*
 * GenKeyResultSet.java Created on 24-Jan-2004
 *
 */
package net.sourceforge.jtds.jdbc;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
/**
 * Implement a specialised result set to return the IDENTITY column value
 * via the getGeneratedKeys() method of the Statement object.
 * 
 * @author Mike Hutchinson
 *
 */
public class GenKeyResultSet extends AbstractResultSet {
	private Statement 		stmt;
	private Context 		context = null;
	private PacketRowResult data = null;
	private int 			row = 0;
	
	/**
	 * Constructor creates an empty ResultSet.
	 * @param stmt The parent Statement object
	 */
	public GenKeyResultSet(Statement stmt)
	{
		this.stmt    = stmt;
		this.context = null;
		this.data    = null;
	}
	
	/**
	 * Constructor creates this ResultSet as a partial clone of the 
	 * ResultSet returned by the call to SELECT @@IDENTITY.
	 * @param stmt The parent Statement object
	 * @param results The current result set containing the generated key.
	 */
	public GenKeyResultSet(Statement stmt, TdsResultSet results)
	{
		try {
			this.stmt = stmt;
			// Use the Context and PacketRowResult objects from the above
			// select to construct a special result set.
			this.context = results.getContext();
			this.data = null;
			if (results.next()) {
				this.data = results.currentRow();
				if (results.wasNull())
					this.data = null; // Need to return empty result set in this case
			}
		} catch(Exception e) {
			// Any errors just setup empty result set
			this.context = null;
			this.data = null;
		}
	}
	/**
	 * Get the result set context information creating a dummy context
	 * if this is an empty result set.
	 */
	public Context getContext() {
		if (this.context == null) {
			//
			// Create an empty result set if no generated keys
			// were available.
			//
			Columns cols = new Columns(1);
			this.context = new Context(cols, null);
			cols.setNativeType(1, Tds.SYBINT4);
				cols.setLabel(1, "ID");
				cols.setName(1, "ID");
		}
		return this.context;
	}
	
	/**
	 * Return the data to the caller (AbstractResultSet).
	 */
	public PacketRowResult currentRow() throws SQLException {
		return this.data;
	}

	public int getConcurrency() throws SQLException {
		return TYPE_FORWARD_ONLY;
	}

	public int getFetchDirection() throws SQLException {
		return FETCH_FORWARD;
	}

	public int getFetchSize() throws SQLException {
		return 0;
	}

	public int getRow() throws SQLException {
		return (row == 1)? 1: 0;
	}

	public int getType() throws SQLException {
		return TYPE_FORWARD_ONLY;
	}

	public void afterLast() throws SQLException {
	}

	public void beforeFirst() throws SQLException {
	}

	public void cancelRowUpdates() throws SQLException {
	}

	public void clearWarnings() throws SQLException {
	}

	public void close() throws SQLException {
	}

	public void deleteRow() throws SQLException {
	}

	public void insertRow() throws SQLException {
	}

	public void moveToCurrentRow() throws SQLException {
	}

	public void moveToInsertRow() throws SQLException {
	}

	public void refreshRow() throws SQLException {
	}

	public void updateRow() throws SQLException {
	}

	public boolean first() throws SQLException {
		return false;
	}

	public boolean isAfterLast() throws SQLException {
		return false;
	}

	public boolean isBeforeFirst() throws SQLException {
		return false;
	}

	public boolean isFirst() throws SQLException {
		return false;
	}

	public boolean isLast() throws SQLException {
		return false;
	}

	public boolean last() throws SQLException {
		return false;
	}

	public boolean next() throws SQLException {
		if (row < 2 && data != null)
			row++;
		return row == 1;
	}

	public boolean previous() throws SQLException {
		return false;
	}

	public boolean rowDeleted() throws SQLException {
		return false;
	}

	public boolean rowInserted() throws SQLException {
		return false;
	}

	public boolean rowUpdated() throws SQLException {
		return false;
	}

	public void setFetchDirection(int arg0) throws SQLException {
	}

	public void setFetchSize(int arg0) throws SQLException {
	}

	public boolean absolute(int arg0) throws SQLException {
		return false;
	}

	public boolean relative(int arg0) throws SQLException {
		return false;
	}

	public String getCursorName() throws SQLException {
		return null;
	}

	public SQLWarning getWarnings() throws SQLException {
		return null;
	}

	public Statement getStatement() throws SQLException {
		return this.stmt;
	}

}
