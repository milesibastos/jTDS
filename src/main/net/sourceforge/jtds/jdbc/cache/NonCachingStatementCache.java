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
package net.sourceforge.jtds.jdbc.cache;

import java.util.Collection;

/**
 * A caching implementation that performs no caching. This cache implementation
 * is generally only useful when the driver is not reusing statement handles
 * as it will save a slight amount of memory and time by not performing any
 * caching logic.
 *
 * @author Brian Heineman
 * @version $Id: NonCachingStatementCache.java,v 1.5 2004-10-26 16:03:10 bheineman Exp $
 */
public class NonCachingStatementCache implements StatementCache {
	/**
	 * Always returns <code>null</code> for the statement handle, as no caching
     * is performed.
	 *
     * @param key the statement key whose associated handle is to be returned
     * @return statement handle
	 */
	public Object get(String key) {
		return null;
	}

	/**
	 * Does nothing.
	 *
     * @param key the statement key to associated with the handle
     * @param handle the statement handle.
     */
	public void put(String key, Object handle) {
	}

	/**
	 * Does nothing.
	 *
     * @param key the statement key whose associated handle is to be removed
     *            from the cache
	 */
	public void remove(String key) {
	}

	/**
	 * Returns the same <code>Collection</code> of statement handles received
     * as parameter, to ensure the <code>Statement</code> releases all the
     * handles it has used.
	 *
     * @param handles the statement handles that are no longer being used
	 * @return the same <code>Collection</code> received as parameter
	 */
	public Collection getObsoleteHandles(Collection handles) {
		return handles;
	}
}
