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
 * A caching implementation that performs no caching.  This cache implemenation is generally
 * only useful when the driver is not creating statement handles as it will save a slight
 * amount of memory and time by not performing any caching logic.
 *
 * @author Brian Heineman
 * @version $Id: NonCachingStatementCache.java,v 1.3 2004-10-25 19:33:40 bheineman Exp $
 */
public class NonCachingStatementCache implements StatementCache {
	/**
	 * Returns a statement handle associated with the specified key or <code>null</code>
	 * if the key specified does not have an associated statement handle.
	 *
     * @param key the statement key whose associated handle is to be returned
     * @return statement handle
	 */
	public Object get(String key) {
		return null;
	}

	/**
	 * Places the specified statement handle in the cache for the given key.  If
	 * a key already exists in the cache, the handle will be overwritten.
	 *
     * @param key the statement key to associated with the handle
     * @param handle the statement handle.
     */
	public void put(String key, Object handle) {
	}

	/**
	 * Removes a statement key and handle from the cache for the specified key.
	 *
     * @param key the statement key whose associated handle is to be removed
     *            from the cache
	 */
	public void remove(String key) {
	}

	/**
	 * Returns an array of obsolete statement handles that may be released,
	 * or <code>null</code> if no statement handles are obsolete.
	 *
     * @param handles the statement handles that are no longer being used.
	 * @return collection of obsolete statement handles to be removed
	 */
	public Collection getObsoleteHandles(Collection handles) {
		return handles;
	}
}
