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

import java.util.*;

/**
 * Implements a fast, low-overhead statement cache using a <code>HashMap</code>.
 * This cache will never release objects and therefore does not incur the
 * overhead associated with latching.  This cache is ideal for environments
 * where there is known to be a limited number of unique handles created during
 * the life of the connection.
 *
 * @author Brian Heineman
 * @version $Id: FastStatementCache.java,v 1.1 2004-10-25 19:33:40 bheineman Exp $
 */
public class FastStatementCache implements StatementCache {
	private HashMap cache = new HashMap();

	/**
	 * Returns a statement handle associated with the specified key or
	 * <code>null</code> if the key specified does not have an associated
	 * statement handle.
	 *
	 * @param key the statement key whose associated handle is to be returned
	 * @return statement handle
	 */
	public synchronized Object get(String key) {
		return cache.get(key);
	}

	/**
	 * Places the specified statement handle in the cache for the given key. If
	 * a key already exists in the cache, the handle will be overwritten.
	 *
	 * @param key the statement key to associated with the handle
	 * @param handle the statement handle
	 */
	public synchronized void put(String key, Object handle) {
		cache.put(key, handle);
	}

	/**
	 * Removes a statement key and handle from the cache for the specified key.
	 *
	 * @param key the statement key whose associated handle is to be removed
	 *            from the cache
	 */
	public synchronized void remove(String key) {
		cache.remove(key);
	}

	/**
	 * Removes and returns just enough statement handles to reduce the number
	 * of cached statements to {@link #maximumCacheTarget}, if that's possible
	 * (it might happen that all statements are actually in use and they cannot
	 * be removed).
	 *
	 * @param handles the statement handles that are no longer being used.
	 * @return collection of obsolete statement handles to be removed
	 */
	public Collection getObsoleteHandles(Collection handles) {
		return null;
	}
}
