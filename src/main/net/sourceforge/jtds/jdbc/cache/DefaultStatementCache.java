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
 * Implements a light-weight statement cache using a <code>HashMap</code>.
 *
 * @author Brian Heineman
 * @version $Id: DefaultStatementCache.java,v 1.4 2004-10-26 12:53:36 alin_sinpalean Exp $
 */
public class DefaultStatementCache extends AbstractStatementCache {
	private HashMap cache = new HashMap();

	/**
	 * Initializes the <code>maximumCacheTarget</code>.
	 *
	 * @param maximumCacheTarget an integer representing the maximum cache size
	 */
	public DefaultStatementCache(int maximumCacheTarget) {
		super(maximumCacheTarget);
	}

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
		latch(handle);
	}

	/**
	 * Removes a statement key and handle from the cache for the specified key.
	 *
     * @param key the statement key whose associated handle is to be removed
     *            from the cache
	 */
	public synchronized void remove(String key) {
		removeLatches(cache.remove(key));
	}

	/**
	 * Removes and returns just enough statement handles to reduce the number
     * of cached statements to {@link #maximumCacheTarget}, if that's possible
     * (it might happen that all statements are actually in use and they cannot
     * be removed).
	 *
     * @param handles the statement handles that are no longer being used
	 * @return <code>Collection</code> of obsolete statement handles to be
     *         removed
	 */
	public synchronized Collection getObsoleteHandles(Collection handles) {
		int cacheOverrun = cache.size() - maximumCacheTarget;

		unlatch(handles);

		if (cacheOverrun <= 0) {
			return null;
		}

		ArrayList obsoleteHandles = new ArrayList(cacheOverrun);

		for (Iterator iterator = cache.entrySet().iterator(); iterator.hasNext();) {
			Map.Entry mapEntry = (Map.Entry) iterator.next();

			if (!isLatched(mapEntry.getValue())) {
				obsoleteHandles.add(mapEntry.getValue());

				iterator.remove();

				if (--cacheOverrun == 0) {
					break;
				}
			}
		}

		if (obsoleteHandles.size() == 0) {
			return null;
		}

		return obsoleteHandles;
	}
}
