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
 * Implements a light-weight statement cache using a HashMap.
 *
 * @author Brian Heineman
 * @version $Id: DefaultStatementCache.java,v 1.1 2004-10-22 04:32:28 bheineman Exp $
 */
public class DefaultStatementCache extends AbstractStatementCache {
	private HashMap cache = new HashMap();
	
	/**
	 * Initializes the <code>maximumCacheTarget</code>.
	 * 
	 * @param maximumCacheTarget an integer representing the maximum cache size.
	 */
	public DefaultStatementCache(int maximumCacheTarget) {
		super(maximumCacheTarget);
	}
	
	/**
	 * Returns a statement handle associated with the specified key or <code>null</code>
	 * if the key specified does not have an associated statement handle.
	 * 
	 * @return statement handle.
     * @param key the statement key whose associated handle is to be returned.
	 */
	public synchronized Object get(String key) {
		return cache.get(key);
	}

	/**
	 * Places the specified statement handle in the cache for the given key.  If
	 * a key already exists in the cache, the handle will be overwritten.
	 * 
     * @param key the statement key to associated with the handle.
	 * @param handle the statement handle.
	 */
	public synchronized void put(String key, Object handle) {
		cache.put(key, handle);
		latch(handle);
	}

	/**
	 * Removes a statement key and handle from the cache for the specified key.
	 *  
     * @param key the statement key whose associated handle is to be removed from the cache.
	 */
	public synchronized void remove(String key) {
		removeLatches(cache.remove(key));
	}

	/**
	 * Returns <code>null</code> indicating no statement handles are obsolete.
	 * Over-riding implementations should return obsolete handles as is
	 * appropriate for the caching heuristic being used. 
	 * 
     * @param handle the statement handle that is no longer being used.
	 * @return <code>null</code>
	 */
	public Object[] getObsoleteHandles(String handle) {
		int cacheOverrun = cache.size() - maximumCacheTarget;
		
		unlatch(handle);

		if (cacheOverrun <= 0) {
			return null;
		}
		
		ArrayList handles = new ArrayList(cacheOverrun);
		
		for (Iterator iterator = cache.entrySet().iterator(); iterator.hasNext();) {
			Map.Entry mapEntry = (Map.Entry) iterator.next();
			
			if (!isLatched(mapEntry.getValue())) {
				handles.add(mapEntry.getValue());
				
				iterator.remove();
				
				if (--cacheOverrun == 0) {
					break;
				}
			}
		}
		
		if (handles.size() == 0) {
			return null;
		}
		
		return handles.toArray();
	}
}
