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

/**
 * Interface for a statement cache.  Abstraction of the caching mechanism by use of this
 * interface will allow developers to create custom caching schemes that are optimal for
 * their specific applications.  Any synchronization required by an implementation should
 * utilize the implementing object for the lock.
 * <p>
 * The caching types provided by jTDS should be:
 * <ul>
 *   <li>Arbitrary first un-latched (initial default until other caches are implemented)</li>
 *   <li>FIFO</li>
 *   <li>LRU</li>
 *   <li>No caching</li>
 *   <li>Touch Count / Most Frequently Used</li>
 * </ul>
 *
 * @author Brian Heineman
 * @version $Id: StatementCache.java,v 1.1 2004-10-22 04:32:28 bheineman Exp $
 */
public interface StatementCache {
	/**
	 * Returns a statement handle associated with the specified key or <code>null</code>
	 * if the key specified does not have an associated statement handle.
	 * 
	 * @return statement handle.
     * @param key the statement key whose associated handle is to be returned.
	 */
	public Object get(String key);

	/**
	 * Places the specified statement handle in the cache for the given key.  If
	 * a key already exists in the cache, the handle will be overwritten.
	 * 
     * @param key the statement key to associated with the handle.
	 * @param handle the statement handle.
	 */
	public void put(String key, Object handle);

	/**
	 * Removes a statement key and handle from the cache for the specified key.
	 *  
     * @param key the statement key whose associated handle is to be removed from the cache.
	 */
	public void remove(String key);

	/**
	 * Returns an array of obsolete statement handles that may be released,
	 * or <code>null</code> if no statement handles are obsolete.
	 * 
     * @param handle the statement handle that is no longer being used.
	 * @return array of obsolete statement handles.
	 */
	public Object[] getObsoleteHandles(String handle);
}
