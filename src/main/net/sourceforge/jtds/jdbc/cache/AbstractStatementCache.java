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
 * Provides a base implementation for other caches.  Statement handle latching
 * is one of the services provided by this class.
 *
 * @author Brian Heineman
 * @version $Id: AbstractStatementCache.java,v 1.3 2004-10-25 19:33:40 bheineman Exp $
 */
abstract class AbstractStatementCache implements StatementCache {
	private static final Integer INTEGER_ONE = new Integer(1);

	/**
	 * An integer representing the maximum cache size.  This value is only a
	 * target and may be exceeded by a specific caching implementation.
	 * However, each cache implementation must make a "best effort" to adhere
	 * to this maximum cache limit.
	 */
	protected int maximumCacheTarget;

	/**
	 * A map of statement handle latches and the associated latch count.  Latches
	 * provide a mechanism for registering use of a statement handle and preventing
	 * a handle from being removed from the cache prematurely.  In general,
	 * specific cache implementations should add a latch or increment an existing
	 * latch count for a statement key when {@link #put} is called.  Conversely,
	 * the latch for a statement handles should be removed of decremented when
	 * {@link #getObsoleteHandles} is called.
	 */
	private HashMap latches = new HashMap();

	/**
	 * Initializes the <code>maximumCacheTarget</code>.
	 *
	 * @param maximumCacheTarget an integer representing the maximum cache size.
	 */
	protected AbstractStatementCache(int maximumCacheTarget) {
		this.maximumCacheTarget = maximumCacheTarget;
	}

	/**
	 * Adds a single latch to the given statement handle.
	 *
     * @param handle the statement handle to add a single latch for.
	 */
	protected void latch(Object handle) {
		// Convert ProcEntry to the procedure name/handle using ProcEntry.toString()
		if (handle != null) {
			handle = handle.toString();
		}

		Integer latchCount = (Integer) latches.get(handle);

		if (latchCount == null) {
			latches.put(handle, INTEGER_ONE);
		} else {
			// Increment the latch count
			latches.put(handle, new Integer(latchCount.intValue() + 1));
		}
	}

	/**
	 * Removes a single latch from each handle in the Collection.
	 *
     * @param handles the statement handles to remove a single latch from.
	 */
	protected void unlatch(Collection handles) {
		if (handles != null) {
			for (Iterator iterator = handles.iterator(); iterator.hasNext();) {
				unlatch(iterator.next());
			}
		}
	}
	
	/**
	 * Removes a single latch from the given statement handle.
	 *
     * @param handle the statement handle to remove a single latch from.
	 */
	protected void unlatch(Object handle) {
		// Convert ProcEntry to the procedure name/handle using ProcEntry.toString()
		if (handle != null) {
			handle = handle.toString();
		}

		Integer latchCount = (Integer) latches.get(handle);

		if (latchCount == null) {
			// nothing to unlatch.
			return;
		} else if (latchCount.intValue() == 1) {
			// Remove the latch entry since there are no more latches.
			latches.remove(handle);
		} else {
			// Decrement the latch count
			latches.put(handle, new Integer(latchCount.intValue() - 1));
		}
	}

	/**
	 * Removes all latches from the given statement handle.
	 *
     * @param handle the statement handle to remove all latches from.
	 */
	protected void removeLatches(Object handle) {
		// Convert ProcEntry to the procedure name/handle using ProcEntry.toString()
		if (handle != null) {
			handle = handle.toString();
		}

		latches.remove(handle);
	}

	/**
	 * Returns <code>true</code> if there are latches for the given statement handle.
	 *
     * @param handle the statement handle to check the latch status of.
	 * @return <code>true</code> if there are latches for the given statement handle;
	 *   <code>false</code> otherwise.
	 */
	protected boolean isLatched(Object handle) {
		// Convert ProcEntry to the procedure name/handle using ProcEntry.toString()
		if (handle != null) {
			handle = handle.toString();
		}

		return latches.containsKey(handle);
	}
}
