//jTDS JDBC Driver for Microsoft SQL Server and Sybase
//Copyright (C) 2004 The jTDS Project
//
//This library is free software; you can redistribute it and/or
//modify it under the terms of the GNU Lesser General Public
//License as published by the Free Software Foundation; either
//version 2.1 of the License, or (at your option) any later version.
//
//This library is distributed in the hope that it will be useful,
//but WITHOUT ANY WARRANTY; without even the implied warranty of
//MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
//Lesser General Public License for more details.
//
//You should have received a copy of the GNU Lesser General Public
//License along with this library; if not, write to the Free Software
//Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
//
package net.sourceforge.jtds.jdbc;

/**
 * Associates a login or query timeout value with a target <code>Object</code>.
 *
 * @version $Id: TimerRequest.java,v 1.1 2005-02-01 04:57:57 alin_sinpalean Exp $
 */
public class TimerRequest {
    /** The time when this timeout will expire. */
    private long time;
    /** Target to notify when the timeout expires. */
    private Object target;

    /**
     * Create a <code>TimerRequest</code>.
     *
     * @param timeout the desired timeout in seconds
     * @param target  the target object; one of <code>SharedSocket</code> or
     *                <code>TdsCore</code>
     */
    TimerRequest(int timeout, Object target) {
        if (timeout <= 0) {
            throw new IllegalArgumentException("Invalid timeout parameter "
                    + timeout);
        }
        this.time = System.currentTimeMillis() + (timeout * 1000);
        this.target = target;
    }

    /**
     * Retrieve the time at which this timer request will expire.
     *
     * @return the time in milliseconds since the Unix epoch
     */
    long getTime() {
        return this.time;
    }

    /**
     * Execute the approriate action on the target Object.
     * <p/>
     * For a login timeout, when the timer expires the network socket is
     * closed, crashing any pending network I/O. Not elegant but it works!
     * <p/>
     * For a query timeout a cancel packet is sent to the server.
     */
    void fire() {
        // Execute operation on target
        if (target instanceof TdsCore) {
            // Query timeout
            ((TdsCore) target).cancel();
        } else if (target instanceof SharedSocket) {
            // Login timeout
            ((SharedSocket) target).forceClose();
        }
    }
}
