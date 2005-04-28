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
package net.sourceforge.jtds.util;

import java.util.LinkedList;
import java.util.ListIterator;

/**
 * Simple timer class used to implement login and query timeouts.
 * <p/>
 * This thread runs as a Daemon thread to ensure that the java VM will exit
 * correctly when normal execution is complete.
 * <p/>
 * It provides both a singleton implementation and a default constructor for
 * the case when more than one timer thread is desired.
 *
 * @author Alin Sinpalean
 * @author Mike Hutchinson
 * @version $Id: TimerThread.java,v 1.5 2005-04-28 14:29:31 alin_sinpalean Exp $
 */
public class TimerThread extends Thread {
    /**
     * Interface to be implemented by classes that request timer services.
     */
    public interface TimerListener {
        /**
         * Event to be fired when the timeout expires.
         */
        void timerExpired();
    }

    /**
     * Internal class associating a login or query timeout value with a target
     * <code>TimerListener</code>.
     */
    private static class TimerRequest {
        /** The time when this timeout will expire. */
        final long time;
        /** Target to notify when the timeout expires. */
        final TimerListener target;

        /**
         * Create a <code>TimerRequest</code>.
         *
         * @param timeout the desired timeout in milliseconds
         * @param target  the target object; one of <code>SharedSocket</code> or
         *                <code>TdsCore</code>
         * @throws IllegalArgumentException if the timeout is negative or 0
         */
        TimerRequest(int timeout, TimerListener target) {
            if (timeout <= 0) {
                throw new IllegalArgumentException("Invalid timeout parameter "
                        + timeout);
            }
            this.time = System.currentTimeMillis() + (timeout);
            this.target = target;
        }
    }

    /** Singleton instance. */
    private static TimerThread instance;

    /** List of <code>TimerRequest</code>s to execute, ordered by time. */
    private final LinkedList timerList = new LinkedList();
    /** Time when the first request time out should occur. */
    private long nextTimeout;

    /**
     * Singleton getter.
     */
    public static synchronized TimerThread getInstance() {
        if (instance == null) {
            instance = new TimerThread();
            instance.start();
        }
        return instance;
    }

    /**
     * Construct a new <code>TimerThread</code> instance.
     */
    public TimerThread() {
        // Set the thread name
        super("jTDS TimerThread");
        // Ensure that this thread does not prevent the VM from exiting
        this.setDaemon(true);
    }

    /**
     * Execute the <code>TimerThread</code> main loop.
     */
    public void run() {
        synchronized (timerList) {
            while (true) {
                try {
                    try {
                        // If nextTimeout == 0 (i.e. there are no more requests
                        // in the queue) wait indefinitely -- wait(0)
                        timerList.wait(nextTimeout == 0 ? 0
                                : nextTimeout - System.currentTimeMillis());
                    } catch (IllegalArgumentException ex) {
                        // Timeout was negative, fire timeout
                    }

                    // Fire expired timeout requests
                    long time = System.currentTimeMillis();
                    while (!timerList.isEmpty()) {
                        // Examime the head of the list and see
                        // if the timer has expired.
                        TimerRequest t = (TimerRequest) timerList.getFirst();
                        if (t.time > time) {
                            break; // No timers have expired
                        }
                        // Notify target of timeout
                        t.target.timerExpired();
                        // Remove the fired timeout request
                        timerList.removeFirst();
                    }

                    // Determine next timeout
                    updateNextTimeout();
                } catch (InterruptedException e) {
                    // nop
                }
            }
        }
    }

    /**
     * Add a timer request to the queue.
     * <p/>
     * The queue is ordered by time so that the head of the list is always the
     * first timer to expire.
     *
     * @param timeout the interval in milliseconds after which the timer will
     *                expire
     * @param l       <code>TimerListener</code> to be notified on timeout
     * @return a handle to the timer request, that can later be used with
     *         <code>cancelTimer</code>
     */
    public Object setTimer(int timeout, TimerListener l) {
        // Create a new timer request
        TimerRequest t = new TimerRequest(timeout, l);

        synchronized (timerList) {
            if (timerList.isEmpty()) {
                // List was empty, just add new request
                timerList.add(t);
            } else {
                // Tiny optimization; new requests will usually go to the end
                TimerRequest crt = (TimerRequest) timerList.getLast();
                if (t.time >= crt.time) {
                    timerList.addLast(t);
                } else {
                    // Iterate the list and insert it into the right place
                    for (ListIterator li = timerList.listIterator(); li.hasNext(); ) {
                        crt = (TimerRequest) li.next();
                        if (t.time < crt.time) {
                            li.previous();
                            li.add(t);
                            break;
                        }
                    }
                }
            }

            // If this request is now the first in the list, interupt timer
            if (timerList.getFirst() == t) {
                nextTimeout = t.time;
                this.interrupt();
            }
        }

        // Return the created request as timer handle
        return t;
    }

    /**
     * Remove a redundant timer before it expires.
     *
     * @param handle handle to the request to be removed from the queue (a
     *        <code>TimerRequest</code> instance)
     * @return <code>true</code> if timer had not expired
     */
    public boolean cancelTimer(Object handle) {
        TimerRequest t = (TimerRequest) handle;

        synchronized (timerList) {
            boolean result = timerList.remove(t);
            if (nextTimeout == t.time) {
                updateNextTimeout();
            }
            return result;
        }
    }

    /**
     * Check whether a timer has expired.
     *
     * @param handle handle to the request to be checked for expiry (a
     *        <code>TimerRequest</code> instance)
     * @return <code>true</code> if timer has expired
     */
    public boolean hasExpired(Object handle) {
        TimerRequest t = (TimerRequest) handle;

        synchronized (timerList) {
            return !timerList.contains(t);
        }
    }

    /** Internal method that updates the value of {@link #nextTimeout}. */
    private void updateNextTimeout() {
        nextTimeout = timerList.isEmpty() ? 0
                : ((TimerRequest) timerList.getFirst()).time;
    }
}
