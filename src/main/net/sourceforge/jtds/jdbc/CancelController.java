//
// Copyright 1998 CDS Networks, Inc., Medford Oregon
//
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
// 1. Redistributions of source code must retain the above copyright
//    notice, this list of conditions and the following disclaimer.
// 2. Redistributions in binary form must reproduce the above copyright
//    notice, this list of conditions and the following disclaimer in the
//    documentation and/or other materials provided with the distribution.
// 3. All advertising materials mentioning features or use of this software
//    must display the following acknowledgement:
//      This product includes software developed by CDS Networks, Inc.
// 4. The name of CDS Networks, Inc.  may not be used to endorse or promote
//    products derived from this software without specific prior
//    written permission.
//
// THIS SOFTWARE IS PROVIDED BY CDS NETWORKS, INC. ``AS IS'' AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED.  IN NO EVENT SHALL CDS NETWORKS, INC. BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
// OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
// HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
// LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
// OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
// SUCH DAMAGE.
//

package net.sourceforge.jtds.jdbc;

/**
 * This class provides support for canceling queries.
 * <p>
 * Basically all threads can be divided into two groups, workers and cancellers. The canceler can
 * cancel at any time, even when there is no outstanding query to cancel. A worker can be in one of
 * 4 states:
 * <ol>
 * <li> not doing anything DB related
 * <li> currently sending a request to the database (Note: Any time a request is sent to the DB the
 *      DB will send a response. This means a thread in state 2 must go to state 3.)
 * <li> waiting for a response from DB
 * <li> reading the response from DB
 * </ol>
 * I can easily make it so that only one thread at a time can be in state 2, 3, or 4.
 * <p>
 * The way that a cancel works in TDS is that you send a cancel packet to server. The server will
 * then stop whatever it might be doing and reply with an <code>END_OF_DATA</code> packet with the
 * cancel flag set. (It sends this packet even if it wasn't doing anything.) I will call this packet
 * a <code>CANCEL_ACK</code> packet.
 * <p>
 * All that I really need is to do is to only have one cancel request outstanding and make sure that
 * some thread is out there ready to read the <code>CANCEL_ACK</code>. There is no point in sending
 * one request for each canceller.
 * <p>
 * Clearly if all my worker threads are in state 1 then the cancel request could be just a nop.
 * <p>
 * If I have some worker thread in state 2, 3, or 4 I think I will be fine if I just make sure that
 * the thread reads until the <code>CANCEL_ACK</code> packet.
 * <p>
 * I think I will just have a control object that has one <code>boolean</code>,
 * <code>readInProgress</code> and two <code>int</code>s, <code>cancelsRequested</code> and
 * <code>cancelsProcessed</code>.
 * <p>
 * The <code>doCancel()</code> method will:
 * <ul>
 * <li> lock the object
 * <li> if there is no read in progress or there is an outstanding cancel request it will unlock and
 *      return.
 * <li> otherwise it will send the cancel packet
 * <li> increment the <code>cancelsRequested</code>
 * <li> unlock the control object.
 * </ul>
 * Whenever the worker thread wants to read a response from the DB it must:
 * <ul>
 * <li> lock the control object
 * <li> set the <code>queryOutstanding</code> flag
 * <li> unlock the control object
 * <li> call the <code>Tds.processSubPacket()</code> method
 * <li> lock the control object
 * <li> if the packet was a <code>CANCEL_ACK</code> it will increment <code>cancelsProcessed</code>
 * <li> unlock the control object.
 * </ul>
 *
 * @version  $Id: CancelController.java,v 1.2 2003-12-16 19:08:48 alin_sinpalean Exp $
 * @author Craig Spannring
 */
public class CancelController
{
    public static final String cvsVersion = "$Id: CancelController.java,v 1.2 2003-12-16 19:08:48 alin_sinpalean Exp $";

    private boolean awaitingData     = false;
    private int     cancelsRequested = 0;
    private int     cancelsProcessed = 0;

    public synchronized void setQueryInProgressFlag()
    {
        awaitingData = true;
    }

    public synchronized void finishQuery(boolean wasCanceled, boolean moreResults)
    {
        // XXX Do we want to clear the query in progress flag if
        // there are still more results for multi result set query?
        // Whatever mechanism is used to handle outstanding query
        // requires knowing if there is any thread out there that could
        // still process the query acknowledgment.  Prematurely clearing
        // could cause data to be thrown out before the thread expecting
        // the data gets a chance to process it.  That could cause the
        // thread to read some other threads query.
        //
        // Is it good enough to just look at the MORERESULTS bit in the
        // TDS_END* packet and not clear the flag if we have more
        // results?
        if( !moreResults )
            awaitingData = false;

        if( wasCanceled )
            cancelsProcessed++;

        // XXX Should we see if there are any more cancels pending and
        // try to read the cancel acknowledgments?
    }

    public synchronized void doCancel(TdsComm comm) throws java.io.IOException, TdsException
    {
        // If we aren't waiting for anything from
        // the server then we have nothing to cancel
        if( awaitingData )
        {
            // SAfe Only send a CANCEL packet if no other unanswered CANCEL exists
            //      (there's no point in sending more than one CANCEL at a time).
            if( cancelsRequested == cancelsProcessed )
            {
                comm.startPacket(TdsComm.CANCEL);
                comm.sendPacket();
                cancelsRequested++;
            }

            // SAfe We won't do any waiting. If the Statement is not closed and
            //      the user doesn't process all data (to get to the TDS_DONE
            //      packet with the cancel flag set, the caller will never get out
            //      of here alive.
        }
    }

    public synchronized int outstandingCancels()
    {
        return cancelsRequested - cancelsProcessed;
    }
}
