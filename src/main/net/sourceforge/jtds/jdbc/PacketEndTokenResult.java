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

public class PacketEndTokenResult extends PacketResult {
    public static final String cvsVersion = "$Id: PacketEndTokenResult.java,v 1.3 2004-04-04 22:12:03 alin_sinpalean Exp $";

    /**
     * Packet status flags.
     */
    private int status;
    /**
     * Affected rows (0 for DDL statements, -1 for SELECT or no valid row count,
     * i.e. <code>flags & 0x10 == 0</code>).
     */
    private int rowCount;

    public PacketEndTokenResult(byte type, int status, int rowCount) {
        super(type);
        this.status = status;
        this.rowCount = rowCount;
    }

    public int getRowCount() {
        return rowCount;
    }

    public boolean moreResults() {
        return (status & 0x01) != 0;
    }

    public boolean wasError() {
        return (status & 0x02) != 0;
    }

    public boolean isRowCountValid() {
        return (status & 0x10) != 0;
    }

    public boolean wasCanceled() {
        return (status & 0x20) != 0;
    }

    public String toString() {
        return "token type- " + Integer.toHexString(getPacketType() & 0xff)
                + ", rowCount- " + getRowCount()
                + ", moreResults- " + moreResults()
                + ", wasCanceled- " + wasCanceled()
                + ", wasError- " + wasError();
    }
}

