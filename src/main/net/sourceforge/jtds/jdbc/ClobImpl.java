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

import java.sql.Clob;
import java.sql.SQLException;

public class ClobImpl implements Clob {
    public static final String cvsVersion = "$Id: ClobImpl.java,v 1.1 2004-01-22 23:49:15 alin_sinpalean Exp $";

    private String _clob;

    ClobImpl(String clob) {
        if (clob == null) {
            throw new IllegalArgumentException("clob cannot be null.");
        }

        _clob = clob;
    }

    public long length() throws SQLException {
        return _clob.length();
    }

    public java.io.InputStream getAsciiStream() throws SQLException {
        try {
            return new java.io.ByteArrayInputStream(_clob.getBytes("ASCII"));
        } catch (java.io.UnsupportedEncodingException e) {
            // This should never happen...
            throw new SQLException("Unexpected encoding exception: "
                                   + e.getMessage());
        }
    }

    public java.io.Reader getCharacterStream() throws SQLException {
        return new java.io.StringReader(_clob);
    }

    public String getSubString(long pos, int length) throws SQLException {
        if (pos > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("pos must be <= " + Integer.MAX_VALUE);
        }

        return _clob.substring((int) pos, length);
    }

    public long position(String searchStr, long start) throws SQLException {
        if (searchStr == null) {
            throw new IllegalArgumentException("searchStr cannot be null.");
        } else if (start < 0) {
            throw new IllegalArgumentException("start must be >= 0.");
        } else if (start > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("start must be <= " + Integer.MAX_VALUE);
        }

        return _clob.indexOf(searchStr, (int) start);
    }

    public long position(Clob searchStr, long start) throws SQLException {
        if (searchStr == null) {
            throw new IllegalArgumentException("searchStr cannot be null.");
        } else if (start > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("start must be <= " + Integer.MAX_VALUE);
        }

        long length = searchStr.length();

        if (length > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("searchStr.length() must be <= "
                                               + Integer.MAX_VALUE);
        }

        return _clob.indexOf(searchStr.getSubString(0, (int) length),
                             (int) start);
    }

    public int setString(long pos, String str, int offset, int len) throws SQLException {
        NotImplemented();
        return 0;
    }

    public java.io.Writer setCharacterStream(long pos) throws SQLException {
        NotImplemented();
        return null;
    }

    public int setString(long pos, String str) throws SQLException {
        NotImplemented();
        return 0;
    }

    public java.io.OutputStream setAsciiStream(long pos) throws SQLException {
        NotImplemented();
        return null;
    }

    public void truncate(long len) throws SQLException {
        NotImplemented();
    }

    public String toString() {
        return _clob;
    }

    private void NotImplemented() throws java.sql.SQLException {
        throw new SQLException("Not Implemented");
    }
}
