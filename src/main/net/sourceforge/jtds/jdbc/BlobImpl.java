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

import java.sql.Blob;
import java.sql.SQLException;

public class BlobImpl implements Blob {
    public static final String cvsVersion = "$Id: BlobImpl.java,v 1.1 2004-01-22 23:49:10 alin_sinpalean Exp $";

    private byte[] _blob;

    BlobImpl(byte[] blob) {
        if (blob == null) {
            throw new IllegalArgumentException("blob cannot be null.");
        }

        _blob = blob;
    }

    public long position(byte[] pattern, long start) throws SQLException {
        int length = _blob.length - pattern.length;

        if (pattern == null) {
            throw new IllegalArgumentException("pattern cannot be null.");
        } else if (start < 0) {
            throw new IllegalArgumentException("start must be >= 0.");
        } else if (start > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("start must be <= " + Integer.MAX_VALUE);
        }

        for (int i = (int) start; i < length; i++) {
            boolean found = true;

            for (int x = 0; x < pattern.length; x++) {
                if (pattern[x] != _blob[i + x]) {
                    found = false;
                    break;
                }
            }

            if (found) {
                return i;
            }
        }

        return -1;
    }

    public long position(Blob pattern, long start) throws SQLException {
        if (pattern == null) {
            throw new IllegalArgumentException("pattern cannot be null.");
        }

        long length = pattern.length();

        if (length > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("pattern.length() must be <= "
                                               + Integer.MAX_VALUE);
        }

        return position(pattern.getBytes(0, (int) length), start);
    }

    public long length() throws SQLException {
        return _blob.length;
    }

    public byte[] getBytes(long pos, int length) throws SQLException {
        if (pos < 0) {
            throw new IllegalArgumentException("pos must be >= 0.");
        } else if (pos >= _blob.length - 1) {
            throw new IllegalArgumentException("pos must be < length of blob.");
        } else if (pos > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("pos must be <= " + Integer.MAX_VALUE);
        } else if (length < 0) {
            throw new IllegalArgumentException("length must be >= 0.");
        } else if (pos + length > _blob.length) {
            throw new IllegalArgumentException("more bytes requested than exist in blob.");
        }

        byte[] value = new byte[length];

        System.arraycopy(_blob, (int) pos, value, 0, length);

        return value;
    }

    public int setBytes(long pos, byte[] bytes) throws SQLException {
        NotImplemented();
        return 0;
    }

    public java.io.InputStream getBinaryStream() throws SQLException {
        return new java.io.ByteArrayInputStream(_blob);
    }

    public java.io.OutputStream setBinaryStream(long pos) throws SQLException {
        NotImplemented();
        return null;
    }

    public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
        NotImplemented();
        return 0;
    }

    public void truncate(long len) throws SQLException {
        NotImplemented();
    }

    public String toString() {
        return _blob.toString();
    }

    private void NotImplemented() throws java.sql.SQLException {
        throw new SQLException("Not Implemented");
    }
}


