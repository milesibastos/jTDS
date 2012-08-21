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

package net.sourceforge.jtds.util;

import java.security.DigestException;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;

/**
 * Provides MD5 via java.security based implementation.
 */
public class MD5Digest {
    private static final int DIGEST_LENGTH = 16;

    MessageDigest md;

    /**
     * Standard constructor
     * <p>
     * Note: the previous implementation (non-java.security based) also
     * included a copy constructor. This has been removed as it doesn't
     * make sense with the java.security based implementation.
     */
    public MD5Digest() {
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (GeneralSecurityException e) {
            throw new RuntimeException("Error initializing MD5Digest", e);
        }
        reset();
    }

    public String getAlgorithmName() {
        return "MD5";
    }

    public int getDigestSize() {
        return DIGEST_LENGTH;
    }

    public int doFinal(byte[] out, int outOff) {
        try {
            md.digest(out, outOff, DIGEST_LENGTH);
        } catch (DigestException e) {
            throw new RuntimeException("Error processing data for MD5Digest", e);
        }
        return DIGEST_LENGTH;
    }

    /**
     * Resets the digest for further use.
     */
    public void reset() {
        md.reset();
    }

    public void update(
            byte in)
    {
        md.update(in);
    }

    public void update(
            byte[]  in,
            int     inOff,
            int     len)
    {
        md.update(in, inOff, len);
    }

    public void finish()
    {
        // finish does the same as doFinal, but the digest is not returned.
        byte [] digTmp = new byte[DIGEST_LENGTH];
        doFinal(digTmp, 0);
    }
}
