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
package net.sourceforge.jtds.jdbc;

import net.sourceforge.jtds.util.MD4Digest;
import net.sourceforge.jtds.util.DESEngine;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

/**
 * This class calculates the two "responses" to the nonce supplied by the server
 * as a part of NTLM authentication.
 *
 * @author Matt Brinkley
 * @version $Id: NtlmAuth.java,v 1.4 2004-08-24 17:45:02 bheineman Exp $
 */
public class NtlmAuth {
    public static final byte[] answerNtChallenge(String password, byte[] nonce)
        throws UnsupportedEncodingException {
        byte[] key = new byte[21];
        Arrays.fill(key, (byte)0);
        byte[] pwd = password.getBytes("UnicodeLittleUnmarked");

        // do the md4 hash of the unicode passphrase...
        MD4Digest md4 = new MD4Digest();
        md4.update(pwd, 0, pwd.length);
        md4.doFinal(key, 0);

        return encryptNonce(key, nonce);
    }

    public static final byte[] answerLmChallenge(String pwd, byte[] nonce)
        throws UnsupportedEncodingException {
        byte[] password = convertPassword(pwd);

        DESEngine d1 = new DESEngine(true, makeDESkey(password,  0));
        DESEngine d2 = new DESEngine(true, makeDESkey(password,  7));
        byte[] encrypted = new byte[21];
        Arrays.fill(encrypted, (byte)0);

        d1.processBlock(nonce, 0, encrypted, 0);
        d2.processBlock(nonce, 0, encrypted, 8);

        return encryptNonce(encrypted, nonce);
    }

    private static byte[] encryptNonce(byte[] key, byte[] nonce) {
        byte[] out = new byte[24];

        DESEngine d1 = new DESEngine(true, makeDESkey(key,  0));
        DESEngine d2 = new DESEngine(true, makeDESkey(key,  7));
        DESEngine d3 = new DESEngine(true, makeDESkey(key,  14));

        d1.processBlock(nonce, 0, out, 0);
        d2.processBlock(nonce, 0, out, 8);
        d3.processBlock(nonce, 0, out, 16);

        return out;
    }

    /**
     * Used by answerNtlmChallenge. We need the password converted to caps,
     * narrowed and padded/truncated to 14 chars...
     */
    private static final byte[] convertPassword(String password)
        throws UnsupportedEncodingException {
        byte[] pwd = password.toUpperCase().getBytes("UTF8");

        byte[] rtn = new byte[14];
        Arrays.fill(rtn, (byte) 0);
        System.arraycopy(
            pwd, 0,                                 // src
            rtn, 0,                                 // dst
            pwd.length > 14 ? 14 : pwd.length);     // length

        return rtn;
    }

    /**
     * Turns a 7-byte DES key into an 8-byte one by adding parity bits. All
     * implementations of DES seem to want an 8-byte key.
     */
    private static final byte[] makeDESkey(byte[] buf, int off) {
        byte[] ret = new byte[8];

        ret[0] = (byte) ((buf[off+0] >> 1) & 0xff);
        ret[1] = (byte) ((((buf[off+0] & 0x01) << 6) | (((buf[off+1] & 0xff)>>2) & 0xff)) & 0xff);
        ret[2] = (byte) ((((buf[off+1] & 0x03) << 5) | (((buf[off+2] & 0xff)>>3) & 0xff)) & 0xff);
        ret[3] = (byte) ((((buf[off+2] & 0x07) << 4) | (((buf[off+3] & 0xff)>>4) & 0xff)) & 0xff);
        ret[4] = (byte) ((((buf[off+3] & 0x0F) << 3) | (((buf[off+4] & 0xff)>>5) & 0xff)) & 0xff);
        ret[5] = (byte) ((((buf[off+4] & 0x1F) << 2) | (((buf[off+5] & 0xff)>>6) & 0xff)) & 0xff);
        ret[6] = (byte) ((((buf[off+5] & 0x3F) << 1) | (((buf[off+6] & 0xff)>>7) & 0xff)) & 0xff);
        ret[7] = (byte) (buf[off+6] & 0x7F);

        for (int i = 0; i < 8; i++) {
            ret[i] = (byte) (ret[i] << 1);
        }

        return ret;
    }
}

