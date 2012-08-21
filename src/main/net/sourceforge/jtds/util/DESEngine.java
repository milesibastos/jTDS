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

import java.security.GeneralSecurityException;
import java.security.spec.KeySpec;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;

/**
 * a class that provides a basic DES engine.
 * Modified by Matt Brinkley (mdb) ... mainly just removed depends on external classes.
 *
 * @author Matt Brinkley
 * @version $Id: DESEngine.java,v 1.3.6.1 2009-08-04 10:33:54 ickzon Exp $
 */
public class DESEngine
{
    protected static final int  BLOCK_SIZE = 8;
    private Cipher cf;

    /**
     * standard constructor.
     */
    public DESEngine() {
        // nothing
    }

    /**
     * mdb: convenient constructor
     */
    public DESEngine( boolean encrypting, byte[] key ) {
        init(encrypting, key);
    }

    /**
     * initialise a DES cipher.
     *
     * @param encrypting whether or not we are for encryption.
     * @param key the parameters required to set up the cipher.
     * @exception IllegalArgumentException if the params argument is
     * inappropriate.
     */
    public void init(boolean encrypting, byte[] key) {
        try {
            KeySpec ks;
            SecretKeyFactory kf;
            SecretKey ky;

            ks = new DESKeySpec(key);
            kf = SecretKeyFactory.getInstance("DES");
            ky = kf.generateSecret(ks);
            cf = Cipher.getInstance("DES/ECB/NoPadding");
            cf.init(
                    (encrypting ? Cipher.ENCRYPT_MODE : Cipher.DECRYPT_MODE), 
                    ky);
        } catch (GeneralSecurityException e) {
            throw new RuntimeException("Error initializing DESEngine", e);
        }
    }

    public String getAlgorithmName() {
        return "DES";
    }

    public int getBlockSize() {
        return BLOCK_SIZE;
    }

    public int processBlock(byte[] in, int inOff, byte[] out, int outOff) {
        // rsc: keep the error behaviour as it was with the previous non-JDK implementation:
        if (cf==null)
        {
            throw new IllegalStateException("DES engine not initialised");
        }

        if ((inOff + BLOCK_SIZE) > in.length)
        {
            //mdb: used to be DataLengthException
            throw new IllegalArgumentException("input buffer too short");
        }

        if ((outOff + BLOCK_SIZE) > out.length)
        {
            //mdb: used to be DataLengthException
            throw new IllegalArgumentException("output buffer too short");
        }

        try {
            int len = cf.doFinal(in, inOff, 8, out, outOff);
            return len;
        } catch (GeneralSecurityException e) {
            throw new RuntimeException("Error processing data block in DESEngine", e);
        }
    }

    public void reset() {
        // nothing
    }

}