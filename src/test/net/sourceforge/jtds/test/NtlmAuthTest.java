package net.sourceforge.jtds.test;

import net.sourceforge.jtds.jdbc.NtlmAuth;

import java.util.Arrays;

/**
 * Unit test for NTLM challenge/response calculation
 * Written by mdb.
 */
public class NtlmAuthTest extends TestBase {
    public NtlmAuthTest(String name) {
        super(name);
    }

    /**
     * Tests the NT challenge/response against a known-good value. This was captured
     * from a successful login to one of my (mdb's) test computers.
     */
    public void testChallengeResponse() throws Exception {
        final String password  = "bark";
        byte[] challenge = new byte[] {
            (byte)0xd9, (byte)0x90, (byte)0xed, (byte)0xaf,
            (byte)0x94, (byte)0x17, (byte)0x36, (byte)0xaf};

        byte[] ntResp = NtlmAuth.answerNtChallenge(password, challenge);
        byte[] lmResp = NtlmAuth.answerLmChallenge(password, challenge);

        byte[] ntExpected = new byte[] {
            (byte)0x8e, (byte)0x75, (byte)0x8e, (byte)0x79, (byte)0xe2, (byte)0xa1, (byte)0x45, (byte)0x75,
            (byte)0xb4, (byte)0x21, (byte)0x55, (byte)0x9b, (byte)0x12, (byte)0x29, (byte)0xd3, (byte)0x5a,
            (byte)0x23, (byte)0x8b, (byte)0x7d, (byte)0xa8, (byte)0x3a, (byte)0x50, (byte)0xc6, (byte)0xa7};


        byte[] lmExpected = new byte[] {
            (byte)0xe6, (byte)0x19, (byte)0x92, (byte)0xcd, (byte)0x84, (byte)0xf7, (byte)0xb8, (byte)0x49,
            (byte)0xaf, (byte)0x75, (byte)0xf9, (byte)0x37, (byte)0xd4, (byte)0x0b, (byte)0xe6, (byte)0x81,
            (byte)0xc4, (byte)0x0c, (byte)0x7c, (byte)0x3f, (byte)0x3e, (byte)0xc6, (byte)0x8b, (byte)0x7f};


        assertTrue(Arrays.equals(ntResp, ntExpected));
        assertTrue(Arrays.equals(lmResp, lmExpected));
    }
}

