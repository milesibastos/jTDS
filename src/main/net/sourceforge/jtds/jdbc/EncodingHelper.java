package net.sourceforge.jtds.jdbc;

import java.io.UnsupportedEncodingException;
import java.util.Hashtable;

/**
 * Helper class to handle server character set conversion.
 *
 * @author Stefan Bodewig <a href="mailto:stefan.bodewig@megabit.net">stefan.bodewig@megabit.net</a>
 * @version  $Id: EncodingHelper.java,v 1.6 2004-05-30 19:53:17 bheineman Exp $
 */
public class EncodingHelper
{
    public static final String cvsVersion = "$Id: EncodingHelper.java,v 1.6 2004-05-30 19:53:17 bheineman Exp $";

    /**
     * Array containig the bytes 0x00 - 0xFF.
     */
    private static byte[] convArray = new byte[256];
    /**
     * Hashtable holding instances for all known encodings.
     */
    private static Hashtable knownEncodings = new Hashtable();

    /**
     * The name of the encoding.
     */
    private String name;

    /**
     * Is this a DBCS charset (does it need more than one byte per character)?
     */
    private boolean wideChars;

    static {
        for (int i = 0; i < 256; i++) {
            convArray[i] = (byte) i;
        }

        // SAfe: SQL Server returns iso_1, but it's actually Cp1252
        try {
            knownEncodings.put("iso_1", new EncodingHelper("Cp1252"));
        } catch (UnsupportedEncodingException e) {
        }
    }


    /**
     * private so only the static accessor can be used.
     */
    private EncodingHelper(String name) throws UnsupportedEncodingException {
        this.name = name;

        // SAfe Hope this works in all cases.
        wideChars = new String(convArray, name).length() != convArray.length;
    }

    /**
     * Translate the String into a byte[] in the server's encoding.
     */
    public byte[] getBytes(String value) {
        try {
            return value.getBytes(name);
        } catch (UnsupportedEncodingException e) {
            return value.getBytes();
        }
    }

    /**
     * Translate the byte[] from the server's encoding to a Unicode String.
     */
    public String getString(byte[] value) {
        return getString(value, 0, value.length);
    }

    /**
     * Translate part of the byte[] from the server's encoding to a
     * Unicode String.
     *
     * The subarray starting at index off and extending to off+len-1
     * is translated.
     */
    public String getString(byte[] value, int off, int len) {
        try {
            return new String(value, off, len, name);
        } catch (UnsupportedEncodingException e) {
            return new String(value, off, len);
        }
    }

    /**
     * Is this a DBCS charset (does it need more than one byte per character)?
     */
    public boolean isDBCS() {
        return wideChars;
    }

    /**
     * Can the given String be converted to the server's charset?
     */
    public boolean canBeConverted(String value) {
        try {
            // SAfe Not very efficient, since a String is always created, but it's the only way I
            //      could think of. Maybe some Reader/Writer combination would have been better
            //      but...
            return new String(value.getBytes(name), name).equals(value);
        } catch (UnsupportedEncodingException e) {
            // SAfe No way.
            return false;
        }
    }

    /**
     * Return the helper object for the given encoding.
     */
    public static EncodingHelper getHelper(String encodingName) {
        EncodingHelper res = (EncodingHelper) knownEncodings.get(encodingName);

        if (res == null) {
            try {
                res = new EncodingHelper(encodingName);
            } catch (UnsupportedEncodingException e) {
            }
        }

        // SAfe Try prepending Cp or MS to the character set. This should
        //      cover all the character sets known by SQL Server.
        if (res == null) {
            try {
                if (encodingName.toLowerCase().startsWith("cp") && encodingName.length() > 2) {
                    res = new EncodingHelper("Cp" + encodingName.substring(2));
                } else {
                    res = new EncodingHelper("Cp" + encodingName);
                }
            } catch (UnsupportedEncodingException e) {
            }
        }

        if (res == null) {
            try {
                if (encodingName.toLowerCase().startsWith("ms") && encodingName.length() > 2) {
                    res = new EncodingHelper("MS" + encodingName.substring(2));
                } else {
                    res = new EncodingHelper("MS" + encodingName);
                }
            } catch (UnsupportedEncodingException e) {
            }
        }

        if (res != null) {
            knownEncodings.put(encodingName, res);
        }

        return res;
    }

    public String getName() {
        return name;
    }
}
