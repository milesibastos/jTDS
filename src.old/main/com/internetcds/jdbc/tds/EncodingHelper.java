package com.internetcds.jdbc.tds;

import java.io.UnsupportedEncodingException;
import java.util.Hashtable;

/**
 * Helper class to handle server character set conversion.
 *
 * @author Stefan Bodewig <a href="mailto:stefan.bodewig@megabit.net">stefan.bodewig@megabit.net</a>
 *
 * @version  $Id: EncodingHelper.java,v 1.5 2002-09-16 11:13:43 alin_sinpalean Exp $
 */
public class EncodingHelper {
    public static final String cvsVersion = "$Id: EncodingHelper.java,v 1.5 2002-09-16 11:13:43 alin_sinpalean Exp $";

    /**
     * The name of the encoding.
     */
    private String name;
    /**
     * Is this a DBCS charset (does it need more than one byte per character)?
     */
    private boolean wideChars;
    /**
     * A String containing all characters of the charset (if this is not
     * a DBCS charset).
     */
    private String converted;

    /**
     * private so only the static accessor can be used.
     */
    private EncodingHelper(String name) throws UnsupportedEncodingException
    {
        this.name = name;
        converted = new String(convArray, name);
        wideChars = converted.length()!=convArray.length;

        if( wideChars )
            converted = null;
    }

    /**
     * Translate the String into a byte[] in the server's encoding.
     */
    public byte[] getBytes(String value) {
        try {
            return value.getBytes(name);
        } catch (UnsupportedEncodingException uee) {
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
        } catch (UnsupportedEncodingException uee) {
            return new String(value, off, len);
        }
    }

    /**
     * Is this a DBCS charset (does it need more than one byte per character)?
     */
    public boolean isDBCS() {return wideChars;}

    /**
     * Can the given String be converted to the server's charset?
     *
     * <p>Does not work for DBCS charsets.
     */
    public boolean canBeConverted(String value) {
        if (isDBCS()) {
            throw new IllegalStateException(name+" is a DBCS charset");
        }

        int len = value.length();
        for (int i=0; i<len; i++) {
            if (converted.indexOf(value.charAt(i)) == -1) {
                return false;
            }
        }
        return true;
    }

    /**
     * Return the helper object for the given encoding.
     */
    public static EncodingHelper getHelper(String encodingName)
    {
        if( !initialized )
        {
            synchronized (com.internetcds.jdbc.tds.EncodingHelper.class)
            {
                if( !initialized )
                    initialize();
            }
        }

        EncodingHelper res = (EncodingHelper)knownEncodings.get(encodingName);
        if( res == null )
            try
            {
                res = new EncodingHelper(encodingName);
                knownEncodings.put(encodingName, res);
            }
            catch( UnsupportedEncodingException ex )
            {
                res = (EncodingHelper)knownEncodings.get("iso_1");
            }

        return res;
    }

    /**
     * Array containig the bytes 0x00 - 0xFF.
     */
    private static byte[] convArray;
    /**
     * Hashtable holding instances for all known encodings.
     */
    private static Hashtable knownEncodings;
    /**
     * Simple boolean to ensure we initialize once and only once.
     */
    private static boolean initialized;

    /**
     * Initialize the static variables.
     *
     * <p>Will be called from the static block below, but some VMs
     * (notably Microsoft's) won't run this.
     */
    private synchronized static void initialize()
    {
        convArray = new byte[256];
        for (int i=0; i<256; i++)
            convArray[i] = (byte)i;

        knownEncodings = new Hashtable();
        // SAfe: SQL Server returns iso_1, but it's actually Cp1252
        try
        {
            knownEncodings.put("iso_1", new EncodingHelper("Cp1252"));
        }
        catch( UnsupportedEncodingException ex )
        {}
        initialized = true;
    }

    static
    {
        initialize();
    }

    public String getName()
    {
        return name;
    }
}
