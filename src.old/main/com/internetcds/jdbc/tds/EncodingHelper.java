package com.internetcds.jdbc.tds;

import java.io.UnsupportedEncodingException;
import java.util.Hashtable;

/**
 * Helper class to handle server character set conversion.
 *
 * @author Stefan Bodewig <a href="mailto:stefan.bodewig@megabit.net">stefan.bodewig@megabit.net</a>
 *
 * @version  $Id: EncodingHelper.java,v 1.2 2001-08-31 12:47:20 curthagenlocher Exp $
 */
public class EncodingHelper {
    public static final String cvsVersion = "$Id: EncodingHelper.java,v 1.2 2001-08-31 12:47:20 curthagenlocher Exp $";

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
    private EncodingHelper(String name, boolean wideChars) {
        this.name = name;
        this.wideChars = wideChars;
        if (!wideChars) {
            converted = getString(convArray);
        }
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
    public static EncodingHelper getHelper(String encodingName) {
        if (!initialized) {
            synchronized (com.internetcds.jdbc.tds.EncodingHelper.class) {
                if (!initialized) {
                    initialize();
                }
            }
        }
        return (EncodingHelper)knownEncodings.get(encodingName);
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
    private synchronized static void initialize() {
        convArray = new byte[256];
        for (int i=0; i<256; i++) {
            convArray[i] = (byte)i;
        }

        knownEncodings = new Hashtable();
        EncodingHelper e = new EncodingHelper("ISO8859_1", false);
        knownEncodings.put("iso_1", e);
        knownEncodings.put("cp1252", e);

        try {
            // simple test for the presence of i18n.jar
            "a".getBytes("Cp437");

            knownEncodings.put("cp437", new EncodingHelper("Cp437", false));
            knownEncodings.put("cp850", new EncodingHelper("Cp850", false));
            knownEncodings.put("cp1250", new EncodingHelper("Cp1250", false));
            knownEncodings.put("cp1251", new EncodingHelper("Cp1251", false));
            knownEncodings.put("cp1253", new EncodingHelper("Cp1253", false));
            knownEncodings.put("cp1254", new EncodingHelper("Cp1254", false));
            knownEncodings.put("cp1255", new EncodingHelper("Cp1255", false));
            knownEncodings.put("cp1256", new EncodingHelper("Cp1256", false));
            knownEncodings.put("cp1257", new EncodingHelper("Cp1257", false));
            
            /*
             * XXX are the CpXXX different from MSXXX? Used MS to be save.
             */
            //thai
            knownEncodings.put("cp874", new EncodingHelper("MS874", true)); 
            //japanese
            knownEncodings.put("cp932", new EncodingHelper("MS932", true)); 
            //simplified chinese
            knownEncodings.put("cp932", new EncodingHelper("MS932", true)); 
            //korean
            knownEncodings.put("cp949", new EncodingHelper("MS949", true));
            //traditional chinese        
            knownEncodings.put("cp950", new EncodingHelper("MS950", true)); 
        } catch (UnsupportedEncodingException uee) {
            // i18n.jar not present, only ISO-8859-1 is available
        }

        initialized = true;
    }

    static {
        initialize();
    }
}
