package com.internetcds.jdbc.tds;

import java.sql.SQLException;
import java.sql.SQLWarning;

/**
 * Helper class to redruce duplicated code.
 *
 * @author Stefan Bodewig <a href="mailto:stefan.bodewig@megabit.net">stefan.bodewig@megabit.net</a>
 *
 * @version  $Id: SQLWarningChain.java,v 1.1.1.1 2001-08-10 01:44:31 skizz Exp $
 */
class SQLWarningChain  {
   public static final String cvsVersion = "$Id: SQLWarningChain.java,v 1.1.1.1 2001-08-10 01:44:31 skizz Exp $";
   
   private SQLWarning warnings;
   
   SQLWarningChain () 
   {
      warnings = null;
   }
   
   /**
    * The first warning added with {@see #addWarning addWarning}.
    * Subsequent warnings will be chained to this SQLWarning.  
    */
    synchronized SQLWarning getWarnings() {
        return warnings;
    }

   /**
    * After this call {@see #getWarnings getWarnings} returns null
    * until {@see #addWarning addWarning} has been called again.  
    */
    synchronized void clearWarnings() {
        warnings = null;
    }

    /**
     * Adds a SQLWarning to the warning chain.
     */
    synchronized void addWarning(SQLWarning warn) {
        if (warnings == null) {
            warnings = warn;
        } else {
            warnings.setNextWarning(warn);
        }
    }

    /**
     * Adds the SQLWarning wrapped in the packet if it's not an ErrorResult.
     * Returns the wrapped SQLException otherwise.
     */
    SQLException addOrReturn(PacketMsgResult pack) {
        if (pack instanceof PacketErrorResult) {
            return pack.getMsg().toSQLException();
        } else {
            addWarning(pack.getMsg().toSQLWarning());
            return null;
        }
    }
}

