package com.internetcds.jdbc.tds;

import java.sql.SQLException;
import java.sql.SQLWarning;

/**
 * Helper class to reduce duplicated code.
 *
 * @author Stefan Bodewig <a href="mailto:stefan.bodewig@megabit.net">stefan.bodewig@megabit.net</a>
 * @author Alin Sinpalean
 * @version $Id: SQLWarningChain.java,v 1.4 2002-09-16 11:13:43 alin_sinpalean Exp $
 */
class SQLWarningChain
{
    public static final String cvsVersion = "$Id: SQLWarningChain.java,v 1.4 2002-09-16 11:13:43 alin_sinpalean Exp $";

    private SQLWarning warnings;
    private SQLException exceptions;

    SQLWarningChain ()
    {
        warnings = null;
    }

    /**
     * The first warning added with {@see #addWarning addWarning}.
     * Subsequent warnings will be chained to this SQLWarning.
     */
    synchronized SQLWarning getWarnings()
    {
        return warnings;
    }

    /**
     * The first exception added with {@see #addException addException}.
     * Subsequent exceptions will be chained to this SQLException.
     */
    synchronized SQLException getExceptions()
    {
        return exceptions;
    }

    /**
     * Checks if there's any exception in the chain and if so, throws the first
     * one. Subsequent exceptions will be chained to this
     * <code>SQLException</code>.
     * <p>
     * This method should (or should I say *has to*) be called after the
     * execution of any piece of code that could generate exceptions if no
     * other actions are to be taken on error. Otherwise,
     * <code>getExceptions()</code> should be called and, if an exception was
     * returned, take needed action and then throw the exception.
     */
    synchronized void checkForExceptions() throws SQLException
    {
        if( exceptions != null )
        {
            // SAfe Clear the exceptions first. Otherwise, the same exception
            //      will be thrown when this method is called, even if no other
            //      exception was thrown
            SQLException tmp = exceptions;
            exceptions = null;

            throw tmp;
        }
    }

    /**
     * After this call {@see #getWarnings getWarnings} returns null
     * until {@see #addWarning addWarning} has been called again.
     */
    synchronized void clearWarnings()
    {
        warnings = null;
        exceptions = null;
    }

    /**
     * Adds an SQLWarning to the warning chain.
     */
    synchronized void addWarning(SQLWarning warn)
    {
        if( warnings == null )
            warnings = warn;
        else
            warnings.setNextWarning(warn);
    }

    /**
     * Adds an SQLException to the exception chain.
     */
    synchronized void addException(SQLException ex)
    {
        if( exceptions == null )
            exceptions = ex;
        else
            exceptions.setNextException(ex);
    }

    /**
     * Adds the SQLWarning wrapped in the packet if it's not an ErrorResult.
     * Adds and returns the wrapped SQLException otherwise.
     */
    SQLException addOrReturn(PacketMsgResult pack)
    {
        if( pack instanceof PacketErrorResult )
        {
            SQLException ex = pack.getMsg().toSQLException();
            addException(ex);
            return ex;
        }
        else
        {
            addWarning(pack.getMsg().toSQLWarning());
            return null;
        }
    }
}