package net.sourceforge.jtds.jdbc;

import java.sql.SQLException;

/**
 * Defines an interface for classes interested in processing output parameters
 * and return values.
 *
 * @created    January 16, 2004
 * @version    1.0
 */
public interface OutputParamHandler
{
    /**
     * Handle a return status.
     *
     * @param packet a RetStat packet
     * @return       <code>true</code> if the return status was handled
     */
    boolean handleRetStat(PacketRetStatResult packet);

    /**
     * Handle an output parameter.
     *
     * @param packet        an OutputParam packet
     * @return              <code>true</code> if the return status was handled
     * @throws SQLException if there is a handling exception
     */
    boolean handleParamResult(PacketOutputParamResult packet)
        throws SQLException;
}
