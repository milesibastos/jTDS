/*
 * File: SQLCacheKey.java
 *
 * Author: Alin Sinpalean
 *
 * Created: Apr 21, 2005
 */
package net.sourceforge.jtds.jdbc.cache;

/**
 * Cache key for an SQL query, consisting of the query and server type, major
 * and minor version.
 *
 * @author Brett Wooldridge
 * @author Alin Sinpalean
 * @version $Id: SQLCacheKey.java,v 1.1 2005-04-25 11:46:55 alin_sinpalean Exp $
 */
public class SQLCacheKey {
    private final String sql;
    private final int serverType;
    private final int majorVersion;
    private final int minorVersion;
    private final int hashCode;

    public SQLCacheKey(String sql, int serverType, int majorVersion,
                       int minorVersion) {
        this.sql = sql;
        this.serverType   = serverType;
        this.majorVersion = majorVersion;
        this.minorVersion = minorVersion;

        this.hashCode = sql.hashCode()
                ^ (serverType << 24 | majorVersion << 16 | minorVersion);
    }

    public int hashCode() {
        return hashCode;
    }

    public boolean equals(Object object) {
        try {
            SQLCacheKey key = (SQLCacheKey) object;

            return this.hashCode == key.hashCode
                    && this.majorVersion == key.majorVersion
                    && this.minorVersion == key.minorVersion
                    && this.serverType == key.serverType
                    && this.sql.equals(key.sql);
        } catch (ClassCastException e) {
            return false;
        }
    }
}
