package freetds;

import java.sql.*;
import com.internetcds.jdbc.tds.Tds;
import com.internetcds.jdbc.tds.TdsConnection;
import com.internetcds.jdbc.tds.TdsStatement;
import com.internetcds.jdbc.tds.TdsResultSet;

/**
 *  Extends the basic statement to provide scrolling.
 *
 *@author     chris
 *@created    17 March 2001
 */

public class CursorStatement extends TdsStatement {

    int type = ResultSet.TYPE_FORWARD_ONLY;
    int concurrency = ResultSet.CONCUR_READ_ONLY;


    public CursorStatement( TdsConnection con, Tds tds, int type, int concurrency )
             throws SQLException
    {
        super( con, tds );
        this.type = type;
        this.concurrency = concurrency;
    }


    public ResultSet executeQuery( String sql )
             throws SQLException
    {
        if ( type == ResultSet.TYPE_FORWARD_ONLY
                && concurrency == ResultSet.CONCUR_READ_ONLY ) {
            return super.executeQuery( sql );
        }
        else {
            return new CursorResultSet( this, sql );
        }
    }

}
