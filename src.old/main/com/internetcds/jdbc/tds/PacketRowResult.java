//
// Copyright 1998 CDS Networks, Inc., Medford Oregon
//
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
// 1. Redistributions of source code must retain the above copyright
//    notice, this list of conditions and the following disclaimer.
// 2. Redistributions in binary form must reproduce the above copyright
//    notice, this list of conditions and the following disclaimer in the
//    documentation and/or other materials provided with the distribution.
// 3. All advertising materials mentioning features or use of this software
//    must display the following acknowledgement:
//      This product includes software developed by CDS Networks, Inc.
// 4. The name of CDS Networks, Inc.  may not be used to endorse or promote
//    products derived from this software without specific prior
//    written permission.
//
// THIS SOFTWARE IS PROVIDED BY CDS NETWORKS, INC. ``AS IS'' AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED.  IN NO EVENT SHALL CDS NETWORKS, INC. BE LIABLE
// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
// OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
// HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
// LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
// OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
// SUCH DAMAGE.
//



package com.internetcds.jdbc.tds;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Vector;

/**
 *  encapsulate the information from one row of a result set.
 *
 *@author     Craig Spannring
 *@created    17 March 2001
 */
public class PacketRowResult extends PacketResult {

    Vector row = null;
    Context context;

    boolean wasNull = false;
    /**
     *  /** @todo Description of the Field
     */
    public final static String cvsVersion = "$Id: PacketRowResult.java,v 1.10 2002-09-16 11:13:43 alin_sinpalean Exp $";


    public PacketRowResult( Context context )
    {
        super( TdsDefinitions.TDS_ROW );
        this.context = context;
        row = new Vector( realColumnCount() );
        row.setSize( realColumnCount() );
    }

    public Context getContext()
    {
        return context;
    }

    /**
     *  Sets the component at the specified index of this vector to be the
     *  specified object. The previous component at that position is discarded.
     *
     *  <UL>Note- <\UL>Unlike the vector class this class starts the index with
     *    1, not 0.
     *
     *@param  obj               The object to store
     *@param  index             Index to store the element at. First element is
     *      at index 1
     */
    public void setElementAt( int index, Object obj )
        throws SQLException
    {
        if ( index < 1 || index > realColumnCount() ) {
            throw new SQLException( "Bad index " + index );
        }

        row.setElementAt( obj, index - 1 );
    }


    private int realColumnCount()
    {
        return context.getColumnInfo().realColumnCount();
    }

    public int getColumnType( int index )
        throws SQLException
    {
        return context.getColumnInfo().getJdbcType( index );
    }



    /**
     *  get an element at the specified index <p>
     *
     *
     *  <UL>Note- <\UL>Unlike the vector class this starts the index with 1, not
     *    0.
     *
     *@param  index             Index to get the element from. First element is
     *      at index 1
     *@return                   The ElementAt value
     *@exception  TdsException  @todo Description of Exception
     */
    public Object getElementAt( int index )
        throws TdsException
    {
        if ( index < 1 || index > realColumnCount() ) {
            throw new TdsException( "Bad index " + index );
        }

        return row.elementAt( index - 1 );
    }


    public Object getObject( int columnIndex )
             throws SQLException
    {
        // This method is implicitly coupled to the getRow() method in the
        // Tds class.  Every type that getRow() could return must
        // be handled in this method.
        //
        // The object type returned by getRow() must correspond with the
        // jdbc SQL type in the switch statement below.
        //
        // Note-  The JDBC spec (version 1.20) does not define the type
        // of the Object returned for LONGVARCHAR data.

        // XXX-  Needs modifications for JDBC 2.0

        try
        {
            Object tmp = getElementAt( columnIndex );
            wasNull = false;

            if( tmp == null )
            {
                wasNull = true;
                return null;
            }
            else
            {
                switch ( getColumnType( columnIndex ) )
                {
                    case java.sql.Types.CHAR:
                    case java.sql.Types.VARCHAR:
                        if( tmp instanceof String )
                            return tmp;
                        else
                            throw new SQLException( "Was expecting CHAR data.  Got"
                                     + tmp.getClass().getName() );

                    case java.sql.Types.TINYINT:
                    case java.sql.Types.SMALLINT:
                    case java.sql.Types.INTEGER:
                        if( tmp instanceof Integer )
                            return tmp;
                        if(! (tmp instanceof Number) )
                            throw new SQLException("Can't convert "+tmp.getClass().getName()+
                                " to Integer.");
                        return new Integer(((Number)tmp).intValue());

                    case java.sql.Types.BIGINT:
                        if( tmp instanceof Long )
                            return tmp;
                        if(! (tmp instanceof Number))
                            throw new SQLException("Internal error");

                      return new Long(((Number)tmp).longValue());

                    case java.sql.Types.REAL:
                        if (! (tmp instanceof Float))
                            throw new SQLException("Internal error");
                        return tmp;

                    case java.sql.Types.FLOAT:
                    case java.sql.Types.DOUBLE:
                        if( tmp instanceof Double )
                            return tmp;
                        else if ( tmp instanceof Number )
                            return new Double( ( ( Number ) tmp ).doubleValue() );
                        else
                            throw new SQLException( "Was expecting Numeric data.  Got"
                                     + tmp.getClass().getName() );

                    case java.sql.Types.DATE:
                        // XXX How do the time types hold up with timezones?
                        if( !( tmp instanceof Timestamp ) )
                            throw new SQLException( "Internal error" );

//                        java.util.Calendar  cal = new java.util.GregorianCalendar();
//                        cal.setTime(getTimestamp(columnIndex));
//                        result = cal.getTime();
                        return new Date( ( ( Timestamp ) tmp ).getTime() );

                    case java.sql.Types.TIME:
                        if( !( tmp instanceof Timestamp ) )
                            throw new SQLException( "Internal error" );
                        return new Time( ( ( Timestamp ) tmp ).getTime() );

                    case java.sql.Types.TIMESTAMP:
                        if( !( tmp instanceof Timestamp ) )
                            throw new SQLException( "Internal error" );
                        return tmp;

                    case java.sql.Types.BINARY:
                    case java.sql.Types.VARBINARY:
                        return getBytes( columnIndex );

                    case java.sql.Types.DECIMAL:
                    case java.sql.Types.NUMERIC:
                        if( tmp instanceof BigDecimal )
                            return tmp;
                        else if( tmp instanceof Number )
                            return new BigDecimal( ( ( Number ) tmp ).doubleValue() );
                        else
                            throw new SQLException( "Was expecting NUMERIC data.  Got"
                                     + tmp.getClass().getName() );

                    case java.sql.Types.LONGVARCHAR:
                        if( tmp instanceof TdsAsciiInputStream )
                            return tmp.toString();
                        else if( tmp instanceof java.lang.String )
                            return tmp;
                        else
                            throw new SQLException( "Was expecting LONGVARCHAR data. "
                                     + "Got "
                                     + tmp.getClass().getName() );

                    case java.sql.Types.LONGVARBINARY:
                        throw new SQLException( "Not implemented" );

                    case java.sql.Types.NULL:
                        throw new SQLException( "Not implemented" );

                    case java.sql.Types.OTHER:
                        throw new SQLException( "Not implemented" );

                    case java.sql.Types.BIT:
                        if( tmp instanceof Boolean )
                            return tmp;
                        else
                            throw new SQLException( "Was expecting BIT data. "
                                     + "Got"
                                     + tmp.getClass().getName() );

                    default:
                        throw new SQLException( "Unknown datatype "
                                     + getColumnType( columnIndex ));
                }
            }
        }
        catch ( com.internetcds.jdbc.tds.TdsException e )
        {
            e.printStackTrace();
            throw new SQLException( e.getMessage() );
        }
    }


    public byte[] getBytes( int columnIndex ) throws SQLException
    {
        byte result[];

        try {
            Object tmp = getElementAt( columnIndex );
            wasNull = false;
            if ( tmp == null ) {
                wasNull = true;
                result = null;
            }
            else if ( tmp instanceof byte[] ) {
                result = ( byte[] ) tmp;
            }
            else if ( tmp instanceof String ) {
                result = context.getEncoder().getBytes( ( String ) tmp );
            }
            else {
                throw new SQLException( "Can't convert column " + columnIndex
                         + " from "
                         + tmp.getClass().getName()
                         + " to byte[]" );
            }
        }
        catch ( TdsException e ) {
            e.printStackTrace();
            throw new SQLException( e.getMessage() );
        }
        return result;
    }


    public double getDouble( int columnIndex ) throws SQLException
    {
        double result;
        Object obj = getObject( columnIndex );

        if ( obj == null ) {
            result = 0.0;
        }
        else {
            try {
                switch ( getColumnType( columnIndex ) ) {
                    case java.sql.Types.TINYINT:
                    case java.sql.Types.SMALLINT:
                    case java.sql.Types.INTEGER:
                    case java.sql.Types.BIGINT:
                    case java.sql.Types.REAL:
                    case java.sql.Types.FLOAT:
                    case java.sql.Types.DOUBLE:
                    case java.sql.Types.DECIMAL:
                    case java.sql.Types.NUMERIC:
                    {
                        result = ((Number)obj).doubleValue();
                        break;
                    }
                    case java.sql.Types.CHAR:
                    case java.sql.Types.VARCHAR:
                    case java.sql.Types.LONGVARCHAR:
                    {
                        try {
                            Double d = new Double( ( String ) obj );
                            result = d.doubleValue();
                        }
                        catch ( NumberFormatException e ) {
                            throw new SQLException( e.getMessage() );
                        }
                        break;
                    }
                    case java.sql.Types.BIT:
                    {
                        // XXX according to JDBC spec we need to handle these
                        // for now just fall through
                    }
                    default:
                    {
                        throw new SQLException( "Internal error. "
                                 + "Don't know how to convert from "
                                 + "java.sql.Types." +
                                TdsUtil.javaSqlTypeToString( getColumnType( columnIndex ) )
                                 + " to an Dboule" );
                    }
                }
            }
            catch ( ClassCastException e ) {
                throw new SQLException( "Couldn't convert column " + columnIndex
                         + " to an long.  "
                         + e.getMessage() );
            }
        }
        return result;
    }


    public long getLong( int columnIndex ) throws SQLException
    {
        long result = 0;
        Object obj = getObject( columnIndex );

        if ( obj == null ) {
            result = 0;
        }
        else {
            try {
                switch ( getColumnType( columnIndex ) ) {
                    case java.sql.Types.TINYINT:
                    case java.sql.Types.SMALLINT:
                    case java.sql.Types.INTEGER:
                    case java.sql.Types.BIGINT:
                    case java.sql.Types.REAL:
                    case java.sql.Types.FLOAT:
                    case java.sql.Types.DOUBLE:
                    {
                        result = ((Number)obj).longValue();
                        break;
                    }
                    case java.sql.Types.CHAR:
                    case java.sql.Types.VARCHAR:
                    case java.sql.Types.LONGVARCHAR:
                    {
                        try {
                            Long i = new Long( ( String ) obj );
                            result = i.longValue();
                        }
                        catch ( NumberFormatException e ) {
                            throw new SQLException( e.getMessage() );
                        }
                        break;
                    }
                    case java.sql.Types.NUMERIC:
                    {
                        result = ( ( Number ) obj ).longValue();
                        break;
                    }
                    case java.sql.Types.DECIMAL:
                    {
                        result = ( ( Number ) obj ).longValue();
                        break;
                    }
                    case java.sql.Types.BIT:
                    {
                        result = ((Boolean)obj).booleanValue() ? 1 : 0;
                        break;
                    }
                    default:
                    {
                        throw new SQLException( "Internal error. "
                                 + "Don't know how to convert from "
                                 + "java.sql.Types " +
                                TdsUtil.javaSqlTypeToString( getColumnType( columnIndex ) )
                                 + " to an long" );
                    }
                }
            }
            catch ( ClassCastException e ) {
                throw new SQLException( "Couldn't convert column " + columnIndex
                         + " to an long.  "
                         + e.getMessage() );
            }
        }
        return result;
    }


    public java.sql.Timestamp getTimestamp( int columnIndex ) throws SQLException
    {
        Timestamp result;

        try {
            Object tmp = getElementAt( columnIndex );

            wasNull = false;
            if ( tmp == null ) {
                wasNull = true;
                result = null;
            }
            else if ( tmp instanceof Timestamp ) {
                result = ( Timestamp ) tmp;
            }
            else {
                throw new SQLException( "Can't convert column " + columnIndex
                         + " from "
                         + tmp.getClass().getName()
                         + " to Timestamp" );
            }
        }
        catch ( TdsException e ) {
            throw new SQLException( e.getMessage() );
        }
        return result;
    }

    public BigDecimal getBigDecimal( int columnIndex )
    throws SQLException
    {
        Object tmp = getObject( columnIndex );

        if ( tmp == null ) {
            return null;
        }

        BigDecimal result = null;

        if ( tmp instanceof java.lang.Double ) {
            result = new BigDecimal( ( ( Double ) tmp ).doubleValue() );
        }
        else if ( tmp instanceof java.lang.Float ) {
            result = new BigDecimal( ( ( Float ) tmp ).doubleValue() );
        }
        else if ( tmp instanceof BigDecimal ) {
            result = ( BigDecimal ) tmp;
        }
        else if ( tmp instanceof java.lang.Number ) {
            // This handles Byte, Short, Integer, and Long
            result = BigDecimal.valueOf( ( ( Number ) tmp ).longValue() );
        }
        else if ( tmp instanceof java.lang.String ) {
            try {
                result = new BigDecimal( ( String ) tmp );
            }
            catch ( NumberFormatException e ) {
                throw new SQLException( e.getMessage() );
            }
        }
        return result;
    }

    public BigDecimal getBigDecimal( int columnIndex, int scale )
             throws SQLException
    {

        BigDecimal result = getBigDecimal( columnIndex );

        if ( result == null ) {
            return null;
        }

        return result.setScale( scale );

    }

    public boolean getBoolean( int columnIndex ) throws SQLException
    {
        Object obj = getObject( columnIndex );
        boolean result;

        if ( obj == null ) {
            result = false;
        }
        else {
            switch ( getColumnType( columnIndex ) ) {
                case java.sql.Types.TINYINT:
                case java.sql.Types.SMALLINT:
                case java.sql.Types.INTEGER:
                case java.sql.Types.BIGINT:
                case java.sql.Types.REAL:
                case java.sql.Types.FLOAT:
                case java.sql.Types.DOUBLE:
                case java.sql.Types.DECIMAL:
                case java.sql.Types.NUMERIC:
                {
                    if ( !( obj instanceof java.lang.Number ) ) {
                        // Must be out of sync with the implementation of
                        // Tds.getRow() for this to happen.
                        throw new SQLException( "Internal error" );
                    }
                    // Would somebody like to tell what a true/false has
                    // to do with a double?
                    result = ( ( java.lang.Number ) obj ).intValue() != 0;
                    break;
                }
                case java.sql.Types.BIT:
                {
                    if ( !( obj instanceof Boolean ) ) {
                        // Must be out of sync with the implementation of
                        // Tds.getRow() for this to happen.
                        throw new SQLException( "Internal error" );
                    }
                    result = ( ( Boolean ) obj ).booleanValue();
                    break;
                }
                case java.sql.Types.CHAR:
                case java.sql.Types.VARCHAR:
                case java.sql.Types.LONGVARCHAR:
                {
                    // Okay, I'm really confused as to what you mean
                    // by a character string being true or false.  What
                    // is the boolean value for "Let the wookie win"?
                    // But since the spec says I have to convert from
                    // character to boolean data...

                    if ( !( obj instanceof String ) ) {
                        // Must be out of sync with the implementation of
                        // Tds.getRow() for this to happen.
                        throw new SQLException( "Internal error" );
                    }
                    char ch = ( ( ( String ) obj ) + "n" ).charAt( 0 );

                    result = ( ch == 'Y' ) || ( ch == 'y' ) || ( ch == 't' ) || ( ch == 'T' );
                    break;
                }
                default:
                {
                    throw new SQLException( "Can't convert column " + columnIndex
                             + " from "
                             + obj.getClass().getName()
                             + " to boolean" );
                }
            }
        }
        return result;
    }
    // getBoolean()


    public boolean wasNull()
    {
        return wasNull;
    }
}