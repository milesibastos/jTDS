//jTDS JDBC Driver for Microsoft SQL Server and Sybase
//Copyright (C) 2004 The jTDS Project
//
//This library is free software; you can redistribute it and/or
//modify it under the terms of the GNU Lesser General Public
//License as published by the Free Software Foundation; either
//version 2.1 of the License, or (at your option) any later version.
//
//This library is distributed in the hope that it will be useful,
//but WITHOUT ANY WARRANTY; without even the implied warranty of
//MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
//Lesser General Public License for more details.
//
//You should have received a copy of the GNU Lesser General Public
//License along with this library; if not, write to the Free Software
//Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
//
package net.sourceforge.jtds.ssl;

/**
 * SSL settings
 *
 * @author Rob Worsnop
 * @version $Id: Ssl.java,v 1.1 2005-01-04 17:13:04 alin_sinpalean Exp $
 */
public interface Ssl {
    /**
     * SSL is not used.
     */
    public static final String SSL_OFF = "off";
    /**
     * SSL is requested; a plain socket is used if SSL is not available.
     */
    public static final String SSL_REQUEST = "request";
    /**
     * SSL is required; an exception if thrown if SSL is not available.
     */
    public static final String SSL_REQUIRE = "require";
    /**
     * SSL is required and the server must return a certificate signed by a
     * client-trusted authority.
     */
    public static final String SSL_AUTHENTICATE = "authenticate";

}
