/*
 * %W% %E%
 * @(#)NegotiatorImpl.java
 *
 * Copyright (c) 2006, Oracle and/or its affiliates. All rights reserved.
 * ORACLE PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package sun.net.www.protocol.http;

import java.io.IOException;
import java.security.PrivilegedActionException;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.security.Security;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;

import sun.security.jgss.GSSManagerImpl;
import sun.security.jgss.GSSUtil;

/**
 * This class encapsulates all JAAS and JGSS API calls in a seperate class
 * outside NegotiateAuthentication.java so that J2SE build can go smoothly
 * without the presence of it.
 *
 * @author weijun.wang@sun.com
 * @since 1.6
 */
public class NegotiatorImpl extends Negotiator {

  private GSSContext context;
  private byte[] oneToken;

  /**
   * Initialize the object, which includes:<ul>
   * <li>Find out what GSS mechanism to use from <code>http.negotiate.mechanism.oid</code>,
   *     defaults SPNEGO
   * <li>Creating the GSSName for the target host, "HTTP/"+hostname
   * <li>Creating GSSContext
   * <li>A first call to initSecContext</ul>
   * @param hostname name of peer server
   * @param scheme auth scheme requested, Negotiate ot Kerberos
   * @throws GSSException if any JGSS-API call fails
   */
  private void init(final String hostname, String scheme) throws GSSException {
    // "1.2.840.113554.1.2.2" Kerberos
    // "1.3.6.1.5.5.2" SPNEGO
    final Oid oid;

    if (scheme.equalsIgnoreCase("Kerberos")) {
      // we can only use Kerberos mech when the scheme is kerberos
      oid = GSSUtil.GSS_KRB5_MECH_OID;
    } else {
      String pref = (String)java.security.AccessController.doPrivileged( 
          new java.security.PrivilegedAction() {
          public Object run() {
          return System.getProperty(
            "http.auth.preference",
            "spnego");
          }
          });
      if (pref.equalsIgnoreCase("kerberos")) {
        oid = GSSUtil.GSS_KRB5_MECH_OID;
      } else {
        // currently there is no 3rd mech we can use
        oid = GSSUtil.GSS_SPNEGO_MECH_OID;
      }
    }

    GSSManagerImpl manager = new GSSManagerImpl(
        GSSUtil.CALLER_HTTP_NEGOTIATE);

    String kerberosInstanceName = System.getProperty("kerberos.instance");
    String peerName = null;
    if (kerberosInstanceName == null) {
    	peerName = "HTTP/" + hostname;
    } else {
    	peerName = "HTTP/" + kerberosInstanceName;
    }

    GSSName serverName = manager.createName(peerName, null);
    context = manager.createContext(serverName,
        oid,
        null,
        GSSContext.DEFAULT_LIFETIME);

    context.requestCredDeleg(true);
    oneToken = context.initSecContext(new byte[0], 0, 0);
  }

  /**
   * Constructor
   * @param hostname name of peer server
   * @param scheme auth scheme requested, Negotiate ot Kerberos
   */
  public NegotiatorImpl(String hostname, String scheme) throws Exception {
    init(hostname, scheme);
  }

  /**
   * Return the first token of GSS, in SPNEGO, it's called NegTokenInit
   */
  public byte[] firstToken() {
    return oneToken;
  }

  /**
   * Return the rest tokens of GSS, in SPNEGO, it's called NegTokenTarg
   */
  public byte[] nextToken(byte[] token) throws Exception {
    return context.initSecContext(token, 0, token.length);
  }
}
