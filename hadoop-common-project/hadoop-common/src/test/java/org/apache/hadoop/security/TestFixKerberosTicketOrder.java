/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.security;

import static org.junit.Assert.*;
import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.DestroyFailedException;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.minikdc.KerberosSecurityTestcase;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.SaslRpcServer.QualityOfProtection;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class TestFixKerberosTicketOrder extends KerberosSecurityTestcase {

  private String clientPrincipal = "client";

  private String server1Protocol = "server1";

  private String server2Protocol = "server2";

  private String host = "localhost";

  private String server1Principal = server1Protocol + "/" + host;

  private String server2Principal = server2Protocol + "/" + host;

  private File keytabFile;

  private Configuration conf = new Configuration();

  private Map<String, String> props;

  @Before
  public void setUp() throws Exception {
    keytabFile = new File(getWorkDir(), "keytab");
    getKdc().createPrincipal(keytabFile, clientPrincipal, server1Principal, server2Principal);
    SecurityUtil.setAuthenticationMethod(AuthenticationMethod.KERBEROS, conf);
    conf.setInt(CommonConfigurationKeys.HADOOP_KERBEROS_MIN_SECONDS_BEFORE_RELOGIN, 0);
    UserGroupInformation.setConfiguration(conf);
    props = new HashMap<String, String>();
    props.put(Sasl.QOP, QualityOfProtection.AUTHENTICATION.saslQop);
  }

  @Test
  public void test() throws IOException, InterruptedException, DestroyFailedException {
    UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(clientPrincipal,
      keytabFile.getCanonicalPath());
    ugi.doAs(new PrivilegedExceptionAction<Void>() {

      @Override
      public Void run() throws Exception {
        SaslClient client = Sasl.createSaslClient(
          new String[] { AuthMethod.KERBEROS.getMechanismName() }, clientPrincipal, server1Protocol,
          host, props, null);
        client.evaluateChallenge(new byte[0]);
        client.dispose();
        return null;
      }
    });

    Subject subject = ugi.getSubject();

    // move tgt to the last
    for (KerberosTicket ticket : subject.getPrivateCredentials(KerberosTicket.class)) {
      if (ticket.getServer().getName().startsWith("krbtgt")) {
        subject.getPrivateCredentials().remove(ticket);
        subject.getPrivateCredentials().add(ticket);
        break;
      }
    }
    for (Object cred : subject.getPrivateCredentials()) {
      if (cred instanceof KerberosTicket) {
        assertFalse(((KerberosTicket) cred).getServer().getName().startsWith("krbtgt"));
        break;
      }
    }
    // our MiniKdc does not reject TGT that does not starts with a krbtgt...
    // so we can only test if fixKerberosTicketOrder can move TGT to the first.
    ugi.fixKerberosTicketOrder();
    for (Object cred : subject.getPrivateCredentials()) {
      if (cred instanceof KerberosTicket) {
        assertTrue(((KerberosTicket) cred).getServer().getName().startsWith("krbtgt"));
        break;
      }
    }
    // make sure we can still get new service ticket after the fix.
    ugi.doAs(new PrivilegedExceptionAction<Void>() {

      @Override
      public Void run() throws Exception {
        SaslClient client = Sasl.createSaslClient(
          new String[] { AuthMethod.KERBEROS.getMechanismName() }, clientPrincipal, server2Protocol,
          host, props, null);
        client.evaluateChallenge(new byte[0]);
        client.dispose();
        return null;
      }
    });
    boolean found = false;
    for (KerberosTicket ticket : subject.getPrivateCredentials(KerberosTicket.class)) {
      if (ticket.getServer().getName().startsWith(server2Protocol)) {
        found = true;
        break;
      }
    }
    assertTrue(found);
  }
}
