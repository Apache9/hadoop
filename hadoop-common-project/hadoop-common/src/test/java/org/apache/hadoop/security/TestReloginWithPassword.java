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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.LoginException;
import javax.security.sasl.Sasl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.minikdc.KerberosSecurityTestcase;
import org.apache.hadoop.security.SaslRpcServer.QualityOfProtection;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.junit.Before;
import org.junit.Test;

public class TestReloginWithPassword extends KerberosSecurityTestcase {

  private String principal = "client";

  private String password = "123456";

  private Configuration conf = new Configuration();

  private Map<String, String> props;

  @Before
  public void setUp() throws Exception {
    getKdc().createPrincipal(principal, password);
    SecurityUtil.setAuthenticationMethod(AuthenticationMethod.KERBEROS, conf);
    conf.setInt(
        CommonConfigurationKeys.HADOOP_KERBEROS_MIN_SECONDS_BEFORE_RELOGIN, 0);
    conf.set(CommonConfigurationKeys.HADOOP_CLIENT_KERBEROS_PRINCIPAL,
        principal);
    conf.set(CommonConfigurationKeys.HADOOP_CLIENT_KERBEROS_PASSWORD, password);
    UserGroupInformation.setConfiguration(conf);
    props = new HashMap<String, String>();
    props.put(Sasl.QOP, QualityOfProtection.AUTHENTICATION.saslQop);
  }

  @Test
  public void test() throws IOException, LoginException {
    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    assertFalse(
        ugi.getSubject().getPrincipals(KerberosPrincipal.class).isEmpty());
    ugi.getLogin().logout();
    assertTrue(
        ugi.getSubject().getPrincipals(KerberosPrincipal.class).isEmpty());
    ugi.reloginFromKerberosKey();
    assertFalse(
        ugi.getSubject().getPrincipals(KerberosPrincipal.class).isEmpty());
  }
}
