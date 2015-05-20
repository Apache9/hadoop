package org.apache.hadoop.security;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import java.io.IOException;

public class EchoCallbackHandler implements CallbackHandler {
  private final String userName;
  private final String password;

  public EchoCallbackHandler(String userName, String password) {
    this.userName = userName;
    this.password = password;
  }

  @Override public void handle(Callback[] callbacks)
      throws IOException, UnsupportedCallbackException {
    for (int i = 0; i < callbacks.length; i++) {
      if (callbacks[i] instanceof NameCallback) {
        NameCallback nc = (NameCallback) callbacks[i];
        nc.setName(userName);
      } else if (callbacks[i] instanceof PasswordCallback) {
        PasswordCallback pc = (PasswordCallback) callbacks[i];
        pc.setPassword(password == null ? null : password.toCharArray());
      } else {
        throw new UnsupportedCallbackException(callbacks[i], "Unrecognized Callback");
      }
    }
  }
}
