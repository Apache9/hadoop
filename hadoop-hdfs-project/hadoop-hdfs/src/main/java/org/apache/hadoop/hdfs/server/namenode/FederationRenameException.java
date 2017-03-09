package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FederationRenameException extends IOException {
  private static final long serialVersionUID = 1L;

  public FederationRenameException(String msg) {
    super(msg);
  }

  public FederationRenameException(Throwable t) {
    super(t);
  }
}
