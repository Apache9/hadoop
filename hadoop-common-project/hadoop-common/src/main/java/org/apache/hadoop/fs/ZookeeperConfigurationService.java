package org.apache.hadoop.fs;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.ZooKeeper;

public class ZookeeperConfigurationService extends ConfigurationService {
  public static final Log LOG = LogFactory.getLog(ZookeeperConfigurationService.class);
  public ZookeeperConfigurationService(Properties prop) {
    super(prop);
  }

  @Override
  public Configuration fetchConfiguration(String authority) throws IOException {
    String zkHost = prop.getProperty(CONFIGURATION_SERVICE_ZOOKEEPER_HOST);
    if (zkHost == null) {
      throw new IOException("Couldn't find Zookeeper Host.");
    }
    String timeout = prop.getProperty(CONFIGURATION_SERVICE_SESSION_TIMEOUT);
    int sessionTimeout = timeout == null ? 3000 : Integer.parseInt(timeout);
    return fetchConfFromZk(zkHost, sessionTimeout, authority);
  }

  private Configuration fetchConfFromZk(String zkHost, int sessionTimeout,
                                        String authority) throws IOException {
    ZooKeeper zk = null;
    String confString = null;
    try {
      zk = new ZooKeeper(zkHost, sessionTimeout, null);
      confString = new String(
              zk.getData("/configuration-service/" + authority, false, null));
    } catch (Exception e) {
      throw new IOException("Failed fetching Configuration from Zookeeper:"+zkHost,e);
    } finally {
      try {
        if (zk != null) {
          zk.close();
        }
      } catch (InterruptedException e) {      }
    }
    if (confString.length() == 0) {
      LOG.warn("Fetched Configuration is empty!");
      return null;
    }
    return parse(confString);
  }

  // easy parse
  // confString is like
  // "dfs.namenode.rpc-address.nameservice.host0:localhost:9090\n"
  private Configuration parse(String confString) throws IOException {
    Configuration conf = new Configuration(false);
    String[] props = confString.split("\\$\n");
    for (String prop : props) {
      int i = prop.indexOf(":");
      if (i < 0 || i > prop.length() - 1) {
        throw new IOException(
                "Bad Configuration String. Couldn't parse:" + prop);
      }
      conf.set(prop.substring(0, i), prop.substring(i + 1, prop.length()));
    }
    return conf;
  }
}
