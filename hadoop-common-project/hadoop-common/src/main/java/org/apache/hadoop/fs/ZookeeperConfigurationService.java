package org.apache.hadoop.fs;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
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
    byte[] bytes = null;
    try {
      zk = new ZooKeeper(zkHost, sessionTimeout, new Watcher() {
        @Override
        public void process(WatchedEvent event) {
        }
      });
      bytes = zk.getData("/configuration-service/" + authority, false, null);
    } catch (Exception e) {
      throw new IOException("Failed fetching Configuration from Zookeeper:"+zkHost,e);
    } finally {
      try {
        if (zk != null) {
          zk.close();
        }
      } catch (InterruptedException e) {
      }
    }
    if (bytes.length == 0) {
      LOG.warn("Fetched Configuration is empty!");
      return null;
    }

    return createConfFromBytes(bytes);
  }

  private Configuration createConfFromBytes(byte[] bytes) throws IOException {
    Configuration conf = new Configuration(false);
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    conf.readFields(new DataInputStream(in));
    in.close();
    return conf;
  }

}
