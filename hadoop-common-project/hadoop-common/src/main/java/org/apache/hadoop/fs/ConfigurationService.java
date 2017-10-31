package org.apache.hadoop.fs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.util.Properties;

public abstract class ConfigurationService {

  Properties prop;
  public static final String CONFIGURATION_SERVICE = "configuration.service";
  public static final String CONFIGURATION_SERVICE_ZOOKEEPER_HOST =
          "configuration.service.zookeeper.host";
  public static final String CONFIGURATION_SERVICE_SESSION_TIMEOUT =
          "configuration.service.zookeeper.sessiontimeout";

  public ConfigurationService(Properties prop) {
    this.prop = prop;
  }

  public abstract Configuration fetchConfiguration(String authority)
          throws IOException;

  public static final Log LOG = LogFactory.getLog(ConfigurationService.class);

  /**
   * Return a Configuration object based on input configuration.
   * If input uri doesn't match input configuration, return the NameService's configuration.
   * If input uri match input configuration but NN address are out-of-date, update them.
   * If fail fetching or parsing configuration from NameService, return a clone of input configuration.
   */
  public static Configuration updateConfigurationWithNameService(URI uri,
                                                                  Configuration configuration) {
    String scheme = uri.getScheme();
    String authority = uri.getAuthority();
    Configuration conf = new Configuration(configuration);
    if (System.getProperty(ConfigurationService.CONFIGURATION_SERVICE) == null) {
      return conf;
    } else if (scheme == null || authority == null) {
      return conf;
    } else if (!scheme.equalsIgnoreCase("hdfs") || authority.length() <= 0) {
      return conf;
    } else {// try to fetch configuration from NameService
      String className =
              System.getProperty(ConfigurationService.CONFIGURATION_SERVICE);
      Class<?> clazz = null;
      Configuration remoteConf = null;
      try {
        clazz = Class.forName(className);
        Constructor<?> cor = clazz.getDeclaredConstructor(Properties.class);
        ConfigurationService confService =
                (ConfigurationService) cor.newInstance(System.getProperties());
        remoteConf = confService.fetchConfiguration(authority);
      } catch (Exception e) {
        LOG.warn(
                "Failed fetching configuration from remote service. Configuration Service is:"
                        + className, e);
      }

      if (remoteConf == null) {
        return conf;
      }

      if (isUriMatchConf(uri, conf)) {
        updateNamenodesAddress(authority, conf, remoteConf);
      } else {
        replaceConfiguration(conf, remoteConf);
      }
      return conf;
    }
  }

  private static boolean isUriMatchConf(URI uri, Configuration conf) {
    String nameservices = conf.get("dfs.nameservices");
    if (nameservices == null || nameservices.length() == 0) {
      return false;
    }
    String[] nameserviceIds = nameservices.split(",");
    for (String id : nameserviceIds) {
      if (id.trim().equals(uri.getAuthority())) {
        return true;
      }
    }
    return false;
  }

  private static void replaceConfiguration(Configuration conf,
                                           Configuration remoteConf) {
    conf.clear();
    conf.addResource(remoteConf);
  }

  private static void updateNamenodesAddress(String nameservice,
                                             Configuration conf, Configuration remoteConf) {
    Configuration defaultConf = new Configuration();
    updateDefaultConfValueWithRemoteConf(
            "dfs.namenode.rpc-address." + nameservice + ".host0", conf, remoteConf,
            defaultConf);
    updateDefaultConfValueWithRemoteConf(
            "dfs.namenode.rpc-address." + nameservice + ".host1", conf, remoteConf,
            defaultConf);
  }

  private static void updateDefaultConfValueWithRemoteConf(String key,
                                                           Configuration conf, Configuration remoteConf, Configuration defaultConf) {
    String confValue = conf.get(key);
    String remoteValue = remoteConf.get(key);
    if (remoteValue != null) {
      if (confValue == null || confValue.equals(defaultConf.get(key))) {
        conf.set(key, remoteValue);
      }
    }
  }
}