package org.apache.hadoop.fs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public abstract class ConfigurationService {

  Configuration conf;
  public static final String CONFIGURATION_SERVICE = "configuration.service";
  public static final String CONFIGURATION_SERVICE_RETRY =
          "configuration.service.retry";
  public static final String CONFIGURATION_SERVICE_PRESERVE_USER_CONF =
      "configuration.service.preserve.user.conf";
  public static final int CONFIGURATION_SERVICE_RETRY_DEFAULT = 3;

  public ConfigurationService(Configuration conf) {
    this.conf = conf;
  }

  public abstract Configuration fetchConfiguration(String authority)
          throws IOException;

  public static final Log LOG = LogFactory.getLog(ConfigurationService.class);
  public static ArrayList<Pattern> ACCESS_CONFIG = new ArrayList<Pattern>();
  static {
    ACCESS_CONFIG.add(Pattern.compile("^dfs\\.nameservices"));
    ACCESS_CONFIG.add(Pattern.compile("^dfs\\.namenode\\.rpc-address\\..*"));
    ACCESS_CONFIG.add(
        Pattern.compile("^dfs\\.client\\.failover\\.proxy\\.provider\\..*"));
    ACCESS_CONFIG.add(Pattern.compile("^dfs\\.ha\\.namenodes\\..*"));
    ACCESS_CONFIG.add(Pattern.compile("^fs\\.viewfs\\.mounttable\\..*"));
    ACCESS_CONFIG.add(Pattern.compile("^fs\\.hdfs\\.impl"));
    ACCESS_CONFIG.add(Pattern.compile("^ha\\.zookeeper\\.quorum\\..*"));
    ACCESS_CONFIG
        .add(Pattern.compile("^dfs\\.client\\.zookeeper\\.observer\\..*"));
  }
  /**
   * Return cluster's configuration based on remote service, cluster is defined
   * by URI. If encounter any error, return the input configuration. The java
   * properties are loaded to update remoteServiceConf, so remote service could
   * be configured more flexible. The input configuration is used both in
   * initializing remote service conf and returning as the default value.
   */
  public static Configuration getConfigurationFromNameServiceWithDefaultValue(
      URI uri, Configuration configuration) {
    Configuration remoteConf =
        getConfigurationFromConfigurationService(uri, configuration);
    if (remoteConf == null) {
      return configuration;
    } else {
      return remoteConf;
    }
  }

  /**
   * Return cluster's configuration based on remote service, cluster is defined
   * by URI. If encounter any error, return null. The java properties are loaded
   * to update remoteServiceConf, so remote service could be configured more
   * flexible. The input configuration is used to initialize remote service
   * conf.
   * Preserve user configuration.
   */
  public static Configuration getConfigurationFromConfigurationService(URI uri,
      Configuration configuration) {
    boolean preserve = configuration
        .getBoolean(CONFIGURATION_SERVICE_PRESERVE_USER_CONF, true);
    return getConfigurationFromConfigurationService(uri, configuration,
        preserve);
  }

  public static Configuration getConfigurationFromConfigurationService(URI uri,
      Configuration configuration, boolean preserveUserConf) {
    String scheme = uri.getScheme();
    String authority = uri.getAuthority();
    Configuration remoteServiceConf = new Configuration(configuration);

    updateConfWithJavaProperties(remoteServiceConf);
    String className = remoteServiceConf.get(ConfigurationService.CONFIGURATION_SERVICE);
    if (className == null) {
      return null;
    } else if (scheme == null || authority == null) {
      return null;
    } else if (!scheme.equalsIgnoreCase("hdfs") || authority.length() <= 0) {
      return null;
    } else {// try to fetch configuration from Remote Service
      Class<?> clazz = null;
      Configuration remoteConf = null;
      try {
        clazz = Class.forName(className);
        Constructor<?> cor = clazz.getDeclaredConstructor(Configuration.class);
        ConfigurationService confService =
                (ConfigurationService) cor.newInstance(remoteServiceConf);
        int retry = remoteServiceConf.getInt(CONFIGURATION_SERVICE_RETRY,
                CONFIGURATION_SERVICE_RETRY_DEFAULT);
        while (retry>0) {
          try {
            remoteConf = confService.fetchConfiguration(authority);
            retry = 0;
          } catch (IOException e) {
            retry--;
            LOG.warn("Failed constructing configuration from remote service. " +
                    "NameService depends on " + className + ". " +
                    retry + " times left to try.", e);
            if (retry <= 0) {
              throw e;
            } else {
              try {
                SecureRandom random = new SecureRandom();
                long randomWait = Math.abs(random.nextLong())%2000;
                Thread.sleep(randomWait);
              } catch (InterruptedException ie) {
              }
            }
          }
        }
      } catch (Exception e) {
        LOG.warn(
                "Failed fetching configuration from remote service. Configuration Service is:"
                        + className, e);
      }

      if (remoteConf == null) {
        return null;
      } else {
        if (preserveUserConf) {
          Configuration res = new Configuration(configuration);
          Iterator<Map.Entry<String, String>> iterator =
              getAccessConf(remoteConf).iterator();
          while (iterator.hasNext()) {
            Map.Entry<String,String> entry = iterator.next();
            res.set(entry.getKey(), entry.getValue());
          }
          return res;
        } else {
          return remoteConf;
        }
      }
    }
  }

  public static Configuration getAccessConf(Configuration conf) {
    Configuration accessConf = new Configuration(false);
    Iterator<Map.Entry<String, String>> iterator = conf.iterator();
    while (iterator.hasNext()) {
      Map.Entry<String,String> entry = iterator.next();
      for (Pattern pattern : ACCESS_CONFIG) {
        if (pattern.matcher(entry.getKey()).matches()) {
          accessConf.set(entry.getKey(), entry.getValue());
          break;
        }
      }
    }
    return accessConf;
  }

  private static void updateConfWithJavaProperties(Configuration conf) {
    Properties props = System.getProperties();
    String prefix = "configuration.service";
    int size = prefix.length();
    for (String key : props.stringPropertyNames()) {
      if (key.length() >= size && key.substring(0, size).equals(prefix)) {
        conf.set(key, props.getProperty(key));
      }
    }
  }

  static Configuration createConfFromBytes(byte[] bytes) throws IOException {
    Configuration conf = new Configuration(false);
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    conf.readFields(new DataInputStream(in));
    in.close();
    return conf;
  }

  public interface Creator<T, E extends Exception> {
    T create(URI uri, Configuration configuration, Object... params) throws E;

    boolean retryWithConfigurationService(Exception e);
  }

  public static <T, E extends Exception> T createTargetObjWithConfigurationService(
      Creator<T, E> creator, URI uri, Configuration conf, Object... params)
      throws E {
    try {
      T obj = creator.create(uri, conf, params);
      return obj;
    } catch (Exception e) {
      if (creator.retryWithConfigurationService(e)) {
        LOG.info("Try to get configuration from remote service.URI:" + uri);
        Configuration remoteConf =
            getConfigurationFromConfigurationService(uri, conf);
        if (remoteConf == null) {
          LOG.warn("Failed getting conf from remote service. URI:" + uri);
          throw (E) e;
        } else {
          LOG.info("Succeed getting conf from remote service. Retry creator "
              + creator.getClass().getName());
          return creator.create(uri, remoteConf, params);
        }
      } else {
        throw (E) e;
      }
    }
  }
}
