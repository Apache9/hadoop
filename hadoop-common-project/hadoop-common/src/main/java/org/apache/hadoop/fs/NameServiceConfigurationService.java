package org.apache.hadoop.fs;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import org.apache.http.util.EntityUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import javax.net.ssl.SSLContext;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NameServiceConfigurationService extends ConfigurationService {

  // NameServiceConfigurationService config items
  public static final String CONFIGURATION_SERVICE_NAME_BASE_PATH =
      "configuration.service.name.base.path";
  public static final String CONFIGURATION_SERVICE_NAME_BASE_PATH_DEFAULT =
      "configuration-service";
  public static final String CONFIGURATION_SERVICE_NAME_HTTP_SERVER_PORT =
      "configuration.service.name.http.server.port";
  public static final int CONFIGURATION_SERVICE_NAME_HTTP_SERVER_PORT_DEFAULT =
      443;
  public static final String CONFIGURATION_SERVICE_NAME_TEAM_ID =
      "configuration.service.name.team.id";
  public static final String CONFIGURATION_SERVICE_NAME_TEAM_ID_DEFAULT =
      "default-team-id";
  public static final String CONFIGURATION_SERVICE_NAME_DOMAIN_PREFIX =
      "configuration.service.name.domain";
  public static final String CONFIGURATION_SERVICE_IDC =
      "configuration.service.idc";
  public static final String CONFIGURATION_SERVICE_NAME_CATALOG_URL =
      "configuration.service.name.catalog.url";
  public static final String CONFIGURATION_SERVICE_NAME_CATALOG_URL_DEFAULT =
      "https://cnbj3-ns.api.xiaomi.net/v1/api/ns/get";
  public static final String CONFIGURATION_SERVICE_NAME_CATALOG_URL_PATH =
      "configuration.service.name.catalog.url.path";
  public static final String CONFIGURATION_SERVICE_NAME_CATALOG_URL_PATH_DEFAULT =
      "configuration-service/conf";
  public static final String CONFIGURATION_SERVICE_NAME_VERSION =
      "configuration.service.name.version";
  public static final String CONFIGURATION_SERVICE_NAME_CLUSTER_PREFIX =
      "configuration.service.name.cluster";

  static public final Log LOG =
      LogFactory.getLog(NameServiceConfigurationService.class);
  private String basePath = conf.get(CONFIGURATION_SERVICE_NAME_BASE_PATH,
      CONFIGURATION_SERVICE_NAME_BASE_PATH_DEFAULT);
  private String teamId = conf.get(CONFIGURATION_SERVICE_NAME_TEAM_ID,
      CONFIGURATION_SERVICE_NAME_TEAM_ID_DEFAULT);
  private int port = conf.getInt(CONFIGURATION_SERVICE_NAME_HTTP_SERVER_PORT,
      CONFIGURATION_SERVICE_NAME_HTTP_SERVER_PORT_DEFAULT);

  public NameServiceConfigurationService(Configuration conf) {
    super(conf);
    // it's ok to modify conf here. It's a copy of user-conf.
    updateConfFromNameService();
  }

  @Override
  // this method never returns null.
  public Configuration fetchConfiguration(String authority) throws IOException {
    String uri = constructURI(authority);
    Configuration configuration = getConfigurationThroughHttp(uri);
    return configuration;
  }

  private String constructURI(String authority) throws IOException {
    String path = basePath + "/" + authority;
    if (conf.get("configuration.service.unit.test") != null) {
      return "http://127.0.0.1:" + port + "/v1/api/ns/get?teamId=" + teamId
          + "&path=" + path;
    } else {
      String domainName = conf.get(
          CONFIGURATION_SERVICE_NAME_DOMAIN_PREFIX + "." + getLocalIDCName(),
          conf.get(CONFIGURATION_SERVICE_NAME_DOMAIN_PREFIX + "."
              + getClusterIDCName(authority)));
      if (domainName == null) {
        throw new IOException(
            "Couldn't find domain name! LocalIDC is " + getLocalIDCName()
                + ", ClusterIDC is " + getClusterIDCName(authority));
      }
      String version = conf.get(CONFIGURATION_SERVICE_NAME_VERSION, "v1");
      String clusterValue = getClusterValue(authority);
      return "https://" + domainName + "-ns.api.xiaomi.net:" + port + "/"
          + version + "/api/ns/get?teamId=" + teamId + "&path=" + path
          + "&cluster=" + clusterValue;
    }
  }

  // never return null.
  public Configuration getConfigurationThroughHttp(String url)
      throws IOException {
    byte[] data = getDataThroughHttp(url);
    Configuration configuration = null;
    try {
      configuration = createConfFromBytes(Base64.decodeBase64(data));
    } catch (EOFException e) {
      throw new IOException("Failed construct configuration from data. Data:"
          + new String(data) + "\n" + e.getMessage());
    }
    return configuration;
  }

  // never return null.
  private byte[] getDataThroughHttp(String url) throws IOException {
    HttpGet httpGet = new HttpGet(url);
    Configuration configuration = null;
    String code = null;
    String description = null;
    byte[] data = null;
    try {
      JSONObject obj = getResponseThroughHttp(httpGet);
      code = obj.getString("code");
      description = obj.getString("description");
      data = obj.getString("data").getBytes();
      if (!code.equals("200")) {
        throw new IOException(
            "Failed code:" + code + " Description:" + description);
      }
    } catch (JSONException e) {
      throw new IOException("Failed parsing response!\n" + e.getMessage());
    }
    if (data == null) {
      throw new IOException(
          "Couldn't construct configuration. Data is null! Code:" + code
              + " Description:" + description + " data:" + data);
    }
    return data;
  }

  private JSONObject getResponseThroughHttp(HttpGet httpGet)
      throws IOException, JSONException {
    CloseableHttpClient httpClient = HttpClientFactory.getHttpClient();
    CloseableHttpResponse response = null;
    JSONObject obj = null;
    try {
      response = httpClient.execute(httpGet);
      String responseContent = EntityUtils.toString(response.getEntity());
      obj = new JSONObject(responseContent);
      EntityUtils.consume(response.getEntity());
    } finally {
      if (response != null) {
        response.close();
      }
    }
    LOG.info("Get response from uri:" + httpGet.getURI() + " successfully.");
    return obj;
  }

  // never return null
  private String getLocalIDCName() throws IOException {
    String host = InetAddress.getLocalHost().getHostName();
    int index = host.indexOf("-");
    if (index < 0) {
      throw new IOException("Couldn't get local IDC");
    }
    return host.substring(0, index);
  }

  private Pattern pattern = null;

  // never return null
  // Get the IDC which the given cluster is deployed in. IDC is in the list of
  // CONFIGURATION_SERVICE_IDC.
  private String getClusterIDCName(String cluster) throws IOException {
    if (pattern == null) {
      String[] idcs = conf.getStrings(CONFIGURATION_SERVICE_IDC);
      if (idcs == null || idcs.length == 0) {
        throw new IOException("IDC configuration is null. Check conf item:"
            + CONFIGURATION_SERVICE_IDC);
      }
      StringBuilder patternString = new StringBuilder("^(");
      for (String idc : idcs) {
        if (idc == null || idc.length() == 0)
          continue;
        patternString.append(idc.trim()).append("|");
      }
      if (patternString.charAt(patternString.length() - 1) == '|') {
        patternString.deleteCharAt(patternString.length() - 1);
      }
      patternString.append(")");
      if (patternString.toString().equals("^()")) {
        throw new IOException("IDC configuration is null. Check conf item:"
            + CONFIGURATION_SERVICE_IDC);
      }
      pattern = Pattern.compile(patternString.toString());
    }
    Matcher m = pattern.matcher(cluster);
    if (m.find()) {
      return m.group();
    } else {
      throw new IOException("Couldn't find IDC. Failed match " + cluster
          + " with pattern " + m.pattern());
    }
  }

  private String getClusterValue(String authority) throws IOException {
    String clusterIDCName = getClusterIDCName(authority);
    return conf.get(
        CONFIGURATION_SERVICE_NAME_CLUSTER_PREFIX + "." + clusterIDCName,
        "xns_" + clusterIDCName + "xnssrv");
  }

  // Request NameService to update conf(used by
  // NameServiceConfigurationService).
  // Won't update item which has already been set in this.conf. Won't update
  // conf if incur any exception.
  // We add this method to make NameServiceConfigurationService more flexible.
  // e.g. constructing url.
  private void updateConfFromNameService() {
    String url = conf.get(CONFIGURATION_SERVICE_NAME_CATALOG_URL,
        CONFIGURATION_SERVICE_NAME_CATALOG_URL_DEFAULT) + "?teamId=" + teamId
        + "&path=" + conf.get(CONFIGURATION_SERVICE_NAME_CATALOG_URL_PATH,
            CONFIGURATION_SERVICE_NAME_CATALOG_URL_PATH_DEFAULT);
    try {
      byte[] data = getDataThroughHttp(url);
      JSONObject obj = new JSONObject(new String(data));
      Iterator ite = obj.keys();
      while (ite.hasNext()) {
        String key = (String) ite.next();
        String value = obj.getString(key);
        if (conf.get(key) == null) {
          conf.set(key, value);
        }
      }
    } catch (IOException e) {
      LOG.warn("Failed update conf from " + url + ".", e);
    } catch (JSONException e) {
      LOG.warn("Failed update conf from " + url + ".", e);
    }
  }
}

class HttpClientFactory {
  static public final Log LOG =
      LogFactory.getLog(NameServiceConfigurationService.class);

  protected HttpClientFactory() {
  }

  static private volatile CloseableHttpClient httpClient;

  static synchronized public CloseableHttpClient getHttpClient() {
    if (httpClient == null) {
      synchronized (HttpClientFactory.class) {
        if (httpClient == null) {
          try {
            SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(
                SSLContext.getDefault(), NoopHostnameVerifier.INSTANCE);
            final Registry<ConnectionSocketFactory> registry =
                RegistryBuilder.<ConnectionSocketFactory> create()
                    .register("http", new PlainConnectionSocketFactory())
                    .register("https", sslsf).build();

            final PoolingHttpClientConnectionManager cm =
                new PoolingHttpClientConnectionManager(registry);
            cm.setMaxTotal(100);

            httpClient = HttpClients.custom().setSSLSocketFactory(sslsf)
                .setConnectionManager(cm).build();
          } catch (NoSuchAlgorithmException e) {
            LOG.warn("Failed create CloseableHttpClient.", e);
          }
        }
      }
    }
    return httpClient;
  }
}
