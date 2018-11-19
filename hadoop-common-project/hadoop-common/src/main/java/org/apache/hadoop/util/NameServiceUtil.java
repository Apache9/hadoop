package org.apache.hadoop.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.util.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.NameServiceConfigurationService;
import org.apache.http.Consts;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import javax.net.ssl.SSLContext;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class NameServiceUtil {

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
  public static final String CONFIGURATION_SERVICE_NAME_CATALOG_URL_GET =
      "configuration.service.name.catalog.url.get";
  public static final String
      CONFIGURATION_SERVICE_NAME_CATALOG_URL_GET_DEFAULT =
      "https://cnbj3-ns.api.xiaomi.net/v1/api/ns/get";
  public static final String CONFIGURATION_SERVICE_NAME_CATALOG_URL_PUT =
      "configuration.service.name.catalog.url.put";
  public static final String
      CONFIGURATION_SERVICE_NAME_CATALOG_URL_PUT_DEFAULT =
      "https://cnbj3-ns.api.xiaomi.net/v1/api/ns/put";
  public static final String CONFIGURATION_SERVICE_NAME_CATALOG_URL_PATH =
      "configuration.service.name.catalog.url.path";
  public static final String
      CONFIGURATION_SERVICE_NAME_CATALOG_URL_PATH_DEFAULT =
      "configuration-service/conf";
  public static final String CONFIGURATION_SERVICE_NAME_VERSION =
      "configuration.service.name.version";
  public static final String CONFIGURATION_SERVICE_NAME_CLUSTER_PREFIX =
      "configuration.service.name.cluster";
  public static final String CONFIGURATION_SERVICE_NAME_URL_GET =
      "configuration.service.name.url.get";
  public static final String CONFIGURATION_SERVICE_NAME_URL_GET_DEFAULT =
      "https://%s-ns.api.xiaomi.net:%d/%s/api/ns/get?teamId=%s&path=%s&cluster=%s";
  public static final String CONFIGURATION_SERVICE_NAME_URL_PUT =
      "configuration.service.name.url.put";
  public static final String CONFIGURATION_SERVICE_NAME_URL_PUT_DEFAULT =
      "https://%s-ns.api.xiaomi.net:%d/%s/api/ns/put";

  public static final Log LOG = LogFactory.getLog(NameServiceUtil.class);

  private NameServiceUtil() {
  }

  public static Configuration getConf(String authority, Configuration localConf)
      throws IOException {
    return getConfInternal(authority, new Configuration(localConf));
  }

  public static void putConf(String authority, Configuration localConf,
      Configuration conf, String authorization) throws IOException {
    putConfInternal(authority, new Configuration(localConf), conf,
        authorization);
  }

  public static Configuration getCatalogConf(Configuration localConf) {
    return getCatalogConfInternal(localConf);
  }

  public static void putCatalogConf(Configuration localConf,
      Configuration catalogConf, String authorization) throws IOException {
    putCatalogConfInternal(localConf, catalogConf, authorization);
  }

  private static Configuration getConfInternal(String authority,
      Configuration localConf) throws IOException {
    Configuration catalogConf = getCatalogConf(localConf);
    updateConf(localConf, catalogConf);

    // construct url.
    String basePath = localConf.get(CONFIGURATION_SERVICE_NAME_BASE_PATH,
        CONFIGURATION_SERVICE_NAME_BASE_PATH_DEFAULT);
    int port = localConf.getInt(CONFIGURATION_SERVICE_NAME_HTTP_SERVER_PORT,
        CONFIGURATION_SERVICE_NAME_HTTP_SERVER_PORT_DEFAULT);
    String teamId = localConf.get(CONFIGURATION_SERVICE_NAME_TEAM_ID,
        CONFIGURATION_SERVICE_NAME_TEAM_ID_DEFAULT);
    String path = basePath + "/" + authority;
    String localIDCName = null;
    try {
      localIDCName = getLocalIDCName();
    } catch (IOException e) {
      LOG.debug("Failed getting local IDC", e);
    }
    String clusterIDCName = null;
    try {
      clusterIDCName = getClusterIDCName(authority, localConf);
    } catch (IOException e) {
      LOG.debug("Failed getting cluster IDC", e);
    }
    String domainName = localConf
        .get(CONFIGURATION_SERVICE_NAME_DOMAIN_PREFIX + "." + localIDCName,
            localConf.get(CONFIGURATION_SERVICE_NAME_DOMAIN_PREFIX + "."
                + clusterIDCName));
    if (domainName == null) {
      throw new IOException(
          "Couldn't find domain name! LocalIDC is " + localIDCName
              + ", ClusterIDC is " + clusterIDCName);
    }
    String version = localConf.get(CONFIGURATION_SERVICE_NAME_VERSION, "v1");
    String clusterValue = localConf
        .get(CONFIGURATION_SERVICE_NAME_CLUSTER_PREFIX + "." + clusterIDCName,
            "xns_" + clusterIDCName + "xnssrv");
    String baseUrl = localConf.get(CONFIGURATION_SERVICE_NAME_URL_GET,
        CONFIGURATION_SERVICE_NAME_URL_GET_DEFAULT);
    String url = String
        .format(baseUrl, domainName, port, version, teamId, path, clusterValue);

    // get remote conf
    Configuration remoteConf = new Configuration(false);
    byte[] data = getDataThroughHttp(url);
    data = Base64.decodeBase64(data);
    try {
      ByteArrayInputStream in = new ByteArrayInputStream(data);
      remoteConf.readFields(new DataInputStream(in));
      in.close();
    } catch (EOFException e) {
      throw new IOException(
          "Failed construct configuration from data. Data:" + new String(data)
              + "\n" + e.getMessage());
    }

    return remoteConf;
  }

  private static void putConfInternal(String authority, Configuration localConf,
      Configuration conf, String authorization) throws IOException {
    Configuration catalogConf = getCatalogConf(localConf);
    updateConf(localConf, catalogConf);

    // construct url
    String basePath = localConf.get(CONFIGURATION_SERVICE_NAME_BASE_PATH,
        CONFIGURATION_SERVICE_NAME_BASE_PATH_DEFAULT);
    int port = localConf.getInt(CONFIGURATION_SERVICE_NAME_HTTP_SERVER_PORT,
        CONFIGURATION_SERVICE_NAME_HTTP_SERVER_PORT_DEFAULT);
    String path = basePath + "/" + authority;
    String localIDCName = getLocalIDCName();
    String clusterIDCName = getClusterIDCName(authority, localConf);
    String clusterValue = localConf
        .get(CONFIGURATION_SERVICE_NAME_CLUSTER_PREFIX + "." + clusterIDCName,
            "xns_" + clusterIDCName + "xnssrv");
    String domainName = localConf
        .get(CONFIGURATION_SERVICE_NAME_DOMAIN_PREFIX + "." + localIDCName,
            localConf.get(CONFIGURATION_SERVICE_NAME_DOMAIN_PREFIX + "."
                + clusterIDCName));
    String version = localConf.get(CONFIGURATION_SERVICE_NAME_VERSION, "v1");
    if (domainName == null) {
      throw new IOException(
          "Couldn't find domain name! LocalIDC is " + localIDCName
              + ", ClusterIDC is " + clusterIDCName);
    }
    String baseUrl = localConf.get(CONFIGURATION_SERVICE_NAME_URL_PUT,
        CONFIGURATION_SERVICE_NAME_URL_PUT_DEFAULT);
    String url = String.format(baseUrl, domainName, port, version);

    // construct post request.
    HttpPost httpPost = new HttpPost(url);
    // construct post request header
    httpPost.setHeader("Authorization", authorization);
    // construct post request content.
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    conf.write(new DataOutputStream(out));
    out.flush();
    byte[] bytes = Base64.encodeBase64(out.toByteArray());
    out.close();
    List<NameValuePair> params = new ArrayList<NameValuePair>();
    params.add(new BasicNameValuePair("path", path));
    params.add(new BasicNameValuePair("value", new String(bytes)));
    params.add(new BasicNameValuePair("cluster", clusterValue));
    httpPost.setEntity(new UrlEncodedFormEntity(params, Consts.UTF_8.name()));

    getResponseThroughHttp(httpPost);
  }

  private static Configuration getCatalogConfInternal(Configuration localConf) {
    String teamId = localConf.get(CONFIGURATION_SERVICE_NAME_TEAM_ID,
        CONFIGURATION_SERVICE_NAME_TEAM_ID_DEFAULT);
    String path = localConf.get(CONFIGURATION_SERVICE_NAME_CATALOG_URL_PATH,
        CONFIGURATION_SERVICE_NAME_CATALOG_URL_PATH_DEFAULT);
    String catalogUrl = localConf
        .get(CONFIGURATION_SERVICE_NAME_CATALOG_URL_GET,
            CONFIGURATION_SERVICE_NAME_CATALOG_URL_GET_DEFAULT) + "?teamId="
        + teamId + "&path=" + path;
    Configuration catalogConf = new Configuration(false);
    try {
      byte[] data = getDataThroughHttp(catalogUrl);
      JSONObject obj = new JSONObject(new String(data));
      Iterator ite = obj.keys();
      while (ite.hasNext()) {
        String key = (String) ite.next();
        String value = obj.getString(key);
        catalogConf.set(key, value);
      }
    } catch (IOException e) {
      LOG.warn("Failed get catalogConf from " + catalogUrl + ".", e);
    } catch (JSONException e) {
      LOG.warn("Failed parse catalogConf from " + catalogUrl + ".", e);
    }
    return catalogConf;
  }

  private static void putCatalogConfInternal(Configuration localConf,
      Configuration catalogConf, String authorization) throws IOException {
    JSONObject obj = new JSONObject();
    try {
      for (Map.Entry<String, String> entry : catalogConf) {
        String key = entry.getKey();
        String value = entry.getValue();
        obj.put(key, value);
      }
    } catch (JSONException e) {
      throw new IOException("Convert Configuration to Json error.", e);
    }
    String catalogUrl = localConf
        .get(CONFIGURATION_SERVICE_NAME_CATALOG_URL_PUT,
            CONFIGURATION_SERVICE_NAME_CATALOG_URL_PUT_DEFAULT);
    String path = localConf.get(CONFIGURATION_SERVICE_NAME_CATALOG_URL_PATH,
        CONFIGURATION_SERVICE_NAME_CATALOG_URL_PATH_DEFAULT);
    HttpPost httpPost = new HttpPost(catalogUrl);
    // construct post request header
    httpPost.setHeader("Authorization", authorization);
    // construct post request content.
    List<NameValuePair> params = new ArrayList<NameValuePair>();
    params.add(new BasicNameValuePair("path", path));
    params.add(new BasicNameValuePair("value", obj.toString()));
    httpPost.setEntity(new UrlEncodedFormEntity(params, Consts.UTF_8.name()));
    getResponseThroughHttp(httpPost);
  }

  private static void updateConf(Configuration dest, Configuration src) {
    // Won't update item which has already been set in conf.
    for (Map.Entry<String, String> entry : src) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (dest.get(key) == null) {
        dest.set(key, value);
      }
    }
  }

  private static byte[] getDataThroughHttp(String url) throws IOException {
    HttpGet httpGet = new HttpGet(url);
    byte[] data = null;
    try {
      JSONObject obj = getResponseThroughHttp(httpGet);
      data = obj.getString("data").getBytes();
    } catch (JSONException e) {
      throw new IOException("No data in response content!\n" + e.getMessage());
    }
    return data;
  }

  private static JSONObject getResponseThroughHttp(HttpRequestBase httpRequest)
      throws IOException {
    CloseableHttpClient httpClient =
        org.apache.hadoop.util.HttpClientFactory.getHttpClient();
    CloseableHttpResponse response = null;
    JSONObject obj = null;
    String code = null;
    String description = null;
    try {
      response = httpClient.execute(httpRequest);
      String responseContent = EntityUtils.toString(response.getEntity());
      obj = new JSONObject(responseContent);
      EntityUtils.consume(response.getEntity());

      code = obj.getString("code");
      description = obj.getString("description");
      if (!code.equals("200")) {
        throw new IOException(
            "Failed code:" + code + " Description:" + description);
      }
    } catch (JSONException e) {
      throw new IOException("Failed parsing response!\n" + e.getMessage());
    } finally {
      if (response != null) {
        response.close();
      }
    }
    LOG.info(httpRequest.getClass() + ": Get response from uri:" + httpRequest
        .getURI() + " successfully.");

    return obj;
  }

  // never return null
  private static String getLocalIDCName() throws IOException {
    String host = InetAddress.getLocalHost().getHostName();
    int index = host.indexOf("-");
    if (index < 0) {
      throw new IOException("Couldn't get local IDC");
    }
    return host.substring(0, index);
  }

  // never return null
  // Get the IDC which the given cluster is deployed in. IDC is in the list of
  // CONFIGURATION_SERVICE_IDC.
  private static String getClusterIDCName(String authority, Configuration conf)
      throws IOException {
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
    Pattern pattern = Pattern.compile(patternString.toString());
    Matcher m = pattern.matcher(authority);
    if (m.find()) {
      return m.group();
    } else {
      throw new IOException(
          "Couldn't find IDC. Failed match " + authority + " with pattern " + m
              .pattern());
    }
  }

}

class HttpClientFactory {
  static public final Log LOG =
      LogFactory.getLog(NameServiceConfigurationService.class);
  static private volatile CloseableHttpClient httpClient;

  protected HttpClientFactory() {
  }

  static synchronized CloseableHttpClient getHttpClient() {
    if (httpClient == null) {
      synchronized (org.apache.hadoop.util.HttpClientFactory.class) {
        if (httpClient == null) {
          try {
            SSLConnectionSocketFactory sslsf =
                new SSLConnectionSocketFactory(SSLContext.getDefault(),
                    NoopHostnameVerifier.INSTANCE);
            final Registry<ConnectionSocketFactory> registry =
                RegistryBuilder.<ConnectionSocketFactory>create()
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
