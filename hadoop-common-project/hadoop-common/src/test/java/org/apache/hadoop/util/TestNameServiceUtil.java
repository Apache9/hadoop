package org.apache.hadoop.util;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.util.Map;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertTrue;

public class TestNameServiceUtil {

  private static final Log LOG = LogFactory.getLog(TestNameServiceUtil.class);
  private static HttpServer httpServer;
  private static String authority = "ut-machine";
  private static String authorization = "secretKey";
  private static Configuration localConf;
  private static Configuration catalogConf;

  @BeforeClass
  public static void setupClass() throws IOException {
    httpServer = HttpServer.create(new InetSocketAddress(0), 0);
    httpServer.createContext("/", new Handler());
    httpServer.setExecutor(Executors.newCachedThreadPool());
    httpServer.start();

    localConf = new Configuration(false);
    localConf.set("configuration.service.unit.test", "unit.test");
    localConf.set(NameServiceUtil.CONFIGURATION_SERVICE_NAME_CATALOG_URL_GET,
        "http://127.0.0.1:" + httpServer.getAddress().getPort()
            + "/v1/api/ns/get");
    localConf.set(NameServiceUtil.CONFIGURATION_SERVICE_NAME_CATALOG_URL_PUT,
        "http://127.0.0.1:" + httpServer.getAddress().getPort()
            + "/v1/api/ns/put");
    localConf
        .setInt(NameServiceUtil.CONFIGURATION_SERVICE_NAME_HTTP_SERVER_PORT,
            httpServer.getAddress().getPort());
    localConf.set(NameServiceUtil.CONFIGURATION_SERVICE_NAME_URL_GET,
        "http://%s:%d/%s/api/ns/get?teamId=%s&path=%s&cluster=%s");
    localConf.set(NameServiceUtil.CONFIGURATION_SERVICE_NAME_URL_PUT,
        "http://%s:%d/%s/api/ns/put");

    catalogConf = new Configuration(false);
    catalogConf.set(NameServiceUtil.CONFIGURATION_SERVICE_IDC, "ut");
    catalogConf
        .set(NameServiceUtil.CONFIGURATION_SERVICE_NAME_DOMAIN_PREFIX + ".ut",
            "127.0.0.1");
    catalogConf.set(NameServiceUtil.CONFIGURATION_SERVICE_NAME_VERSION, "v1");
    catalogConf
        .set(NameServiceUtil.CONFIGURATION_SERVICE_NAME_CLUSTER_PREFIX + ".ut",
            "xns_c3xnssrv");
  }

  @AfterClass
  public static void cleanupClass() {
    httpServer.stop(0);
  }

  private static boolean isConfEqual(Configuration a, Configuration b) {
    if (a.size() != b.size()) {
      return false;
    }
    for (Map.Entry<String, String> entry : a) {
      String key = entry.getKey();
      String value = entry.getValue();
      String valueb = b.get(key);
      if (!value.equals(valueb)) {
        return false;
      }
    }
    return true;
  }

  private void testGetAndPutCatalogConf() throws IOException {

    NameServiceUtil.putCatalogConf(localConf, catalogConf, authorization);
    Configuration remoteCatalogConf = NameServiceUtil.getCatalogConf(localConf);

    assertTrue("get catalogConf isn't equals to origin put catalogConf.",
        isConfEqual(catalogConf, remoteCatalogConf));
  }

  @Test
  public void testGetAndPutConf() throws IOException {
    testGetAndPutCatalogConf();

    Configuration conf = new Configuration(false);
    conf.set("configuration.service.testmsg", "HelloWorld");

    NameServiceUtil.putConf(authority, localConf, conf, authorization);

    Configuration remoteConf = NameServiceUtil.getConf(authority, localConf);
    assertTrue("get conf isn't equals to origin put conf.",
        isConfEqual(conf, remoteConf));

  }

  static class Handler implements HttpHandler {
    String conf;
    String catalog;

    public Handler() {
      super();
    }

    @Override
    public void handle(HttpExchange httpExchange) throws IOException {
      String method = httpExchange.getRequestMethod();
      // construct response
      if ("GET".equals(method)) {
        URI uri = httpExchange.getRequestURI();
        LOG.info("GET: Request uri: " + uri);
        String path = uri.getQuery();
        String data = null;
        if (path.contains(
            NameServiceUtil.CONFIGURATION_SERVICE_NAME_CATALOG_URL_PATH_DEFAULT)) {
          data = catalog;
        } else if (path.contains("ut-machine")) {
          data = conf;
        }
        Headers responseHeaders = httpExchange.getResponseHeaders();
        String jsonString =
            "{\n" + "\"code\": 200,\n" + "\"data\": " + data + ",\n"
                + "\"description\": \"save success\"\n" + "}";
        LOG.info("Response string: " + jsonString);
        responseHeaders.set("Context-Type", "application/json");
        httpExchange.sendResponseHeaders(200, 0);
        OutputStream responseBodyOut = httpExchange.getResponseBody();
        responseBodyOut.write(jsonString.getBytes());
        responseBodyOut.close();
      } else if ("POST".equals(method)) {
        InputStream in = httpExchange.getRequestBody();
        byte[] bytes = IOUtils.toByteArray(in);
        String body = new String(bytes, "UTF-8");
        body = URLDecoder.decode(body, "utf-8");
        LOG.info("POST: Request body: " + body);
        String param[] = body.split("&");
        String path = param[0];
        String data = param[1].substring(6);
        if (path.contains(
            NameServiceUtil.CONFIGURATION_SERVICE_NAME_CATALOG_URL_PATH_DEFAULT)) {
          catalog = data;
        } else if (path.contains("ut-machine")) {
          conf = "\"" + data + "\"";
        }
        Headers responseHeaders = httpExchange.getResponseHeaders();
        responseHeaders.set("Context-Type", "text/plain");
        httpExchange.sendResponseHeaders(200, 0);
        OutputStream responseBodyOut = httpExchange.getResponseBody();
        String jsonString =
            "{\n" + "\"code\": 200,\n" + "\"description\": \"save success\"\n"
                + "}";
        responseBodyOut.write(jsonString.getBytes());
        responseBodyOut.close();
      } else {
        throw new IOException("Non support request." + method);
      }
    }
  }
}
