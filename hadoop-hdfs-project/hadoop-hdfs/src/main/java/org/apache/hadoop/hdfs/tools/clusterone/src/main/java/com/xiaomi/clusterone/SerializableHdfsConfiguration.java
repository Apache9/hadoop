package com.xiaomi.clusterone;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.HdfsConfiguration;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * Created by xiegang1 on 17-1-18.
 */
public class SerializableHdfsConfiguration extends HdfsConfiguration implements Serializable {
  private static final Log LOG = LogFactory.getLog(SerializableHdfsConfiguration.class);
  private String propertiesStr = null;
  private long lastModifiedTime = 0;

  public long getLastModifiedTime() {
    return lastModifiedTime;
  }

  public void setLastModifiedTime(long lastModifiedTime) {
    this.lastModifiedTime = lastModifiedTime;
  }

  public void setPropertiesStr(String s) {
    this.propertiesStr = s;
  }

  public String getPropertiesStr(boolean update) {
    Properties properties = getProps();
    if (update && properties != null) {
      StringBuffer sb = new StringBuffer();
      Iterator<Map.Entry<Object, Object>> it = properties.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry<Object, Object> entry = it.next();
        String key = (String)entry.getKey();
        String value = (String)entry.getValue();
        sb.append(key);
        sb.append("=");
        if (value.contains(",")) {
          sb.append("\"");
        }
        sb.append(value);
        if (value.contains(",")) {
          sb.append("\"");
        }
        sb.append(",");
      }
      if (sb.length() > 0) {
        sb.delete(sb.length() - 1, sb.length() - 1);
      }
      propertiesStr = sb.toString();
    }
    return propertiesStr;
  }

  public void updatePropertiesFromString(String propertiesStr) {
    boolean ignore = false;
    StringBuffer sb = new StringBuffer(propertiesStr);
    int i = 0;
    while (sb.length() > 0) {
      if (sb.charAt(i) == '\"') {
        if (ignore) {
          ignore = false;
        } else {
          ignore = true;
        }
        sb.delete(i,i + 1);
        continue;
      }

      if (sb.charAt(i) == ',') {
        if (!ignore) {
          // end of the property
          String[] tmp = sb.substring(0, i).toString().split("=");
          sb.delete(0, i+1);
          //LOG.info(tmp[0] + "=" + tmp[1]);
          set(tmp[0], tmp[1]);
          i = 0;
          continue;
        }
      }

      i ++;
    }

    if (sb.length() > 0) {
      String[] tmp = sb.toString().split("=");
      set(tmp[0], tmp[1]);
    }
  }


  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    propertiesStr = getProps().toString();
    sb.append("PropertiesStr=");
    sb.append(propertiesStr);
    return sb.toString();
  }
}
