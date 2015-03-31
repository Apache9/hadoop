/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.shell;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIsNotDirectoryException;
import org.apache.hadoop.util.StringUtils;

import com.google.common.base.Preconditions;

/**
 * Ttl related operations.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class TtlCommands extends FsCommand {
  private static final String GET_TTL = "getTtl";
  private static final String SET_TTL = "setTtl";
  private static final String ATTR_NAME = "user.ttl";
  private static final String ATTR_PROPERTY = "user.ttlproperty";
  private static final int SINCELASTWRITE = 0x1;
  private static final int KEEPEMPTYDIR = 0x2;
  private static final int KEEPEMPTYSUBDIR = 0x4;

  private static final Map<String, Integer> SUFFIX_MAP =
      new HashMap<String, Integer>();
  static {
    SUFFIX_MAP.put("M", 1);
    SUFFIX_MAP.put("h", 60);
    SUFFIX_MAP.put("d", 1440);
    SUFFIX_MAP.put("w", 10080);
    SUFFIX_MAP.put("m", 43200);
  }

  public static void registerCommands(CommandFactory factory) {
    factory.addClass(GetTtlCommand.class, "-" + GET_TTL);
    factory.addClass(SetTtlCommand.class, "-" + SET_TTL);
  }

  /**
   * Implements the "-getTtl" command for the FsShell.
   */
  public static class GetTtlCommand extends FsCommand {
    public static final String NAME = GET_TTL;
    public static final String USAGE = "[-h] <path>";
    public static final String DESCRIPTION =
        "Gets the TTL of a file or directory. By default, the TTL will be "
            + "displayed in minutes\n"
            + "-h: Display the ttl value in human readable fashion.\n"
            + "<path> The file or directory.\n";

    private boolean humanReadable = false;

    @Override
    protected void processOptions(LinkedList<String> args) throws IOException {
      humanReadable = StringUtils.popOption("-h", args);

      if (args.isEmpty()) {
        throw new HadoopIllegalArgumentException("<path> is missing.");
      }
      if (args.size() > 1) {
        throw new HadoopIllegalArgumentException("Too many arguments.");
      }
    }

    @Override
    protected void processPath(PathData item) throws IOException {
      Path path = item.path;
      List<Path> allPaths = new LinkedList<Path>();
      do {
        allPaths.add(path);
        path = path.getParent();
      } while (path != null);

      int ttl = -1, ttlProperty = 0;
      Path effectTtlPath = null;
      ListIterator<Path> iter = allPaths.listIterator(allPaths.size());
      boolean isDirectory = item.fs.getFileStatus(item.path).isDirectory();
      // Check ttl from the oldest parent
      while (iter.hasPrevious()) {
        Path curPath = iter.previous();
        Map<String, byte[]> ttlValue = item.fs.getXAttrs(curPath);

        if (ttlValue.get(ATTR_NAME) == null) {
          continue;
        }
        // Override the parent's ttl
        int intValue =
            ByteBuffer.wrap(ttlValue.get(ATTR_NAME)).asIntBuffer().get();
        if (ttlValue.get(ATTR_PROPERTY) != null) {
          ttlProperty =
              ByteBuffer.wrap(ttlValue.get(ATTR_PROPERTY)).asIntBuffer().get();
        }
        if (intValue > 0) {
          ttl = intValue;
          effectTtlPath = curPath;
        }
      }

      printTtlResult(item.path, ttl, ttlProperty, effectTtlPath, isDirectory);
    }

    private void printTtlResult(Path path, int ttl, int ttlProperty,
        Path ttlPath, boolean isDirectory) {
      boolean sinceLastWrite = false, keepEmptyDir = false, keepEmptySubDir =
          false;
      if (ttlProperty != 0) {
        sinceLastWrite = ((ttlProperty & SINCELASTWRITE) != 0);
        keepEmptyDir = ((ttlProperty & KEEPEMPTYDIR) != 0);
        keepEmptySubDir = ((ttlProperty & KEEPEMPTYSUBDIR) != 0);
      }

      StringBuilder sb = new StringBuilder();
      sb.append("Effective TTL: ");
      if (ttl > 0 && ttlPath != null) {
        if (!humanReadable) {
          sb.append(ttl).append(".");
        } else {
          if (!sinceLastWrite) {
            SimpleDateFormat format =
                new SimpleDateFormat("EEEE, dd-MMM-yy HH:mm:ss z", Locale.US);
            format.setTimeZone(TimeZone.getTimeZone("UTC"));
            String formatedTtl = format.format(new Date(ttl * 60l * 1000));
            sb.append(formatedTtl).append(".");
          } else {
            int tmpTtl = ttl;
            Integer minsPerMon = SUFFIX_MAP.get("m"), minsPerWeek =
                SUFFIX_MAP.get("w"), minsPerDay = SUFFIX_MAP.get("d"), minsPerHour =
                SUFFIX_MAP.get("h");
            int months = tmpTtl / minsPerMon;
            tmpTtl -= months * minsPerMon;
            int weeks = tmpTtl / minsPerWeek;
            tmpTtl -= weeks * minsPerWeek;
            int days = tmpTtl / minsPerDay;
            tmpTtl -= days * minsPerDay;
            int hours = tmpTtl / minsPerHour;
            tmpTtl -= hours * minsPerHour;
            int mins = tmpTtl;
            boolean firstNzo = true;
            if (months > 0) {
              sb.append(months).append((months == 1) ? " month" : " months");
              firstNzo = false;
            }
            if (weeks > 0) {
              sb.append((firstNzo ? "" : " ") + weeks).append(
                  (weeks == 1) ? " week" : " weeks");
              firstNzo = false;
            }
            if (days > 0) {
              sb.append((firstNzo ? "" : " ") + days).append(
                  (days == 1) ? " day" : " days");
              firstNzo = false;
            }
            if (hours > 0) {
              sb.append((firstNzo ? "" : " ") + hours).append(
                  (hours == 1) ? " hour" : " hours");
              firstNzo = false;
            }
            if (mins > 0) {
              sb.append((firstNzo ? "" : " ") + mins).append(
                  (mins == 1) ? " minute" : " minutes");
              firstNzo = false;
            }
            sb.append(".");
          }
        }

        if (sinceLastWrite) {
          sb.append(" TTL will be applied from the last modification time.");
        }

        boolean isInherited = !ttlPath.equals(path);
        if (isDirectory) {
          boolean keptSelf =
              (keepEmptyDir && !isInherited)
                  || (keepEmptySubDir && isInherited);
          if (keptSelf && keepEmptySubDir) {
            sb.append(" Both empty dir and empty sub dir will be kept.");
          } else if (keepEmptySubDir) {
            sb.append(" Empty sub dir will be kept whereas itself will not be kept if empty.");
          } else if (keptSelf) {
            sb.append(" Empty dir will be kept whereas empty sub dir will not be kept.");
          }
        }

        if (isInherited) {
          sb.append(" Inherits from " + ttlPath + ".");
        }
      } else {
        sb.append("0.");
      }
      out.println(sb.toString());
    }
  }

  /**
   * Implements the "-setTtl" command for the FsShell.
   */
  public static class SetTtlCommand extends FsCommand {
    public static final String NAME = SET_TTL;
    public static final String USAGE =
        "[-sinceLastWrite] [-keepEmptyDir] [-keepEmptySubDir] <ttl> <path>";
    public static final String DESCRIPTION =
        "Sets the TTL for a file or directory. By default, ttl is a number "
            + "value in minutes since Epoch(00:00:00 UTC, January 1, 1970). Users can "
            + "use -sinceLastWrite option to set the epoch to be the time of the last "
            + " write of files. Users can use -keepEmptyDir to keep empty dir even if "
            + "their ttl expires. Users can use -keepEmptySubDir to keep empty sub "
            + " dirs even if their ttl expires. For convenience, users "
            + "can use 'M'(minute), 'h'(hour), 'd'(day), 'w'(week), and 'm'(month) "
            + "as a suffix to specify ttl time from now on.\n"
            + "<ttl> The ttl value to set.\n"
            + "<path> The file or directory.\n"
            + " For e.g: \"-setTtl 1h /test\" will set ttl of path /test to be 1 hour later.\n";

    private int ttl = -1;
    private boolean sinceLastWrite = false;
    private boolean keepEmptyDir = false;
    private boolean keepEmptySubDir = false;
    private int property = 0;

    @Override
    protected void processOptions(LinkedList<String> args) throws IOException {
      if (args.isEmpty()) {
        throw new HadoopIllegalArgumentException("<path> is missing.");
      }
      if (args.size() > 5) {
        throw new HadoopIllegalArgumentException("Too many arguments.");
      }

      CommandFormat cf =
          new CommandFormat(2, Integer.MAX_VALUE, "sinceLastWrite",
              "keepEmptyDir", "keepEmptySubDir");

      cf.parse(args);

      sinceLastWrite = cf.getOpt("sinceLastWrite");
      if (sinceLastWrite) {
        property |= SINCELASTWRITE;
      }
      keepEmptyDir = cf.getOpt("keepEmptyDir");
      if (keepEmptyDir) {
        property |= KEEPEMPTYDIR;
      }
      keepEmptySubDir = cf.getOpt("keepEmptySubDir");
      if (keepEmptySubDir) {
        property |= KEEPEMPTYSUBDIR;
      }

      String ttlValue = StringUtils.popFirstNonOption(args);
      ttl = parseTtlFromString(ttlValue);
    }

    private int parseTtlFromString(String value) {
      Preconditions.checkArgument(!value.isEmpty());

      int result = 0;
      Integer factor = SUFFIX_MAP.get(value.substring(value.length() - 1));
      if (factor == null) {
        result = Integer.parseInt(value);
        return result;
      } else {
        result = Integer.parseInt(value.substring(0, value.length() - 1));
        result *= factor.intValue();
        if (sinceLastWrite) {
          return result;
        } else {
          return result + (int) (System.currentTimeMillis() / 1000 / 60);
        }
      }
    }

    @Override
    protected void processPath(PathData item) throws IOException {
      if (ttl > 0) {
        if ((keepEmptyDir || keepEmptySubDir)
            && !item.fs.getFileStatus(item.path).isDirectory()) {
          throw new PathIsNotDirectoryException(item.path.toString());
        }
        byte[] value = ByteBuffer.allocate(4).putInt(ttl).array();
        byte[] propertyValue = ByteBuffer.allocate(4).putInt(property).array();
        item.fs.setXAttr(item.path, ATTR_PROPERTY, propertyValue);
        item.fs.setXAttr(item.path, ATTR_NAME, value);
      } else if (ttl == 0) {
        // 0 means to remove the ttl xattr
        item.fs.removeXAttr(item.path, ATTR_NAME);
        item.fs.removeXAttr(item.path, ATTR_PROPERTY);
      }
    }
  }
}
