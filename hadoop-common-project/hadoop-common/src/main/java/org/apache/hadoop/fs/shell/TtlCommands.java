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

import com.google.common.base.Preconditions;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;

/**
 * Ttl related operations.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class TtlCommands extends FsCommand {
  private static final String GET_TTL = "getTtl";
  private static final String SET_TTL = "setTtl";
  private static final String ATTR_NAME = "user.ttl";

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
        "Gets the TTL of a file or directory. By default, the TTL will be " +
        "displayed in minutes\n" +
        "-h: Display the ttl value in human readable fashion.\n" +
        "<path> The file or directory.\n";

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

      int ttl = -1;
      Path effectTtlPath = null;
      ListIterator<Path> iter = allPaths.listIterator(allPaths.size());
      // Check ttl from the oldest parent
      while (iter.hasPrevious()) {
        Path curPath = iter.previous();
        byte[] value = item.fs.getXAttr(curPath, ATTR_NAME);
        if (value == null) {
          continue;
        }

        // Override the parent's ttl
        int intValue = ByteBuffer.wrap(value).asIntBuffer().get();
        if (intValue > 0) {
          ttl = intValue;
          effectTtlPath = curPath;
        }
      }

      printTtlResult(item.path, ttl, effectTtlPath);
    }

    private void printTtlResult(Path path, int ttl, Path ttlPath) {
      if (ttl > 0 && ttlPath != null) {
        if (!humanReadable) {
          out.print("Effective TTL: " + ttl);
        } else {
          SimpleDateFormat format = new SimpleDateFormat(
              "EEEE, dd-MMM-yy HH:mm:ss z", Locale.US);
          format.setTimeZone(TimeZone.getTimeZone("UTC"));
          String formatedTtl = format.format(new Date(ttl * 60l * 1000));
          out.print("Effective TTL: " + formatedTtl);
        }

        if (!ttlPath.equals(path)) {
          out.print(" [Inherits from " + ttlPath + "]");
        }
        out.println();
      } else {
        out.println("Effective TTL: 0");
      }
    }
  }

  /**
   * Implements the "-setTtl" command for the FsShell.
   */
  public static class SetTtlCommand extends FsCommand {
    public static final String NAME = SET_TTL;
    public static final String USAGE = "<ttl> <path>";
    public static final String DESCRIPTION =
        "Sets the TTL for a file or directory. By default, ttl is a number " +
        "value in minutes since Epoch(00:00:00 UTC, January 1, 1970). Users " +
        "can use 'M'(minute), 'h'(hour), 'd'(day), 'w'(week), and 'm'(month) " +
        "as a suffix to specify relative value for convenience.\n" +
        "<ttl> The ttl value to set.\n" +
        "<path> The file or directory.\n" +
        " For e.g: \"-setTtl 1h /test\" will set ttl of path /test to be 1 hour later.\n";

    private static final Map<String, Integer> SUFFIX_MAP =
        new HashMap<String, Integer>();
    static {
      SUFFIX_MAP.put("M", 1);
      SUFFIX_MAP.put("h", 60);
      SUFFIX_MAP.put("d", 1440);
      SUFFIX_MAP.put("w", 10080);
      SUFFIX_MAP.put("m", 43200);
    }

    private int ttl = -1;

    @Override
    protected void processOptions(LinkedList<String> args) throws IOException {
      String ttlValue = StringUtils.popFirstNonOption(args);

      if (args.isEmpty()) {
        throw new HadoopIllegalArgumentException("<path> is missing.");
      }
      if (args.size() > 1) {
        throw new HadoopIllegalArgumentException("Too many arguments.");
      }
      
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
        return result + (int)(System.currentTimeMillis() / 1000 / 60);
      }
    }

    @Override
    protected void processPath(PathData item) throws IOException {
      if (ttl > 0) {
        byte[] value = ByteBuffer.allocate(4).putInt(ttl).array();
        item.fs.setXAttr(item.path, ATTR_NAME, value);
      } else if (ttl == 0) {
        // 0 means to remove the ttl xattr
        item.fs.removeXAttr(item.path, ATTR_NAME);
      }
    }
  }
}
