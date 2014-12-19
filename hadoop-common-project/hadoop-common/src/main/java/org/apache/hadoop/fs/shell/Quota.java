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
import java.util.Arrays;
import java.util.LinkedList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.QuotaSummary;
import org.apache.hadoop.fs.FsShell;

/**
 * Show quota summary of direcotry with quota set
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class Quota extends FsCommand {
  /**
   * Register the names for the quota command
   * @param factory the command factory that will instantiate this class
   */
  public static void registerCommands(CommandFactory factory) {
    factory.addClass(Quota.class, "-quota");
  }

  public static final String NAME = "Quota";
  public static final String USAGE = "<path> ...";
  public static final String DESCRIPTION = 
    "Show the quota of directory with quota set.\n" +
    "The output columns are:\n" +
    "QUOTA USED_QUOTA REMAINING_QUOTA SPACE_QUOTA " +
    "USED_SPACE REMAINING_SPACE FILE_NAME";

  @Override
  protected void processOptions(LinkedList<String> args) {
    CommandFormat cf = new CommandFormat(1, Integer.MAX_VALUE);
    cf.parse(args);
  }

  @Override
  protected void processPath(PathData src) throws IOException {
    QuotaSummary summary = src.fs.getQuotaSummary(src.path);
    out.println(summary.toString() + src);
  }
}
