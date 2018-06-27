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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.Trash;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Classes that delete paths in trash for federation
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DeleteTrash extends FsCommand {
  public static void registerCommands(CommandFactory factory) {
    factory.addClass(DeleteTrash.class, "-rmTrash");
  }

  public static final String NAME = "rmTrash";
  public static final String USAGE = "<src>";
  public static final String DESCRIPTION =
    "Delete files in trash.";

  private void deleteFile(FileSystem fs, Path path) throws IOException  {
    boolean success = Trash.moveToAppropriateTrash(fs, path, getConf());
    if (!success) {
      if (!fs.delete(path, true)) {
        throw new IOException("Cannot delete trash at " + fs.getUri() + path.toString());
      } else {
        out.println("Successfully delete " + fs.getUri() + path.toString());
        return;
      }
    }
  }

  private boolean isInTrash(Path path) {
    String str = path.toString();
    int pos = str.indexOf(".Trash");
    if (pos < 0) {
      return false;
    }
    Path pathUser = new Path(str.substring(0, pos-1));
    if (pathUser.getParent().toString().endsWith("/user")) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  protected void processPath(PathData src) throws IOException {
    if (isInTrash(src.path)) {
      Path noSchemaSrc = Path.getPathWithoutSchemeAndAuthority(src.path);
      FileSystem[] childrenFs = src.fs.getChildFileSystems();
      if (childrenFs == null) {
        if (src.fs.exists(noSchemaSrc)) {
          deleteFile(src.fs, noSchemaSrc);
        }
      } else {
        for (FileSystem childFs : childrenFs) {
          if (childFs.exists(noSchemaSrc)) {
            deleteFile(childFs, noSchemaSrc);
          }
        }
      }
    } else {
      throw new PathIOException("Input path is not in any trash");
    }
  }
}