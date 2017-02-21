package org.apache.hadoop.fs.shell;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathExistsException;
import org.apache.hadoop.fs.PathIOException;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RestoreTrash extends FsCommand {
  public static void registerCommands(CommandFactory factory) {
    factory.addClass(RestoreTrash.class, "-restoreTrash");
  }

  public static final String NAME = "restoreTrash";
  public static final String USAGE = "<src>";
  public static final String DESCRIPTION =
      "Move files in trash to their original locations ";

  private String getTrashOrigin(String pathStr) {
    int trashIdx = pathStr.indexOf(".Trash");
    if (trashIdx == -1) {
      return null;
    }
    String tmpStr = pathStr.substring(trashIdx);
    int firstSepAfterTrash = tmpStr.indexOf("/");
    if (firstSepAfterTrash == -1) {
      return null;
    }
    int secondSepAfterTrash = tmpStr.indexOf("/", firstSepAfterTrash + 1);
    if (secondSepAfterTrash == -1) {
      return null;
    }
    return tmpStr.substring(secondSepAfterTrash);
  }

  @Override
  protected void processPath(PathData src) throws IOException {
    if (src.path.toString().contains(".Trash")) {
      Path noSchemaSrc = Path.getPathWithoutSchemeAndAuthority(src.path);
      String srcPath = noSchemaSrc.toString();
      String tgtPath = getTrashOrigin(srcPath);
      if (tgtPath == null) {
        throw new PathIOException("Incorrect input path");
      }
      assert (tgtPath.startsWith("/"));
      Path noSchemaTgt = new Path(tgtPath);
      FileSystem[] childrenFs = src.fs.getChildFileSystems();
      if (childrenFs == null) {
        if (!src.fs.rename(noSchemaSrc, noSchemaTgt)) {
          throw new PathIOException("Cannot restore trash at "
              + noSchemaSrc.toString());
        }
      } else {
        for (FileSystem childFs : childrenFs) {
          if (childFs.exists(noSchemaSrc)) {
            if (!childFs.rename(noSchemaSrc, noSchemaTgt)) {
              // we have no way to know the actual error...
              throw new PathIOException("Cannot restore trash at "
                  + noSchemaSrc.toString());
            } else {
              out.println("Successfully restored " + src.path.toString());
              return;
            }
          }
        }
      }
    }
    throw new PathIOException("Input path is not in any trash");
  }
}
