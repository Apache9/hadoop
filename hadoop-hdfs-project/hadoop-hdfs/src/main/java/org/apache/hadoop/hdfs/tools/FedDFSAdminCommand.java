package org.apache.hadoop.hdfs.tools;

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathExistsException;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.PathIsNotDirectoryException;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.ChmodParser;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.shell.Command;
import org.apache.hadoop.fs.shell.CommandFormat;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.util.Shell;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

abstract public class FedDFSAdminCommand extends Command {

  protected static final Log LOG = LogFactory.getLog(FedDFSAdminCommand.class);

  protected FedDFSAdminCommand(Configuration conf) {
    super();
    super.setConf(conf);
  }

  @Override
  protected List<PathData> expandArgument(String arg) throws IOException {
    PathData[] items = PathData.expandsAsGlobFromChildFs(arg, getConf());
    if (items.length == 0) {
      // it's a glob that failed to match
      throw new PathNotFoundException(arg);
    }
    return Arrays.asList(items);
  }

  @Override
  protected void postProcessPath(PathData item) throws IOException {
    out.println("Success run " + getName() + " on " + pathData2UriString(item));
  }

  @Override
  protected void processArguments(LinkedList<PathData> args)
      throws IOException {
    for (PathData arg : args) {
      try {
        out.println("Running " + getName() + " on " + pathData2UriString(arg));
        processArgument(arg);
      } catch (IOException e) {
        displayError(e);
      }
    }
  }

  @Override
  protected void processNonexistentPath(PathData item) throws IOException {
    throw new PathNotFoundException(pathData2UriString(item));
  }

  @Override
  protected void run(Path path) throws IOException {
    throw new RuntimeException("not supposed to get here");
  }

  void printUris(URI[] uris) {
    StringBuilder msg = new StringBuilder();
    msg.append("Success run command to uri:\n");
    for (URI uri : uris) {
      msg.append(uri);
      msg.append("\n");
    }
    out.println(msg.toString());
  }

  String pathData2UriString(PathData item) {
    return item.fs.getScheme() + "://" + item.fs.getUri().getAuthority() + item
        .toString();
  }

  /**
   * Same as {@link org.apache.hadoop.fs.FsShellPermissions.Chown},
   * just do it at all cluster under the federation.
   */
  protected static class FedchownCommand extends FedDFSAdminCommand {

    static private String allowedChars =
        Shell.WINDOWS ? "[-_./@a-zA-Z0-9 ]" : "[-_./@a-zA-Z0-9]";

    private static final String NAME = "fedchown";
    public static final String USAGE = "[-R] [OWNER][:[GROUP]] PATH...";
    public static final String DESCRIPTION =
        "Changes owner and group of a file in all clusters of federation. ";

    ///allows only "allowedChars" above in names for owner and group
    static private final Pattern chownPattern = Pattern.compile(
        "^\\s*(" + allowedChars + "+)?([:](" + allowedChars + "*))?\\s*$");

    private String owner = null;
    private String group = null;

    protected FedchownCommand(Configuration conf) {
      super(conf);
    }

    public static boolean matches(String cmd) {
      return ("-" + NAME).equals(cmd);
    }

    @Override
    public String getCommandName() {
      return NAME;
    }

    @Override
    protected void processOptions(LinkedList<String> args) throws IOException {
      CommandFormat cf = new CommandFormat(2, Integer.MAX_VALUE, "R");
      cf.parse(args);
      setRecursive(cf.getOpt("R"));
      parseOwnerGroup(args.removeFirst());
    }

    void parseOwnerGroup(String ownerStr) {
      Matcher matcher = chownPattern.matcher(ownerStr);
      if (!matcher.matches()) {
        throw new IllegalArgumentException("'" + ownerStr
            + "' does not match expected pattern for [owner][:group].");
      }
      owner = matcher.group(1);
      group = matcher.group(3);
      if (group != null && group.length() == 0) {
        group = null;
      }
      if (owner == null && group == null) {
        throw new IllegalArgumentException(
            "'" + ownerStr + "' does not specify owner or group.");
      }
    }

    @Override
    protected void processPath(PathData item) throws IOException {
      //Should we do case insensitive match?
      String newOwner =
          (owner == null || owner.equals(item.stat.getOwner())) ? null : owner;
      String newGroup =
          (group == null || group.equals(item.stat.getGroup())) ? null : group;

      if (newOwner != null || newGroup != null) {
        try {
          item.fs.setOwner(item.path, newOwner, newGroup);
        } catch (IOException e) {
          LOG.debug("Error changing ownership of " + item, e);
          throw new IOException(
              "changing ownership of '" + item + "': " + e.getMessage());
        }
      }
    }
  }

  /**
   * Same as {@link org.apache.hadoop.fs.FsShellPermissions.Chmod},
   * just do it at all cluster under the federation.
   */
  protected static class FedchmodCommand extends FedDFSAdminCommand {
    private static final String NAME = "fedchmod";
    public static final String USAGE =
        "[-R] <MODE[,MODE]... | OCTALMODE> PATH...";
    public static final String DESCRIPTION =
        "Changes permissions of a file in all clusters of federation.";

    private ChmodParser pp;

    protected FedchmodCommand(Configuration conf) {
      super(conf);
    }

    public static boolean matches(String cmd) {
      return ("-" + NAME).equals(cmd);
    }

    @Override
    public String getCommandName() {
      return NAME;
    }

    @Override
    protected void processOptions(LinkedList<String> args) throws IOException {
      CommandFormat cf = new CommandFormat(2, Integer.MAX_VALUE, "R", null);
      cf.parse(args);
      setRecursive(cf.getOpt("R"));

      String modeStr = args.removeFirst();
      try {
        pp = new ChmodParser(modeStr);
      } catch (IllegalArgumentException iea) {
        // TODO: remove "chmod : " so it's not doubled up in output, but it's
        // here for backwards compatibility...
        throw new IllegalArgumentException("chmod : mode '" + modeStr
            + "' does not match the expected pattern.");
      }
    }

    @Override
    protected void processPath(PathData item) throws IOException {
      short newperms = pp.applyNewPermission(item.stat);
      if (item.stat.getPermission().toShort() != newperms) {
        try {
          item.fs.setPermission(item.path, new FsPermission(newperms));
        } catch (IOException e) {
          LOG.debug("Error changing permissions of " + item, e);
          throw new IOException(
              "changing permissions of '" + item + "': " + e.getMessage());
        }
      }
    }

    @Override
    protected List<PathData> expandArgument(String arg) throws IOException {
      PathData[] items = PathData.expandsAsGlobFromChildFs(arg, getConf());
      if (items.length == 0) {
        // it's a glob that failed to match
        throw new PathNotFoundException(arg);
      }
      return Arrays.asList(items);
    }
  }

  /**
   * Same as {@link org.apache.hadoop.fs.shell.AclCommands.SetfaclCommand},
   * just do it at all cluster under the federation.
   */
  protected static class FedSetfaclCommand extends FedDFSAdminCommand {
    private static final String NAME = "fedsetfacl";
    public static String USAGE = "[-R] [{-b|-k} {-m|-x <acl_spec>} <path>]"
        + "|[--set <acl_spec> <path>]";
    public static String DESCRIPTION = "Sets Access Control Lists (ACLs)"
        + " of files and directories in all clusters of federation.";

    CommandFormat cf =
        new CommandFormat(0, Integer.MAX_VALUE, "b", "k", "R", "m", "x",
            "-set");
    List<AclEntry> aclEntries = null;
    List<AclEntry> accessAclEntries = null;

    protected FedSetfaclCommand(Configuration conf) {
      super(conf);
    }

    public static boolean matches(String cmd) {
      return ("-" + NAME).equals(cmd);
    }

    @Override
    public String getCommandName() {
      return NAME;
    }

    @Override
    protected void processOptions(LinkedList<String> args) throws IOException {
      cf.parse(args);
      setRecursive(cf.getOpt("R"));
      // Mix of remove and modify acl flags are not allowed
      boolean bothRemoveOptions = cf.getOpt("b") && cf.getOpt("k");
      boolean bothModifyOptions = cf.getOpt("m") && cf.getOpt("x");
      boolean oneRemoveOption = cf.getOpt("b") || cf.getOpt("k");
      boolean oneModifyOption = cf.getOpt("m") || cf.getOpt("x");
      boolean setOption = cf.getOpt("-set");
      if ((bothRemoveOptions || bothModifyOptions) || (oneRemoveOption
          && oneModifyOption) || (setOption && (oneRemoveOption
          || oneModifyOption))) {
        throw new HadoopIllegalArgumentException(
            "Specified flags contains both remove and modify flags");
      }

      // Only -m, -x and --set expects <acl_spec>
      if (oneModifyOption || setOption) {
        if (args.size() < 2) {
          throw new HadoopIllegalArgumentException("<acl_spec> is missing");
        }
        aclEntries = AclEntry.parseAclSpec(args.removeFirst(), !cf.getOpt("x"));
      }

      if (args.isEmpty()) {
        throw new HadoopIllegalArgumentException("<path> is missing");
      }
      if (args.size() > 1) {
        throw new HadoopIllegalArgumentException("Too many arguments");
      }

      // In recursive mode, save a separate list of just the access ACL entries.
      // Only directories may have a default ACL.  When a recursive operation
      // encounters a file under the specified path, it must pass only the
      // access ACL entries.
      if (isRecursive() && (oneModifyOption || setOption)) {
        accessAclEntries = Lists.newArrayList();
        for (AclEntry entry : aclEntries) {
          if (entry.getScope() == AclEntryScope.ACCESS) {
            accessAclEntries.add(entry);
          }
        }
      }
    }

    @Override
    protected void processPath(PathData item) throws IOException {
      if (cf.getOpt("b")) {
        item.fs.removeAcl(item.path);
      } else if (cf.getOpt("k")) {
        item.fs.removeDefaultAcl(item.path);
      } else if (cf.getOpt("m")) {
        List<AclEntry> entries = getAclEntries(item);
        if (!entries.isEmpty()) {
          item.fs.modifyAclEntries(item.path, entries);
        }
      } else if (cf.getOpt("x")) {
        List<AclEntry> entries = getAclEntries(item);
        if (!entries.isEmpty()) {
          item.fs.removeAclEntries(item.path, entries);
        }
      } else if (cf.getOpt("-set")) {
        List<AclEntry> entries = getAclEntries(item);
        if (!entries.isEmpty()) {
          item.fs.setAcl(item.path, entries);
        }
      }
    }

    /**
     * Returns the ACL entries to use in the API call for the given path.  For a
     * recursive operation, returns all specified ACL entries if the item is a
     * directory or just the access ACL entries if the item is a file.  For a
     * non-recursive operation, returns all specified ACL entries.
     *
     * @param item PathData path to check
     * @return List<AclEntry> ACL entries to use in the API call
     */
    private List<AclEntry> getAclEntries(PathData item) {
      if (isRecursive()) {
        return item.stat.isDirectory() ? aclEntries : accessAclEntries;
      } else {
        return aclEntries;
      }
    }
  }

  protected static class FedSetQuotaCommand extends FedDFSAdminCommand {

    private static final String NAME = "fedsetquota";
    public static final String USAGE =
        "-" + NAME + " <quota> <dirname>...<dirname>";
    public static final String DESCRIPTION =
        "-setQuota <quota> <dirname>...<dirname>: "
            + "Set the quota <quota> for each directory <dirName>.\n"
            + "\t\tThe directory quota is a long integer that puts a hard limit\n"
            + "\t\ton the number of names in the directory tree\n"
            + "\t\tFor each directory, attempt to set the quota. An error will be reported if\n"
            + "\t\t1. N is not a positive integer, or\n"
            + "\t\t2. User is not an administrator, or\n"
            + "\t\t3. The directory does not exist or is a file.\n"
            + "\t\tNote: A quota of 1 would force the directory to remain empty.\n";

    CommandFormat cf = new CommandFormat(2, Integer.MAX_VALUE);

    private long quota = -1;

    protected FedSetQuotaCommand(Configuration conf) {
      super(conf);
    }

    public static boolean matches(String cmd) {
      return ("-" + NAME).equals(cmd);
    }

    @Override
    public String getCommandName() {
      return NAME;
    }

    @Override
    protected void processOptions(LinkedList<String> args) throws IOException {
      cf.parse(args);
      quota = Long.parseLong(args.remove(0));
    }

    @Override
    protected void processPath(PathData item) throws IOException {
      if (item.fs instanceof DistributedFileSystem) {
        ((DistributedFileSystem) item.fs)
            .setQuota(item.path, quota, HdfsConstants.QUOTA_DONT_SET);
      } else {
        throw new IOException(
            "Can't get DistributedFileSystem instance for uri: ");
      }
    }
  }

  protected static class FedMkdirs extends FedDFSAdminCommand {
    public static final String NAME = "fedmkdirs";
    public static final String USAGE = "[-p] <path> ...";
    public static final String DESCRIPTION =
        "Create a directory in specified location in all clusters of federation."
            + "-p: Do not fail if the directory already exists";

    private boolean createParents;

    protected FedMkdirs(Configuration conf) {
      super(conf);
    }

    public static boolean matches(String cmd) {
      return ("-" + NAME).equals(cmd);
    }

    @Override
    public String getCommandName() {
      return NAME;
    }

    @Override
    protected void processOptions(LinkedList<String> args) {
      CommandFormat cf = new CommandFormat(1, Integer.MAX_VALUE, "p");
      cf.parse(args);
      createParents = cf.getOpt("p");
    }

    @Override
    protected void processPath(PathData item) throws IOException {
      if (item.stat.isDirectory()) {
        if (!createParents) {
          throw new PathExistsException(pathData2UriString(item));
        }
      } else {
        throw new PathIsNotDirectoryException(pathData2UriString(item));
      }
    }

    @Override
    protected void processNonexistentPath(PathData item) throws IOException {
      // check if parent exists. this is complicated because getParent(a/b/c/) returns a/b/c, but
      // we want a/b
      if (!item.fs.exists(new Path(item.path.toString()).getParent())
          && !createParents) {
        throw new PathNotFoundException(pathData2UriString(item));
      }
      if (!item.fs.mkdirs(item.path)) {
        throw new PathIOException(pathData2UriString(item));
      }
    }
  }
}