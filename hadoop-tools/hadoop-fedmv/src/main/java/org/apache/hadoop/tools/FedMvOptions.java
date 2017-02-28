package org.apache.hadoop.tools;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.LinkedList;

public class FedMvOptions {
  final private static String FED_MV_MAX_MAP = "hdfs.fedmv.maxmaps";
  final private static int FED_MV_MAP_DEFAULT = 16;

  private List<Path> sourcePaths;
  private Path targetPath;
  private int maxMaps = FED_MV_MAP_DEFAULT;

  public enum FedMvOptionSwitch {
    MAX_MAPS(FED_MV_MAX_MAP, new Option("m", true,
        "Max number of concurrent maps to use for move"));

    private final String confLabel;
    private final Option option;

    FedMvOptionSwitch(String confLabel, Option option) {
      this.confLabel = confLabel;
      this.option = option;
    }

    /**
     * Get Configuration label for the option
     * 
     * @return configuration label name
     */
    public String getConfigLabel() {
      return confLabel;
    }

    /**
     * Get CLI Option corresponding to the distcp option
     * 
     * @return option
     */
    public Option getOption() {
      return option;
    }

    /**
     * Get Switch symbol
     * 
     * @return switch symbol char
     */
    public String getSwitch() {
      return option.getOpt();
    }

    @Override
    public String toString() {
      return super.name() + " {" + "confLabel='" + confLabel + '\''
          + ", option=" + option + '}';
    }

    /**
     * Helper function to add an option to hadoop configuration object
     * 
     * @param conf - Configuration object to include the option
     * @param option - Option to add
     * @param value - Value
     */
    public static void addToConf(Configuration conf, FedMvOptionSwitch option,
        String value) {
      conf.set(option.getConfigLabel(), value);
    }

    /**
     * Helper function to set an option to hadoop configuration object
     * 
     * @param conf - Configuration object to include the option
     * @param option - Option to add
     */
    public static void addToConf(Configuration conf, FedMvOptionSwitch option) {
      conf.set(option.getConfigLabel(), "true");
    }
  }

  private static final Log LOG = LogFactory.getLog(FedMvOptions.class);

  private static final Options cliOptions = new Options();

  static {
    for (FedMvOptionSwitch option : FedMvOptionSwitch.values()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Adding option " + option.getOption());
      }
      cliOptions.addOption(option.getOption());
    }
  }

  public FedMvOptions() {
    this.sourcePaths = new LinkedList<Path>();
  }

  public List<Path> getSourcePath() {
    return sourcePaths;
  }

  public Path getTargetPath() {
    return targetPath;
  }

  public int getMaxMap() {
    return maxMaps;
  }

  public static FedMvOptions parse(String args[])
      throws IllegalArgumentException {

    CommandLineParser parser = new GnuParser();

    CommandLine command;
    try {
      command = parser.parse(cliOptions, args, true);
    } catch (ParseException e) {
      throw new IllegalArgumentException("Unable to parse arguments. "
          + Arrays.toString(args), e);
    }

    FedMvOptions option = new FedMvOptions();

    String leftOverArgs[] = command.getArgs();
    if (leftOverArgs == null || leftOverArgs.length < 1) {
      throw new IllegalArgumentException("Target path not specified");
    }

    // Last Argument is the target path
    option.targetPath = new Path(leftOverArgs[leftOverArgs.length - 1].trim());

    // Copy any source paths in the arguments to the list
    for (int index = 0; index < leftOverArgs.length - 1; index++) {
      option.sourcePaths.add(new Path(leftOverArgs[index].trim()));
    }

    if (command.hasOption(FedMvOptionSwitch.MAX_MAPS.getSwitch())) {
      try {
        Integer maps =
            Integer.parseInt(getVal(command,
                FedMvOptionSwitch.MAX_MAPS.getSwitch()).trim());
        option.maxMaps = maps;
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Number of maps is invalid: "
            + getVal(command, FedMvOptionSwitch.MAX_MAPS.getSwitch()), e);
      }
    }
    return option;
  }

  private static String getVal(CommandLine command, String swtch) {
    String optionValue = command.getOptionValue(swtch);
    if (optionValue == null) {
      return null;
    } else {
      return optionValue.trim();
    }
  }

  public String[] buildDistCpArgs() {
    // 1 (map number) + 1 (target path) + number of source paths
    String[] res = new String[2 + sourcePaths.size()];
    res[0] = "-m " + maxMaps;
    for (int i = 0; i < sourcePaths.size(); i++) {
      res[i + 1] = sourcePaths.get(i).toString();
    }
    res[res.length - 1] = targetPath.toString();
    return res;
  }
}
