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
package org.apache.hadoop.examples;

import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BytesWritable; 
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.map.RegexMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/* Extracts matching regexs from input files and counts them. */
public class GrepSeq extends Configured implements Tool {
  private GrepSeq() {}                               // singleton

  public static String PATTERN = "mapreduce.mapper.regex";
  public static String GROUP = "mapreduce.mapper.regexmapper..group";
public static class GrepSeqMapper extends Mapper<BytesWritable, BytesWritable, Text, LongWritable> {

  private Pattern pattern;
  private int group;

  public void setup(Context context) {
    Configuration conf = context.getConfiguration();
    pattern = Pattern.compile(conf.get(PATTERN));
    group = conf.getInt(GROUP, 0);
  }

  public void map(BytesWritable key, BytesWritable value,
                  Context context)
    throws IOException, InterruptedException {
    String text = new String(value.getBytes(), 0, value.getLength(), "UTF-8"); 
    Matcher matcher = pattern.matcher(text);
    while (matcher.find()) {
      context.write(new Text(text), new LongWritable(1));
    }
  }
}

  public int run(String[] args) throws Exception {
    if (args.length < 3) {
      System.out.println("GrepSeq <inDir> <outDir> <regex> [<group>]");
      ToolRunner.printGenericCommandUsage(System.out);
      return 2;
    }

    Path tempDir =
      new Path(args[1] + "grep-temp-"+
          Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    Configuration conf = getConf();
    conf.set(PATTERN, args[2]);
    if (args.length == 4)
      conf.set(GROUP, args[3]);

    Job grepJob = new Job(conf);
    grepJob.setJarByClass(GrepSeq.class);
    
    try {
      
      grepJob.setJobName("grep-search");

      FileInputFormat.setInputPaths(grepJob, args[0]);
      grepJob.setInputFormatClass(SequenceFileInputFormat.class);

      grepJob.setMapperClass(GrepSeqMapper.class);

      grepJob.setCombinerClass(LongSumReducer.class);
      grepJob.setReducerClass(LongSumReducer.class);

      FileOutputFormat.setOutputPath(grepJob, new Path(args[1]));
      grepJob.setOutputFormatClass(TextOutputFormat.class);
      grepJob.setOutputKeyClass(Text.class);
      grepJob.setOutputValueClass(LongWritable.class);

      grepJob.waitForCompletion(true);

    } finally {
      // nothing to clean-up 
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new GrepSeq(), args);
    System.exit(res);
  }

}
