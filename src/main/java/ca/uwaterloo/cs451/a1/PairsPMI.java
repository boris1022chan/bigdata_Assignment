/**
 * Bespin: reference implementations of "big data" algorithms
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ca.uwaterloo.cs451.a1;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.pair.PairOfStrings;
import tl.lin.data.pair.PairOfFloatInt;

import java.io.IOException;
import java.util.*;
import java.lang.Math;
import java.lang.Float;
import java.lang.Integer;

/**
 * <p>
 * Implementation of the "pairs" algorithm for computing co-occurrence matrices from a large text
 * collection. This algorithm is described in Chapter 3 of "Data-Intensive Text Processing with 
 * MapReduce" by Lin &amp; Dyer, as well as the following paper:
 * </p>
 *
 * <blockquote>Jimmy Lin. <b>Scalable Language Processing Algorithms for the Masses: A Case Study in
 * Computing Word Co-occurrence Matrices with MapReduce.</b> <i>Proceedings of the 2008 Conference
 * on Empirical Methods in Natural Language Processing (EMNLP 2008)</i>, pages 419-428.</blockquote>
 *
 * @author Jimmy Lin
 */
public class PairsPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(PairsPMI.class);
  public static enum LINENUM {
    counter
  }

  private static final class MyMapper extends Mapper<LongWritable, Text, PairOfStrings, PairOfFloatInt> {
    private static final PairOfStrings PAIR = new PairOfStrings();
    private static final PairOfFloatInt ONE = new PairOfFloatInt(.0f, 1);



    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      List<String> tokens = Tokenizer.tokenize(value.toString());
      Set<String> hs = new HashSet<>();
      hs.addAll(tokens.subList(0, Math.min(tokens.size(), 40)));
      tokens.clear();
      tokens.addAll(hs);

      context.getCounter(LINENUM.counter).increment(1);

      for (int i = 0; i < tokens.size(); i++) {
        for (int j = 0; j < tokens.size(); j++) {
          if (i == j) continue;
          PAIR.set(tokens.get(i), tokens.get(j));
          context.write(PAIR, ONE);
        }
        PAIR.set(tokens.get(i), "*");
        context.write(PAIR, ONE);
      }
    }
  }

  private static final class MyCombiner extends Reducer<PairOfStrings, PairOfFloatInt, PairOfStrings, PairOfFloatInt> {
    private static final PairOfFloatInt SUM = new PairOfFloatInt();

    @Override
    public void reduce(PairOfStrings key, Iterable<PairOfFloatInt> values, Context context)
            throws IOException, InterruptedException {
      int sum = 0;
      Iterator<PairOfFloatInt> iter = values.iterator();
      while (iter.hasNext()) {
        sum += iter.next().getRightElement();
      }
      SUM.set(0.0f, sum);
      context.write(key, SUM);
    }
  }




  private static final class MyReducer extends
          Reducer<PairOfStrings, PairOfFloatInt, PairOfStrings, PairOfFloatInt> {

    private static final PairOfStrings KEY = new PairOfStrings();
    private static final PairOfFloatInt VALUE = new PairOfFloatInt();
    private float marginal = 0.0f;
    private int threshold = 1;


    @Override
    public void setup(Context context) {
      threshold = context.getConfiguration().getInt("threshold", 1);
    }

    @Override
    public void reduce(PairOfStrings key, Iterable<PairOfFloatInt> values, Context context)
            throws IOException, InterruptedException {
      Iterator<PairOfFloatInt> iter = values.iterator();
      float sum = 0.0f;
      while (iter.hasNext()) {
        sum += iter.next().getRightElement();
      }

      if (key.getRightElement().equals("*")) {
        VALUE.set(1.0f, (int) sum);
        context.write(key, VALUE);
        marginal = sum;
      } else {
        if (sum >= threshold) {
          KEY.set(key.getRightElement(), key.getLeftElement());
          VALUE.set((float) sum / marginal, (int) sum);
          context.write(KEY, VALUE);
        }
      }
    }
  }


  private static final class MyPartitioner extends Partitioner<PairOfStrings, PairOfFloatInt> {
    @Override
    public int getPartition(PairOfStrings key, PairOfFloatInt value, int numReduceTasks) {
      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }


    private static final class MyMapper2 extends Mapper<LongWritable, Text, PairOfStrings, PairOfFloatInt> {
      private static final PairOfStrings PAIR = new PairOfStrings();
      private static final PairOfFloatInt VALUE = new PairOfFloatInt();

      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {

        List<String> tokens = new ArrayList<>();

        String[] splits = value.toString().split("\\s+|\\(|,\\s+|\\)|\r|\n");
        for (int i = 0; i < splits.length; i++){
          if (splits[i].length() != 0){
            tokens.add(splits[i]);
          }
        }

        if (tokens.size() == 4) {
          PAIR.set(tokens.get(0), tokens.get(1));
          VALUE.set(Float.parseFloat(tokens.get(2)), Integer.parseInt(tokens.get(3)));
          context.write(PAIR, VALUE);
        }
      }
    }

    private static final class MyReducer2 extends Reducer<PairOfStrings, PairOfFloatInt, PairOfStrings, PairOfFloatInt> {
      private static final PairOfStrings KEY = new PairOfStrings();
      private static final PairOfFloatInt VALUE = new PairOfFloatInt();
      private float marginal = 0.0f;
      private int threshold = 1;
      private int lineCount = 1;

      @Override
      public void setup(Context context) {
        threshold = context.getConfiguration().getInt("threshold", 1);
	lineCount = context.getConfiguration().getInt("lineCount", 1);
      }

      @Override
      public void reduce(PairOfStrings key, Iterable<PairOfFloatInt> values, Context context)
              throws IOException, InterruptedException {
        float sum = 0.0f;
        float pmi = 0.0f;
        Iterator<PairOfFloatInt> iter = values.iterator();
        while (iter.hasNext()) {
	    PairOfFloatInt tem = iter.next();
	    sum += tem.getRightElement();
	    pmi += tem.getLeftElement();
	}

        if (key.getRightElement().equals("*")) {
          marginal = sum;
        } else {
          if (sum >= threshold) {
            VALUE.set((float) Math.log10(lineCount * pmi / marginal), (int) sum);
            context.write(key, VALUE);
          }
        }
      }
    }





  /**
   * Creates an instance of this tool.
   */
  private PairsPMI() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
    int numReducers = 1;

    @Option(name = "-threshold", metaVar = "[num]", usage = "threshold of co-occurrence")
    int threshold = 1;
  }

  /**
   * Runs this tool.
   */
  @Override
  public int run(String[] argv) throws Exception {
    final Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info("Tool name: " + PairsPMI.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - num reducers: " + args.numReducers);
    LOG.info(" - threshold: " + args.threshold);

    //=======================================JOB1==========================================//
    Job job1 = Job.getInstance(getConf());
    job1.setJobName(PairsPMI.class.getSimpleName());
    job1.setJarByClass(PairsPMI.class);

    job1.setNumReduceTasks(args.numReducers);
    job1.getConfiguration().setInt("threshold", args.threshold);

    job1.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
    job1.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    job1.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    job1.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    job1.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

    FileInputFormat.setInputPaths(job1, new Path(args.input));
    FileOutputFormat.setOutputPath(job1, new Path(args.output + "_JOB1"));

    job1.setMapOutputKeyClass(PairOfStrings.class);
    job1.setMapOutputValueClass(PairOfFloatInt.class);
    job1.setOutputKeyClass(PairOfStrings.class);
    job1.setOutputValueClass(PairOfFloatInt.class);

    job1.setOutputFormatClass(TextOutputFormat.class);

    job1.setMapperClass(MyMapper.class);
    job1.setCombinerClass(MyCombiner.class);
    job1.setReducerClass(MyReducer.class);
    job1.setPartitionerClass(MyPartitioner.class);

    // Delete the output directory if it exists already.
    Path outputDir1 = new Path(args.output + "_JOB1");
    FileSystem.get(getConf()).delete(outputDir1, true);

    long startTime = System.currentTimeMillis();
    job1.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    long lineCount = job1.getCounters().findCounter(PairsPMI.LINENUM.counter).getValue();

//=======================================JOB2==========================================//
    Job job2 = Job.getInstance(getConf());
    job2.setJobName(PairsPMI.class.getSimpleName());
    job2.setJarByClass(PairsPMI.class);

    job2.setNumReduceTasks(args.numReducers);
    job2.getConfiguration().setInt("threshold", args.threshold);
    job2.getConfiguration().setInt("lineCount", (int) lineCount);

    job2.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
    job2.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    job2.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    job2.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    job2.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

    FileInputFormat.setInputPaths(job2, new Path(args.output + "_JOB1"));
    FileOutputFormat.setOutputPath(job2, new Path(args.output));

    job2.setMapOutputKeyClass(PairOfStrings.class);
    job2.setMapOutputValueClass(PairOfFloatInt.class);
    job2.setOutputKeyClass(PairOfStrings.class);
    job2.setOutputValueClass(PairOfFloatInt.class);

    job2.setOutputFormatClass(TextOutputFormat.class);

    job2.setMapperClass(MyMapper2.class);
    job2.setReducerClass(MyReducer2.class);
    job2.setPartitionerClass(MyPartitioner.class);

    // Delete the output directory if it exists already.
    Path outputDir2 = new Path(args.output);
    FileSystem.get(getConf()).delete(outputDir2, true);

    startTime = System.currentTimeMillis();
    job2.waitForCompletion(true);
    System.out.println("Job2 Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    FileSystem.get(getConf()).delete(outputDir1, true);
    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new PairsPMI(), args);
  }
}
