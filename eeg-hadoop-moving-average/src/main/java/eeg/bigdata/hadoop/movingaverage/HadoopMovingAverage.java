package eeg.bigdata.hadoop.movingaverage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import static eeg.bigdata.hadoop.movingaverage.Constants.*;


public class HadoopMovingAverage extends Configured implements Tool {

    private static Logger LOG = LoggerFactory.getLogger(HadoopMovingAverage.class);

    /**
     * Moving Average Mapper.
     */
    public static class MovingAverageMapper extends
            Mapper<LongWritable, LongWritable, LongWritable, LongWritable> {

        private Queue<LongWritable> readings;
        private int N;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            N = context.getConfiguration().getInt(N_CONFIGURATION_NAME,DEFAULT_N);
            readings = new ArrayBlockingQueue<LongWritable>(N);
        }

        @Override
        protected void map(LongWritable key, LongWritable value, Context context)
                throws IOException, InterruptedException {

            readings.add(new LongWritable(value.get()));

            if(readings.size() == N) {
                for (LongWritable reading : readings) {
                    context.write(key,reading);
                }
                readings.poll();
            }
        }
    }

    /**
     * Moving Average Reducer.
     */
    public static class MovingAverageReducer extends
            Reducer<LongWritable, LongWritable, LongWritable, DoubleWritable> {

        private int N;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            N = context.getConfiguration().getInt(N_CONFIGURATION_NAME,DEFAULT_N);
        }

        @Override
        protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0.0D;
            for (LongWritable value : values) {
                sum = sum + value.get();
            }
            sum = sum / (double) N;
            context.write(key, new DoubleWritable(sum));
        }
    }

    /**
     * Run method.
     * @param args arguments
     * @return true for sucessfull job, false otherwise.
     * @throws Exception exception.
     */
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        Job job = new Job(conf, JOB_DESCRIPTION);
        final Path inputPath = new Path(args[0]);
        final Path outputPath = new Path(args[1]);

        OpenTSDBAsciiInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setJarByClass(HadoopMovingAverage.class);

        job.setInputFormatClass(OpenTSDBAsciiInputFormat.class);
        job.setOutputFormatClass(OpenTSDBAsciiOutputFormat.class);

        job.setMapperClass(MovingAverageMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(MovingAverageReducer.class);

        boolean succ = job.waitForCompletion(true);
        if (!succ) {
            System.out.println("Job failed, exiting");
            return -1;
        }
        return 0;
    }

    /**
     * Main method.
     * @param args input arguments.
     * @throws Exception exception.
     */
    public static void main(String[] args) throws Exception {
        GenericOptionsParser gop = new GenericOptionsParser(args);
        Configuration configuration = gop.getConfiguration();

        final int N = configuration.getInt(N_CONFIGURATION_NAME, DEFAULT_N);

        final String metric = configuration.get(INPUT_METRIC_CONFIGURATION_NAME);
        final String sid = configuration.get(INPUT_SID_CONFIGURATION_NAME);
        final String rid = configuration.get(INPUT_RID_CONFIGURATION_NAME);
        final String inputFile  = "eeg-data/" + metric + "_" + sid + "_" + rid + ".txt";
        final String outputPath = "eeg-data/" + metric + "_" + sid + "_" + rid + "_MA_" + N;
        final String[] inputOutput = new String[]{inputFile, outputPath};
        checkInputOutputPaths(inputOutput, configuration);
        configuration.set(OUTPUT_RID_CONFIGURATION_NAME, rid + "-MA-" + N);

        HadoopMovingAverage hadoopMovingAverageJob = new HadoopMovingAverage();
        int res = ToolRunner.run(configuration, hadoopMovingAverageJob , inputOutput);
        System.exit(res);
    }


    private static void checkInputOutputPaths(final String paths[], final Configuration configuration)
            throws IOException {
        Path input = new Path(paths[0]);
        Path output = new Path(paths[1]);
        if (!FileSystem.get(configuration).exists(input))
            throw new FileNotFoundException("File " + paths[0] + " not found in HDFS");
        if (FileSystem.get(configuration).exists(output)) {
            LOG.info("Output path \"" + paths[1] + "\"already existing. Deleting it");
            FileSystem.get(configuration).delete(output, true);
        }
    }
}