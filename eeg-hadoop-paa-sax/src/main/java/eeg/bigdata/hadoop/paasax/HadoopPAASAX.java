package eeg.bigdata.hadoop.paasax;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import static eeg.bigdata.hadoop.paasax.Constants.*;


/**
 * The famous MapReduce word count example for Hadoop.
 */
public class HadoopPAASAX extends Configured implements Tool {

    private static Logger LOG = LoggerFactory.getLogger(HadoopPAASAX.class);

    public static class PAASAXMapper extends
            Mapper<LongWritable, LongWritable, LongWritable, Text> {

        private Queue<LongWritable> timestamps;
        private Queue<LongWritable> readings;
        private int N;
        private int L;
        private double maxPaa;
        private double minPaa;
        private double[] regions;
        private boolean isSAX;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            N = context.getConfiguration().getInt(N_CONFIGURATION_NAME, DEFAULT_N);
            L = context.getConfiguration().getInt(L_CONFIGURATION_NAME, DEFAULT_L);
            readings = new ArrayBlockingQueue<LongWritable>(N);
            timestamps = new ArrayBlockingQueue<LongWritable>(N);
            isSAX = context.getConfiguration().getBoolean(IS_SAX_CONFIGURATION_NAME, false);
            if(isSAX) {
                maxPaa = context.getConfiguration().getFloat(PAA_MAX_VALUE_CONFIGURATION_NAME,DEFAULT_PAA_MAX_VALUE);
                minPaa = context.getConfiguration().getFloat(PAA_MIN_VALUE_CONFIGURATION_NAME,
                    DEFAULT_PAA_MIN_VALUE);
                regions = new double[SAX_ALPHABET.length + 1];
                setupRegions();
            }
        }

        @Override
        protected void map(LongWritable key, LongWritable value, Context context)
                throws IOException, InterruptedException {

            timestamps.add(new LongWritable(key.get()));
            readings.add(new LongWritable(value.get()));

            if (readings.size() == N) {
                double sum = 0.0D;
                do {
                    sum += (double) readings.poll().get();
                } while (!readings.isEmpty());
                double paa = (double) N / (double) L * sum;
                do {
                    if(isSAX) {
                        context.write(timestamps.poll(), new Text(findSaxSymbol(paa)));
                    } else {
                        context.write(timestamps.poll(), new Text(Double.toString(paa)));
                    }
                } while (!timestamps.isEmpty());
            }
        }

        private String findSaxSymbol(final double paa) throws InterruptedException {
            for (int i = 0; i < SAX_ALPHABET.length; i++) {
                if (paa > regions[i] && paa <= regions[i + 1]) return SAX_ALPHABET[i];
            }
            throw new InterruptedException("paa( " + paa + " ) cannot be outside (Double.NEGATIVE_INFINITY (" +
                    (paa < Double.NEGATIVE_INFINITY) + "),Double.POSITIVE_INFINITY (" +
                    (paa > Double.POSITIVE_INFINITY) + ").");
        }

        private void setupRegions() {
            regions[0] = Double.NEGATIVE_INFINITY;
            regions[SAX_ALPHABET.length] = Double.POSITIVE_INFINITY;
            double step = (maxPaa - minPaa) / SAX_ALPHABET.length;
            for (int i = 1; i < SAX_ALPHABET.length; i++) {
                regions[i] = minPaa + i * step;
            }
        }
    }
    @Override
    public int run(String[] args) throws Exception {

        Configuration configuration = getConf();

        Job job = new Job(configuration, JOB_DESCRIPTION);
        final Path inputPath = new Path(args[0]);
        final Path outputPath = new Path(args[1]);

        OpenTSDBAsciiInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setJarByClass(HadoopPAASAX.class);

        job.setMapperClass(PAASAXMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(OpenTSDBAsciiInputFormat.class);
        if (configuration.getBoolean(IS_SAX_CONFIGURATION_NAME, false)) {
            job.setOutputFormatClass(TextOutputFormat.class);
        } else {
            job.setOutputFormatClass(OpenTSDBAsciiOutputFormat.class);
        }
        job.setNumReduceTasks(0);

        boolean succ = job.waitForCompletion(true);
        if (!succ) {
            System.out.println("Job failed, exiting");
            return -1;
        }
        return 0;
    }


    public static void main(String[] args) throws Exception {
        GenericOptionsParser gop = new GenericOptionsParser(args);
        Configuration configuration = gop.getConfiguration();

        final int N = configuration.getInt(N_CONFIGURATION_NAME, DEFAULT_N);
        final int L = configuration.getInt(L_CONFIGURATION_NAME, DEFAULT_L);
        final boolean isSAX = configuration.getBoolean(IS_SAX_CONFIGURATION_NAME, false);

        final String metric = configuration.get(INPUT_METRIC_CONFIGURATION_NAME);
        final String sid = configuration.get(INPUT_SID_CONFIGURATION_NAME);
        final String rid = configuration.get(INPUT_RID_CONFIGURATION_NAME);
        final String inputFile  = "eeg-data/" + metric + "_" + sid + "_" + rid + ".txt";
        final String outputPath = "eeg-data/" + metric + "_" + sid + "_" + rid + ((isSAX)?"_sax_":"_paa_") + N;
        final String[] inputOutput = new String[]{inputFile, outputPath};
        checkInputOutputPaths(inputOutput, configuration);
        configuration.set(OUTPUT_RID_CONFIGURATION_NAME, rid + "-PAA-" + N);
        HadoopPAASAX hadoopPAASAX = new HadoopPAASAX();

        LOG.info("Running Hadoop " + (isSAX ? "SAX" : "PAA"));
        LOG.info("PAA N : " + N);
        LOG.info("Timeseries length : " + L);
        LOG.info("Input file  : " + inputFile);
        LOG.info("Output path : " + outputPath);
        int res = ToolRunner.run(configuration, hadoopPAASAX, inputOutput);
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
