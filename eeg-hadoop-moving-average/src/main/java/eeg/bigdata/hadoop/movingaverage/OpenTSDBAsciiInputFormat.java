package eeg.bigdata.hadoop.movingaverage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

/**
 * Class for OpenTSDB-ascii files.
 */
public class OpenTSDBAsciiInputFormat extends FileInputFormat<LongWritable, LongWritable> {

    @Override
    public RecordReader<LongWritable, LongWritable> createRecordReader(
            InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new OpenTSDBAsciiReader();
    }

    protected class OpenTSDBAsciiReader extends RecordReader<LongWritable, LongWritable> {

        private LineReader in;
        private LongWritable key;
        private LongWritable value;
        private long start = 0;
        private long end = 0;
        private long pos = 0;
        private int maxLineLength;

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext context)
                throws IOException, InterruptedException {
            FileSplit split = (FileSplit) inputSplit;
            final Path file = split.getPath();
            Configuration conf = context.getConfiguration();
            this.maxLineLength = conf.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
            FileSystem fs = file.getFileSystem(conf);
            start = split.getStart();
            end = start + split.getLength();
            boolean skipFirstLine = false;
            FSDataInputStream filein = fs.open(split.getPath());

            if (start != 0) {
                skipFirstLine = true;
                --start;
                filein.seek(start);
            }
            in = new LineReader(filein, conf);
            if (skipFirstLine) {
                start += in.readLine(new Text(), 0, (int) Math.min((long) Integer.MAX_VALUE, end - start));
            }
            this.pos = start;
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (key == null) {
                key = new LongWritable();
            }
            if (value == null) {
                value = new LongWritable();
            }

            Text v = new Text();
            int newSize = in.readLine(v, maxLineLength,
                    Math.max((int) Math.min(Integer.MAX_VALUE, end - pos), maxLineLength));
            if(newSize == 0) {
                key = null;
                value = null;
                return false;
            }

            String line = v.toString();
            String[] lineParts = line.split(" ");
            if(lineParts.length != 5) {
                throw new InterruptedException("Invalid OpenTSDB datapoint (" + line + ")(" + lineParts.length +")");
            }
            try{
                key.set(Long.parseLong(lineParts[1]));
                value.set(Long.parseLong(lineParts[2]));
            } catch (NumberFormatException e) {
                throw new InterruptedException(e.getMessage());
            }

            pos += newSize;
            return true;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            if (start == end) {
                return 0.0f;
            } else {
                return Math.min(1.0f, (pos - start) / (float) (end - start));
            }
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public LongWritable getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public void close() throws IOException {
            if (in != null) {
                in.close();
            }
        }
    }
}
