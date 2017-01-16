package eeg.bigdata.hadoop.paasax;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import static eeg.bigdata.hadoop.paasax.Constants.INPUT_METRIC_CONFIGURATION_NAME;
import static eeg.bigdata.hadoop.paasax.Constants.INPUT_SID_CONFIGURATION_NAME;
import static eeg.bigdata.hadoop.paasax.Constants.OUTPUT_RID_CONFIGURATION_NAME;

public class OpenTSDBAsciiOutputFormat extends TextOutputFormat<LongWritable, Text> {

    protected static class OpenTSDBAsciiWriter extends RecordWriter<LongWritable, Text> {
        private static final String utf8 = "UTF-8";
        private static final byte[] newline;
        static {
            try {
                newline = "\n".getBytes(utf8);
            } catch (UnsupportedEncodingException uee) {
                throw new IllegalArgumentException("can't find " + utf8 + " encoding");
            }
        }
        protected DataOutputStream out;
        private final byte[] spaceSeperator;
        private final String electrode;
        private final String sid;
        private final String rid;

        public OpenTSDBAsciiWriter(DataOutputStream out, final String electrode, final String sid, final String rid) {
            this.out = out;
            try {
                this.spaceSeperator = " ".getBytes(utf8);
            } catch (UnsupportedEncodingException uee) {
                throw new IllegalArgumentException("can't find " + utf8 + " encoding");
            }
            this.electrode = electrode;
            this.sid = sid;
            this.rid = rid;
        }

        /**
         * Write the object to the byte stream, handling Text as a special
         * case.
         *
         * @param o the object to print
         * @throws java.io.IOException if the write throws, we pass it on
         */
        private void writeObject(Object o) throws IOException {
            if (o instanceof Text) {
                Text to = (Text) o;
                out.write(to.getBytes(), 0, to.getLength());
            } else {
                out.write(o.toString().getBytes(utf8));
            }
        }

        public synchronized void write(LongWritable key, Text value)
                throws IOException {

            boolean nullKey = key == null;
            boolean nullValue = value == null;
            if (nullKey && nullValue) {
                return;
            }
            writeObject(new Text(electrode));
            out.write(spaceSeperator);
            if (!nullKey) {
                writeObject(key);
            }
            if (!(nullKey || nullValue)) {
                out.write(spaceSeperator);
            }
            if (!nullValue) {
                writeObject(value);
            }
            out.write(spaceSeperator);
            writeObject(new Text("sid="+sid));
            out.write(spaceSeperator);
            writeObject(new Text("rid="+rid));
            out.write(newline);
        }

        public synchronized void close(TaskAttemptContext context) throws IOException {
            out.close();
        }
    }


    public RecordWriter<LongWritable, Text>
    getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        final String electrode = context.getConfiguration().get(INPUT_METRIC_CONFIGURATION_NAME);
        final String sid = context.getConfiguration().get(INPUT_SID_CONFIGURATION_NAME);
        final String rid = context.getConfiguration().get(OUTPUT_RID_CONFIGURATION_NAME);
        Configuration conf = context.getConfiguration();
        Path file = getDefaultWorkFile(context, "");
        FileSystem fs = file.getFileSystem(conf);
        FSDataOutputStream fileOut = fs.create(file, false);
        return new OpenTSDBAsciiWriter(fileOut,electrode,sid,rid);
    }
}
