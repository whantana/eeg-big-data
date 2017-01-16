package eeg.bigdata.hadoop.fft;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HadoopFFT extends Configured implements Tool{

    @Override
    public int run(String[] strings) throws Exception {
        return 0;
    }

    public static void main(final String args[]) throws Exception {
        Configuration configuration = new Configuration();
        HadoopFFT job = new HadoopFFT();
        ToolRunner.run(configuration, job, args);
    }
}
