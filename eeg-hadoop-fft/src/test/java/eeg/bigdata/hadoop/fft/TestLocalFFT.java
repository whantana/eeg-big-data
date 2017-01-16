package eeg.bigdata.hadoop.fft;

import edu.emory.mathcs.jtransforms.fft.DoubleFFT_1D;
import edu.emory.mathcs.utils.ConcurrencyUtils;
import edu.emory.mathcs.utils.IOUtils;
import junit.framework.TestCase;

import java.util.Arrays;

public class TestLocalFFT extends TestCase {

    int n = 10;
    private double[] signal;

    public void setUp() {

        // initialize
        signal = new double[n];
        IOUtils.fillMatrix_1D(n, signal);
        IOUtils.showReal_1D(signal,"signal (setup)");

        // set thread to 1
        ConcurrencyUtils.setNumberOfThreads(1);

    }

    public void testLocalFft() {

        // keep original for later comparision
        double[] original = new double[n];
        System.arraycopy(signal,0,original,0,n);

        // calculate fft
        DoubleFFT_1D fft = new DoubleFFT_1D(n);
        fft.realForward(signal);

        // caluclate inverse fft
        fft.realInverse(signal,true);

        // assert equals for original and transformed signal for a given input
        double eps = Math.pow(2, -52);
        for(int i=0; i<n; i++) assertEquals(signal[i],original[i],eps);
    }

    public void testSplitFft() {

        int half = n/2;

        // keep original for later comparision
        double[] original = new double[n];
        System.arraycopy(signal,0,original,0,n);

        // split 1
        double[] split1 = Arrays.copyOfRange(signal,0,half);
        IOUtils.showReal_1D(split1,"signal (split 1)");

        // split 2
        double[] split2 = Arrays.copyOfRange(signal,half,n);
        IOUtils.showReal_1D(split2,"signal (split 2)");

        // Transformation
        DoubleFFT_1D fft = new DoubleFFT_1D(half);
        fft.realForward(split1);
        IOUtils.showReal_1D(split1,"Transformed signal (split 1)");
        fft.realForward(split2);
        IOUtils.showReal_1D(split2,"Transformed signal (split 2)");
        fft.realForward(signal);
        IOUtils.showReal_1D(signal,"Transformed signal");

        // Inverse transformation
        fft.realInverse(split1,true);
        IOUtils.showReal_1D(split1,"signal (split 1)");
        fft.realInverse(split2,true);
        IOUtils.showReal_1D(split2,"signal (split 2)");
        fft.realInverse(signal,true);
        IOUtils.showReal_1D(signal,"signal");

        // assert equals for original and transformed signal for a given input
        double eps = Math.pow(2, -52);
        for(int i=0; i<n; i++) assertEquals(signal[i],original[i],eps);
        for(int i=0; i<half ; i++) assertEquals(split1[i],original[i],eps);
        for(int i=0; i<half ; i++) assertEquals(split2[i],original[i+half],eps);
    }

    public void tearDown() {
        signal = null;
    }
}
