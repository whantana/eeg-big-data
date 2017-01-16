package eeg.bigdata.hadoop.paasax;

public class Constants {

    public static final String N_CONFIGURATION_NAME = "eeg.paasax.N";

    public static final String L_CONFIGURATION_NAME = "eeg.paasax.L";

    public static final String IS_SAX_CONFIGURATION_NAME = "eeg.paasax.isSAX";

    public static final String PAA_MAX_VALUE_CONFIGURATION_NAME = "eeg.paasax.MaxPAA";

    public static final String PAA_MIN_VALUE_CONFIGURATION_NAME = "eeg.paasax.MinPAA";

    public static final String INPUT_METRIC_CONFIGURATION_NAME = "eeg.input.metric";

    public static final String INPUT_SID_CONFIGURATION_NAME = "eeg.input.sid";

    public static final String INPUT_RID_CONFIGURATION_NAME = "eeg.input.rid";

    public static final String OUTPUT_RID_CONFIGURATION_NAME = "eeg.output.rid";

    public static final String[] SAX_ALPHABET = {
            "a","b","c","d","e","f","g","h","i","j","k","l","m","n","o",
            "p","q","r","s","t","u","v","w","x","y","z"};

    public static final String JOB_DESCRIPTION = "PAA/SAX on timeseries with Hadoop";

    public static final int DEFAULT_N = 5;

    public static final int DEFAULT_L = 10000;

    public static final float DEFAULT_PAA_MAX_VALUE = 10F;

    public static final float DEFAULT_PAA_MIN_VALUE = -10F;
}
