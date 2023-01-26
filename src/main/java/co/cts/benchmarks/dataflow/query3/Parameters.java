package co.cts.benchmarks.dataflow.query3;

public final class Parameters {

    private Parameters(){}
    public static final String PROJECT = "TODO: write the name of your project";
    public static final String DATASET = PROJECT + ".tpcds_1TB";
    public static final String RESULT_TABLE = DATASET + ".output_dataflow";

    public static final String REGION = "TODO: select your region";
    public static final String GCS_DIRECTORY = "gs://<TODO select your bucket>/dataflowJob";
}
