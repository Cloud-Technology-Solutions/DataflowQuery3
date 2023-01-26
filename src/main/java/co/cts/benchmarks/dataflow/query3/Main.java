package co.cts.benchmarks.dataflow.query3;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class Main {

    public static void main(String[] args) {
        Pipeline pipeline = createPipeline();
        launchPipeline(pipeline);
    }

    private static Pipeline createPipeline() {
        DataflowPipelineOptions options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);

        options.setRunner(DataflowRunner.class);
        options.setJobName("query3-benchmark");

        options.setTempLocation(Parameters.GCS_DIRECTORY + "/temp");
        options.setStagingLocation(Parameters.GCS_DIRECTORY + "/staging");
        options.setRegion(Parameters.REGION);

        options.setNumWorkers(154);
        // Remove the expensive Shuffle Service
        options.setExperiments(List.of("shuffle_mode=appliance"));

        return Pipeline.create(options);
    }

    private static void launchPipeline(Pipeline pipeline) {
        readInputTables(pipeline)
                .apply(query())
                .apply(saveToBigQuery());

        pipeline.run();
    }

    private static PCollectionTuple readInputTables(Pipeline pipeline) {
        PCollection<Row> storeSalesRows = pipeline.apply("Get store_sales", InputReader.getStoreSales());
        PCollection<Row> dateDimRows = pipeline.apply("Get date_dim", InputReader.getDates());
        PCollection<Row> itemRows = pipeline.apply("Get item", InputReader.getItems());

        return PCollectionTuple
                .of(new TupleTag<>("store_sales"), storeSalesRows)
                .and(new TupleTag<>("date_dim"), dateDimRows)
                .and(new TupleTag<>("item"), itemRows);
    }

    private static SqlTransform query() {
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        StringBuilder sb = new StringBuilder();

        try (InputStream inputStream = classloader.getResourceAsStream("query3.sql")) {
            for (int ch; (ch = inputStream.read()) != -1; ) {
                sb.append((char) ch);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        String query3 = sb.toString();

        return SqlTransform.query(query3);
    }

    private static BigQueryIO.Write<Row> saveToBigQuery() {
        return BigQueryIO.<Row>write()
                .to(Parameters.RESULT_TABLE)
                .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
                .useBeamSchema()
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED);
    }
}

