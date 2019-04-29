package net.hardnorth.yelp.ingest.csv.options;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

public interface IngestOptions extends DataflowPipelineOptions
{
    @Description("A reference on Data Source as URI, e.g.: gs://yelp-dataset/business.json")
    String getDataSourceReference();

    void setDataSourceReference(String reference);

    @Description("A reference on Data Output as URI, e.g.: gs://yelp-dataset/business-processed.csv")
    String getDataOutputReference();

    void setDataOutputReference(String id);

    @Description("Wait until job execution with log output")
    @Default.Boolean(false)
    void setSyncExecution(Boolean sync);

    Boolean getSyncExecution();
}
