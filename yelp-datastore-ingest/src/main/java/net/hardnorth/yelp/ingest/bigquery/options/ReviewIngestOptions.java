package net.hardnorth.yelp.ingest.bigquery.options;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

public interface ReviewIngestOptions extends CommonIngestOptions
{
    @Description("A reference on a file with business IDs, e.g.: gs://yelp-dataset/us-business-ids.csv")
    String getBusinessIdFile();

    void setBusinessIdFile(String reference);
}
