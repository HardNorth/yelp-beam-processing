package net.hardnorth.yelp.ingest.csv.options;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;

public interface IngestOptions extends DataflowPipelineOptions
{
    @Description("A reference on Data Source as URI, e.g.: gs://yelp-dataset/business.json")
    String getDataSourceReference();

    void setDataSourceReference(String reference);

    @Description("A reference on Data Output as URI, e.g.: gs://yelp-dataset/business-processed.csv")
    String getDataOutputReference();

    void setDataOutputReference(String id);
}
