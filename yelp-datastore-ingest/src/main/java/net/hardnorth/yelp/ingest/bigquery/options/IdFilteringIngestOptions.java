package net.hardnorth.yelp.ingest.bigquery.options;

import org.apache.beam.sdk.options.Description;

public interface IdFilteringIngestOptions extends CommonIngestOptions
{
    @Description("A reference on a file with selected IDs for ingest, e.g.: gs://yelp-dataset/us-business-ids.csv")
    String getSelectedIdFile();

    void setSelectedIdFile(String reference);
}
