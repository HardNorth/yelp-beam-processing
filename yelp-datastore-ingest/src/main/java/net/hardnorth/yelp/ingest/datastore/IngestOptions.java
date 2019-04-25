package net.hardnorth.yelp.ingest.datastore;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

public interface IngestOptions extends DataflowPipelineOptions
{
    @Description("A reference on Data Source as URI, e.g.: gs://yelp-dataset/business.json")
    String getDataSourceReference();
    void setDataSourceReference(String reference);

    @Description("Output dataset ID")
    @Default.String("id")
    String getDatasetId();
    void setDatasetId(String id);

    @Description("Table name to save")
    String getTableName();
    void setTableName(String table);
}
