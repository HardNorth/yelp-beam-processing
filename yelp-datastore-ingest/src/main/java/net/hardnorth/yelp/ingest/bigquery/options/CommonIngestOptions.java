package net.hardnorth.yelp.ingest.bigquery.options;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

public interface CommonIngestOptions extends DataflowPipelineOptions
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

    @Description("Wait until job execution with log output")
    @Default.Boolean(false)
    Boolean getSyncExecution();

    void setSyncExecution(Boolean sync);
}
