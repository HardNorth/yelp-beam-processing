package net.hardnorth.yelp.ingest.datastore;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface IngestOptions extends PipelineOptions
{
    @Description("A reference on Data Source as URI, e.g.: gs://yelp-dataset/business.json")
    String getDataSourceReference();
    void setDataSourceReference(String reference);

    @Description("Which field use as key, e.g.: 'business_id'")
    @Default.String("id")
    String getKeyField();
    void setKeyField(String keyField);

    @Description("GCP Project ID")
    String getProject();
    void setProject(String id);
}
