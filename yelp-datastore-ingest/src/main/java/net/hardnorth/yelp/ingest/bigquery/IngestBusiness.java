package net.hardnorth.yelp.ingest.bigquery;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
import com.google.gson.JsonObject;
import net.hardnorth.yelp.ingest.bigquery.conversions.JsonObjectToTableRow;
import net.hardnorth.yelp.ingest.bigquery.conversions.StringToJsonObjectFunction;
import net.hardnorth.yelp.ingest.bigquery.options.IngestOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Objects;

import static org.apache.beam.sdk.values.TypeDescriptor.of;

public class IngestBusiness
{
    private static final Logger LOGGER = LoggerFactory.getLogger(IngestBusiness.class);

    private static final TableSchema SCHEMA = new TableSchema().setFields(
            ImmutableList.<TableFieldSchema>builder()
                    .add(new TableFieldSchema().setName("hours").setType("STRING").setMode("NULLABLE"))
                    .add(new TableFieldSchema().setName("address").setType("STRING").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("city").setType("STRING").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("is_open").setType("FLOAT64").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("latitude").setType("FLOAT64").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("review_count").setType("FLOAT64").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("stars").setType("FLOAT64").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("name").setType("STRING").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("attributes").setType("STRING").setMode("NULLABLE"))
                    .add(new TableFieldSchema().setName("state").setType("STRING").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("categories").setType("STRING").setMode("NULLABLE"))
                    .add(new TableFieldSchema().setName("postal_code").setType("STRING").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("business_id").setType("STRING").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("longitude").setType("FLOAT64").setMode("REQUIRED"))
                    .build()
    );

    public static void main(String[] args)
    {
        LOGGER.info("Running with parameters:" + Arrays.asList(args).toString());
        IngestOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(IngestOptions.class);
        final TableReference tr = new TableReference().setProjectId(options.getProject()).setDatasetId(options.getDatasetId()).setTableId(options.getTableName());

        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply("Read input file line-by-line", TextIO.read().from(options.getDataSourceReference()))
                .apply("Convert to JsonObject to test JSON integrity", MapElements
                        .into(of(JsonObject.class))
                        .via(new StringToJsonObjectFunction()))
                .apply("Throw away null rows", Filter.by(Objects::nonNull))
                .apply("Convert JsonObject to one step depth TableRow objects for BigQuery", MapElements
                        .into(of(TableRow.class))
                        .via(new JsonObjectToTableRow()))
                .apply("Save to BigQuery", BigQueryIO.writeTableRows().to(tr)
                        .withSchema(SCHEMA)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        pipeline.run().waitUntilFinish();
    }
}