package net.hardnorth.yelp.ingest.bigquery;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
import net.hardnorth.yelp.ingest.bigquery.conversions.JsonTableRowFunction;
import net.hardnorth.yelp.ingest.bigquery.options.IngestOptions;
import net.hardnorth.yelp.ingest.common.BusinessCommon;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static org.apache.beam.sdk.values.TypeDescriptor.of;

public class IngestBusiness
{
    private static final Logger LOGGER = LoggerFactory.getLogger(IngestBusiness.class);

    private static final TableSchema SCHEMA = new TableSchema().setFields(
            ImmutableList.<TableFieldSchema>builder()
                    .add(new TableFieldSchema().setName("business_id").setType("STRING").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("name").setType("STRING").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("address").setType("STRING").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("state").setType("STRING").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("city").setType("STRING").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("postal_code").setType("STRING").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("hours").setType("STRING").setMode("NULLABLE"))
                    .add(new TableFieldSchema().setName("is_open").setType("FLOAT64").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("latitude").setType("FLOAT64").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("longitude").setType("FLOAT64").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("review_count").setType("FLOAT64").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("stars").setType("FLOAT64").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("categories").setType("STRING").setMode("NULLABLE"))
                    .add(new TableFieldSchema().setName("attributes").setType("STRING").setMode("NULLABLE"))
                    .build()
    );
    private static final JsonTableRowFunction JSON_TO_TABLE_ROW_CONVERSION_FUNCTION = new JsonTableRowFunction();

    public static void main(String[] args)
    {
        LOGGER.info("Running with parameters:" + Arrays.asList(args).toString());
        IngestOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(IngestOptions.class);
        final TableReference tr = new TableReference().setProjectId(options.getProject()).setDatasetId(options.getDatasetId()).setTableId(options.getTableName());

        Pipeline pipeline = Pipeline.create(options);

        BusinessCommon.getUsBusiness(pipeline, options.getDataSourceReference(), options.getTempLocation())
                .apply("Convert JSONs to one step depth TableRow objects for BigQuery", MapElements
                        .into(of(TableRow.class))
                        .via(JSON_TO_TABLE_ROW_CONVERSION_FUNCTION))
                .apply("Save to BigQuery", BigQueryIO.writeTableRows().to(tr)
                        .withSchema(SCHEMA)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        PipelineResult started = pipeline.run();
        if (options.getSyncExecution())
        {
            started.waitUntilFinish();
        }
    }
}