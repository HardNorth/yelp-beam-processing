package net.hardnorth.yelp.ingest.bigquery;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
import net.hardnorth.yelp.ingest.bigquery.options.IdFilteringIngestOptions;
import net.hardnorth.yelp.ingest.common.conversions.IdKvFunction;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class IngestUsers
{
    private static final String NOT_WANTED_JSONS_FILE = "not-wanted-users.json";
    private static final String TEMP_INVALID_JSON_FILE_NAME = "invalid_users.json";

    private static final Logger LOGGER = LoggerFactory.getLogger(IngestBusiness.class);

    private static final TableSchema SCHEMA = new TableSchema().setFields(
            ImmutableList.<TableFieldSchema>builder()
                    .add(new TableFieldSchema().setName("user_id").setType("STRING").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("name").setType("STRING").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("yelping_since").setType("STRING").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("review_count").setType("FLOAT64").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("friends").setType("STRING").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("elite").setType("STRING").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("cool").setType("FLOAT64").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("fans").setType("FLOAT64").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("useful").setType("FLOAT64").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("funny").setType("FLOAT64").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("average_stars").setType("FLOAT64").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("compliment_more").setType("FLOAT64").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("compliment_writer").setType("FLOAT64").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("compliment_funny").setType("FLOAT64").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("compliment_plain").setType("FLOAT64").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("compliment_note").setType("FLOAT64").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("compliment_profile").setType("FLOAT64").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("compliment_hot").setType("FLOAT64").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("compliment_photos").setType("FLOAT64").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("compliment_cool").setType("FLOAT64").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("compliment_list").setType("FLOAT64").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("compliment_cute").setType("FLOAT64").setMode("REQUIRED"))
                    .build()
    );

    private static final IdKvFunction ID_AS_KEY_FUNCTION = new IdKvFunction("user_id");

    public static void main(String[] args)
    {
        LOGGER.info("Running with parameters:" + Arrays.asList(args).toString());
        IdFilteringIngestOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(IdFilteringIngestOptions.class);

        Pipeline pipeline = Pipeline.create(options);
        IngestCommon.ingestWithIdFilteringCollection(pipeline, options, ID_AS_KEY_FUNCTION, NOT_WANTED_JSONS_FILE,
                TEMP_INVALID_JSON_FILE_NAME, SCHEMA);
    }
}
