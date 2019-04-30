package net.hardnorth.yelp.ingest.bigquery;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
import net.hardnorth.yelp.ingest.bigquery.conversions.JsonTableRowFunction;
import net.hardnorth.yelp.ingest.bigquery.options.ReviewIngestOptions;
import net.hardnorth.yelp.ingest.common.CommonUtil;
import net.hardnorth.yelp.ingest.common.conversions.BusinessIdKvFunction;
import net.hardnorth.yelp.ingest.common.processors.JsonObjectProcessor;
import net.hardnorth.yelp.ingest.common.processors.SortByKeyContains;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;

import static net.hardnorth.yelp.ingest.common.processors.JsonObjectProcessor.INVALID_JSON_OBJECT;
import static net.hardnorth.yelp.ingest.common.processors.JsonObjectProcessor.VALID_JSON_OBJECT;
import static net.hardnorth.yelp.ingest.common.processors.SortByKeyContains.*;
import static org.apache.beam.sdk.values.TypeDescriptor.of;
import static org.apache.beam.sdk.values.TypeDescriptors.kvs;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;

public class IngestReviews
{
    private static final String NOT_WANTED_JSONS_FILE = "not-wanted-reviews.json";
    private static final String TEMP_INVALID_JSON_FILE_NAME = "invalid_reviews.json";

    private static final Logger LOGGER = LoggerFactory.getLogger(IngestBusiness.class);

    private static final TableSchema SCHEMA = new TableSchema().setFields(
            ImmutableList.<TableFieldSchema>builder()
                    .add(new TableFieldSchema().setName("review_id").setType("STRING").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("user_id").setType("STRING").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("business_id").setType("STRING").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("date").setType("STRING").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("text").setType("STRING").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("cool").setType("FLOAT64").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("stars").setType("FLOAT64").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("useful").setType("FLOAT64").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("funny").setType("FLOAT64").setMode("REQUIRED"))
                    .build()
    );

    private static final BusinessIdKvFunction BUSINESS_ID_AS_KEY_FUNCTION = new BusinessIdKvFunction();
    private static final JsonObjectProcessor JSON_OBJECT_PROCESSOR = new JsonObjectProcessor();
    private static final JsonTableRowFunction JSON_TO_TABLE_ROW_CONVERSION_FUNCTION = new JsonTableRowFunction();

    public static void main(String[] args)
    {
        LOGGER.info("Running with parameters:" + Arrays.asList(args).toString());
        ReviewIngestOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(ReviewIngestOptions.class);
        final TableReference tr = new TableReference().setProjectId(options.getProject())
                .setDatasetId(options.getDatasetId()).setTableId(options.getTableName());

        // Read all business IDs into a Singleton View and create sorting function
        Pipeline pipeline = Pipeline.create(options);
        PCollectionView<String> businessIds = pipeline
                .apply("Read business IDs file", TextIO.read().from(options.getBusinessIdFile()))
                .apply("Remove duplicate IDs", Distinct.create())
                .apply(Combine.globally((SerializableFunction<Iterable<String>, String>) elements -> StringUtils.join(elements, ',')).asSingletonView());
        SortByKeyContains businessIdProcessor = new SortByKeyContains(businessIds);

        // Read all reviews into a collection and extract business_ids from them as keys
        PCollection<KV<String, String>> allReviewsById = pipeline
                .apply("Read input file line-by-line", TextIO.read().from(options.getDataSourceReference()))
                .apply(MapElements.into(kvs(strings(), strings()))
                        .via(BUSINESS_ID_AS_KEY_FUNCTION));

        // Sort reviews into into two collections which contain and not contain wanted business IDs
        PCollectionTuple businessSortResult = allReviewsById
                .apply("Sort by business IDs", ParDo.of(businessIdProcessor).withSideInputs(businessIds)
                .withOutputTags(CONTAINS, TupleTagList.of(NOT_CONTAINS)));

        // Save not wanted business IDs into temp file
        String tempNotWantedJsons = CommonUtil.getLocation(options.getTempLocation(), NOT_WANTED_JSONS_FILE);
        businessSortResult.get(NOT_CONTAINS)
                .apply("Extract values", MapElements.into(strings()).via(KV::getValue))
                .apply("Write invalid not wanted JSONs to a temporary file: " + tempNotWantedJsons, TextIO.write().to(tempNotWantedJsons));

        // Process wanted JSONs
        PCollectionTuple jsonConversionResult = businessSortResult.get(CONTAINS)
                .apply("Extract values", MapElements.into(strings()).via(KV::getValue))
                .apply("Verify String to JSON conversion", ParDo.of(JSON_OBJECT_PROCESSOR)
                        .withOutputTags(VALID_JSON_OBJECT, TupleTagList.of(INVALID_JSON_OBJECT)));

        // Save invalid JSONs to a temp file
        String tempInvalidJsons = CommonUtil.getLocation(options.getTempLocation(), TEMP_INVALID_JSON_FILE_NAME);
        jsonConversionResult.get(INVALID_JSON_OBJECT)
                .apply("Write invalid JSONs to a temporary file: " + tempInvalidJsons, TextIO.write().to(tempInvalidJsons));

        // Process valid JSONs further
        jsonConversionResult.get(VALID_JSON_OBJECT)
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
