package net.hardnorth.yelp.ingest.bigquery;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import net.hardnorth.yelp.ingest.bigquery.conversions.JsonTableRowFunction;
import net.hardnorth.yelp.ingest.bigquery.options.CommonIngestOptions;
import net.hardnorth.yelp.ingest.bigquery.options.IdFilteringIngestOptions;
import net.hardnorth.yelp.ingest.common.CommonUtil;
import net.hardnorth.yelp.ingest.common.conversions.CombineStrings;
import net.hardnorth.yelp.ingest.common.conversions.IdKvFunction;
import net.hardnorth.yelp.ingest.common.conversions.ReadFileFully;
import net.hardnorth.yelp.ingest.common.processors.JsonObjectProcessor;
import net.hardnorth.yelp.ingest.common.processors.SortByKeyContains;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;

import static net.hardnorth.yelp.ingest.common.processors.JsonObjectProcessor.INVALID_JSON_OBJECT;
import static net.hardnorth.yelp.ingest.common.processors.JsonObjectProcessor.VALID_JSON_OBJECT;
import static net.hardnorth.yelp.ingest.common.processors.SortByKeyContains.CONTAINS;
import static net.hardnorth.yelp.ingest.common.processors.SortByKeyContains.NOT_CONTAINS;
import static org.apache.beam.sdk.values.TypeDescriptor.of;
import static org.apache.beam.sdk.values.TypeDescriptors.kvs;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;

public class IngestCommon
{
    private static final JsonObjectProcessor JSON_OBJECT_PROCESSOR = new JsonObjectProcessor();
    private static final JsonTableRowFunction JSON_TO_TABLE_ROW_CONVERSION_FUNCTION = new JsonTableRowFunction();
    private static final ReadFileFully FILE_READER = new ReadFileFully();
    private static final CombineStrings STRING_COMBINER = new CombineStrings();

    public static TableReference getTableReference(CommonIngestOptions options)
    {
        return new TableReference().setProjectId(options.getProject())
                .setDatasetId(options.getDatasetId()).setTableId(options.getTableName());
    }

    public static void ingestWithIdFilteringCollection
            (Pipeline pipeline, IdFilteringIngestOptions options, IdKvFunction sortFunction,
             String rejectedDataFileName, String invalidJsonsFileName, TableSchema schema)
    {
        // Read all business IDs into a Singleton View and create sorting function
        PCollectionView<String> sortIds = pipeline
                .apply("Get ID files", FileIO.match().filepattern(options.getSelectedIdFile()))
                .apply("Read ID file descriptors", FileIO.readMatches())
                .apply("Read ID file content", MapElements.into(strings()).via(FILE_READER))
                .apply("Combine ID file content", Combine.globally(STRING_COMBINER).asSingletonView());
        SortByKeyContains sortIdProcessor = new SortByKeyContains(sortIds);

        // Read all reviews into a collection and extract business_ids from them as keys
        PCollection<KV<String, String>> dataById = pipeline
                .apply("Read input file line-by-line", TextIO.read().from(options.getDataSourceReference()))
                .apply(MapElements.into(kvs(strings(), strings()))
                        .via(sortFunction));

        // Sort reviews into into two collections which contain and not contain wanted business IDs
        PCollectionTuple sortResult = dataById
                .apply("Sort by IDs", ParDo.of(sortIdProcessor).withSideInputs(sortIds)
                        .withOutputTags(CONTAINS, TupleTagList.of(NOT_CONTAINS)));

        // Save not wanted business IDs into temp file
        String tempUnwantedJsons = CommonUtil.getLocation(options.getTempLocation(), rejectedDataFileName);
        sortResult.get(NOT_CONTAINS)
                .apply("Extract values", MapElements.into(strings()).via(KV::getValue))
                .apply("Write unwanted JSONs to a temporary file: " + tempUnwantedJsons, TextIO.write().to(tempUnwantedJsons));

        // Process wanted JSONs
        PCollectionTuple jsonConversionResult = sortResult.get(CONTAINS)
                .apply("Extract values", MapElements.into(strings()).via(KV::getValue))
                .apply("Verify String to JSON conversion", ParDo.of(JSON_OBJECT_PROCESSOR)
                        .withOutputTags(VALID_JSON_OBJECT, TupleTagList.of(INVALID_JSON_OBJECT)));

        // Save invalid JSONs to a temp file
        String tempInvalidJsons = CommonUtil.getLocation(options.getTempLocation(), invalidJsonsFileName);
        jsonConversionResult.get(INVALID_JSON_OBJECT)
                .apply("Write invalid JSONs to a temporary file: " + tempInvalidJsons, TextIO.write().to(tempInvalidJsons));

        // Process valid JSONs further
        jsonConversionResult.get(VALID_JSON_OBJECT)
                .apply("Convert JSONs to one step depth TableRow objects for BigQuery", MapElements
                        .into(of(TableRow.class))
                        .via(JSON_TO_TABLE_ROW_CONVERSION_FUNCTION))
                .apply("Save to BigQuery", BigQueryIO.writeTableRows().to(IngestCommon.getTableReference(options))
                        .withSchema(schema)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        PipelineResult started = pipeline.run();
        if (options.getSyncExecution())
        {
            started.waitUntilFinish();
        }
    }
}
