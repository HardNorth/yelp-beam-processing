package net.hardnorth.yelp.ingest.csv;

import com.google.common.collect.ImmutableList;
import net.hardnorth.yelp.ingest.common.BusinessCommon;
import net.hardnorth.yelp.ingest.common.CommonUtil;
import net.hardnorth.yelp.ingest.csv.options.IngestOptions;
import net.hardnorth.yelp.ingest.csv.processors.JsonCsvStringProcess;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static net.hardnorth.yelp.ingest.csv.processors.JsonCsvStringProcess.CONVERTED;
import static net.hardnorth.yelp.ingest.csv.processors.JsonCsvStringProcess.INVALID_JSON;

public class IngestBusiness
{
    private static final String TEMP_INVALID_JSON_FILE_NAME = "invalid_business.json";
    private static final Logger LOGGER = LoggerFactory.getLogger(IngestBusiness.class);

    private static final List<String> CSV_SCHEMA = ImmutableList.<String>builder()
            .add("business_id", "name", "address", "state", "city", "postal_code", "hours", "is_open", "latitude")
            .add("longitude", "review_count", "stars", "categories", "attributes").build();
    private static final String CSV_HEADER = StringUtils.join(CSV_SCHEMA.iterator(), ',');
    private static final JsonCsvStringProcess CSV_CONVERT_FUNCTION = new JsonCsvStringProcess(CSV_SCHEMA);

    public static void main(String[] args)
    {
        LOGGER.info("Running with parameters:" + Arrays.asList(args).toString());

        IngestOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(IngestOptions.class);

        Pipeline pipeline = Pipeline.create(options);
        PCollectionTuple jsonConversionResult =
                BusinessCommon.getUsBusiness(pipeline, options.getDataSourceReference(), options.getTempLocation())
                        .apply("Convert JSONs to CSV row", ParDo.of(CSV_CONVERT_FUNCTION)
                                .withOutputTags(CONVERTED, TupleTagList.of(INVALID_JSON)));

        // Save invalid JSONs to a temp file
        String tempInvalidJsons = CommonUtil.getLocation(options.getTempLocation(), TEMP_INVALID_JSON_FILE_NAME);
        jsonConversionResult.get(INVALID_JSON)
                .apply("Write non invalid JSONs to a temporary file: " + tempInvalidJsons, TextIO.write().to(tempInvalidJsons));

        // Output valid JSONs to a CSV file with a Header
        jsonConversionResult.get(CONVERTED)
                .apply("Save to CSV", TextIO.write().to(options.getDataOutputReference()).withHeader(CSV_HEADER));

        PipelineResult started = pipeline.run();
        if (options.getSyncExecution())
        {
            started.waitUntilFinish();
        }
    }
}
