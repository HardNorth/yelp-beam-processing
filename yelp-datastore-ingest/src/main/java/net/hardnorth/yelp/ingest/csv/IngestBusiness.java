package net.hardnorth.yelp.ingest.csv;

import com.google.common.collect.ImmutableList;
import net.hardnorth.yelp.ingest.common.BusinessCommon;
import net.hardnorth.yelp.ingest.csv.options.IngestOptions;
import net.hardnorth.yelp.ingest.csv.processors.JsonCsvStringProcess;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static org.apache.beam.sdk.values.TypeDescriptors.strings;

public class IngestBusiness
{
    private static final Logger LOGGER = LoggerFactory.getLogger(IngestBusiness.class);

    private static final List<String> CSV_SCHEMA = ImmutableList.<String>builder()
            .add("business_id", "name", "address", "state", "city", "postal_code", "hours", "is_open", "latitude")
            .add("longitude", "review_count", "stars", "categories", "attributes").build();
    private static final String CSV_HEADER = StringUtils.join(CSV_SCHEMA.iterator(), ',');
    private static final JsonCsvStringProcess JSON_TO_CSV_CONVERSION_FUNCTION = new JsonCsvStringProcess(CSV_SCHEMA);

    public static void main(String[] args)
    {
        LOGGER.info("Running with parameters:" + Arrays.asList(args).toString());

        IngestOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(IngestOptions.class);

        Pipeline pipeline = Pipeline.create(options);
        BusinessCommon.getUsBusiness(pipeline, options.getDataSourceReference(), options.getTempLocation())
                .apply("Convert JSONs to CSV row", MapElements.into(strings())
                        .via(JSON_TO_CSV_CONVERSION_FUNCTION))
                .apply("Save to CSV", TextIO.write().to(options.getDataOutputReference()).withHeader(CSV_HEADER));

        PipelineResult started = pipeline.run();
        if (options.getSyncExecution())
        {
            started.waitUntilFinish();
        }
    }
}
