package net.hardnorth.yelp.ingest.csv;

import com.google.common.collect.ImmutableMap;
import net.hardnorth.yelp.ingest.common.USStateFilter;
import net.hardnorth.yelp.ingest.csv.conversions.JsonCsvStringFunction;
import net.hardnorth.yelp.ingest.csv.options.IngestOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;

import static org.apache.beam.sdk.values.TypeDescriptors.strings;

public class IngestBusiness
{
    private static final Logger LOGGER = LoggerFactory.getLogger(IngestBusiness.class);

    private static final Map<String, Integer> CSV_SCHEMA = ImmutableMap.<String, Integer>builder()
            .put("business_id", 0)
            .put("name", 1)
            .put("address", 2)
            .put("state", 3)
            .put("city", 4)
            .put("postal_code", 5)
            .put("hours", 6)
            .put("is_open", 7)
            .put("latitude", 8)
            .put("longitude", 9)
            .put("review_count", 10)
            .put("stars", 11)
            .put("categories", 12)
            .put("attributes", 13)
            .build();

    private static final String CSV_HEADER = StringUtils.join(CSV_SCHEMA.entrySet().stream()
            .sorted(Comparator.comparing(Map.Entry::getValue)).map(Map.Entry::getKey).iterator(), ',');

    public static void main(String[] args)
    {
        LOGGER.info("Running with parameters:" + Arrays.asList(args).toString());
        IngestOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(IngestOptions.class);

        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply("Read input file line-by-line", TextIO.read().from(options.getDataSourceReference()))
                .apply("Throw away non US businesses by state", Filter.by(new USStateFilter()))
                .apply("Throw away null rows", Filter.by(Objects::nonNull))
                .apply("Convert JSONs to CSV row", MapElements
                        .into(strings())
                        .via(new JsonCsvStringFunction(CSV_SCHEMA)))
                .apply("Throw away null rows", Filter.by(Objects::nonNull))
                .apply("Save to CSV", TextIO.write().to(options.getDataOutputReference()).withHeader(CSV_HEADER));
        pipeline.run().waitUntilFinish();
    }
}
