package net.hardnorth.yelp.ingest.common;

import net.hardnorth.yelp.ingest.common.processors.JsonObjectProcessor;
import net.hardnorth.yelp.ingest.common.processors.USStateProcess;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;

import java.util.Arrays;

import static net.hardnorth.yelp.ingest.common.processors.JsonObjectProcessor.INVALID_JSON_OBJECT;
import static net.hardnorth.yelp.ingest.common.processors.JsonObjectProcessor.VALID_JSON_OBJECT;
import static net.hardnorth.yelp.ingest.common.processors.USStateProcess.*;

public class IngestBusinessCommon
{
    private static final String TEMP_NON_US_FILE_NAME = "non-us_business.json";
    private static final String TEMP_NON_DETERMINED_FILE_NAME = "cannot-determine_business.json";
    private static final String TEMP_INVALID_JSON_FILE_NAME = "invalid_business.json";
    private static final USStateProcess US_STATE_PROCESS = new USStateProcess();
    private static final JsonObjectProcessor JSON_OBJECT_PROCESSOR = new JsonObjectProcessor();

    public static PCollection<String> getUsBusiness(Pipeline pipeline, String source, String tempLocation)
    {
        // Read and tag JSONs with "US", "NON_US" and "CANNOT_DETERMINE" tags
        PCollectionTuple taggedBusiness =
                pipeline.apply("Read input file line-by-line", TextIO.read().from(source))
                        .apply("Throw away non US businesses by state", ParDo.of(US_STATE_PROCESS)
                                .withOutputTags(US, TupleTagList.of(Arrays.asList(NON_US, CANNOT_DETERMINE))));

        // Save "NON_US" and "CANNOT_DETERMINE" tagged elements into temporary files and do not process them further
        String tempNonUs = CommonUtil.getLocation(tempLocation, TEMP_NON_US_FILE_NAME);
        taggedBusiness.get(NON_US)
                .apply("Write non US elements to a temporary file: " + tempNonUs, TextIO.write().to(tempNonUs));
        String tempNotDetermined = CommonUtil.getLocation(tempLocation, TEMP_NON_DETERMINED_FILE_NAME);
        taggedBusiness.get(CANNOT_DETERMINE)
                .apply("Write JSONs without 'state' field to a temporary file: " + tempNotDetermined, TextIO.write().to(tempNotDetermined));

        // Process only "US" elements
        PCollectionTuple jsonConversionResult = taggedBusiness.get(US)
                .apply("Verify String to JSON conversion", ParDo.of(JSON_OBJECT_PROCESSOR)
                        .withOutputTags(VALID_JSON_OBJECT, TupleTagList.of(INVALID_JSON_OBJECT)));

        // Save invalid JSONs to a temp file
        String tempInvalidJsons = CommonUtil.getLocation(tempLocation, TEMP_INVALID_JSON_FILE_NAME);
        jsonConversionResult.get(INVALID_JSON_OBJECT)
                .apply("Write invalid JSONs to a temporary file: " + tempInvalidJsons, TextIO.write().to(tempInvalidJsons));

        // Return only VALID JSONs within "US"
        return jsonConversionResult.get(VALID_JSON_OBJECT);
    }
}
