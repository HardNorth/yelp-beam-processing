package net.hardnorth.yelp.ingest.common.conversions;

import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BusinessIdKvFunction implements SerializableFunction<String, KV<String, String>>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BusinessIdKvFunction.class);

    private static final Pattern BUSINESS_ID_EXTRACT_PATTERN = Pattern.compile("\"business_id\"\\s*:\\s*\"([\\w\\-]+)\"");

    @Override
    public KV<String, String> apply(String input)
    {
        Matcher m = BUSINESS_ID_EXTRACT_PATTERN.matcher(input);
        if (!m.find())
        {
            LOGGER.warn("Unable to find required business ID field in: " + input);
            return null;
        }
        return KV.of(m.group(1), input);
    }
}
