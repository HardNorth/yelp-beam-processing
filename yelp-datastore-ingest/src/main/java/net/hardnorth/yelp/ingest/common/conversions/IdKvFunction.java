package net.hardnorth.yelp.ingest.common.conversions;

import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IdKvFunction implements SerializableFunction<String, KV<String, String>>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(IdKvFunction.class);

    private final String key;
    private final Pattern idExtractPattern;

    public IdKvFunction(String keyFieldName)
    {
        key = keyFieldName;
        idExtractPattern = Pattern.compile("\"" + key + "\"\\s*:\\s*\"([\\w\\-]+)\"");
    }

    @Override
    public KV<String, String> apply(String input)
    {
        Matcher m = idExtractPattern.matcher(input);
        if (!m.find())
        {
            LOGGER.warn("Unable to find required '{}' field in: {}", key, input);
            return null;
        }
        return KV.of(m.group(1), input);
    }
}
