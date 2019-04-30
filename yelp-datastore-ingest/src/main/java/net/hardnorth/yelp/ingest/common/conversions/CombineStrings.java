package net.hardnorth.yelp.ingest.common.conversions;

import org.apache.beam.sdk.transforms.SerializableFunction;

public class CombineStrings implements SerializableFunction<Iterable<String>, String>
{
    @Override
    public String apply(Iterable<String> input)
    {
        StringBuilder sb = new StringBuilder();
        input.forEach(e -> sb.append(e).append(System.lineSeparator()));
        return sb.toString();
    }
}
