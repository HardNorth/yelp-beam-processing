package net.hardnorth.yelp.ingest.common.conversions;

import net.hardnorth.yelp.ingest.common.entity.StringSetFilter;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class StringSetFilterCombiner implements SerializableFunction<Iterable<StringSetFilter>, StringSetFilter>
{
    @Override
    public StringSetFilter apply(Iterable<StringSetFilter> input)
    {
        return new StringSetFilter(input);
    }
}
