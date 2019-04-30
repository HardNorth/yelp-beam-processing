package net.hardnorth.yelp.ingest.common.conversions;

import net.hardnorth.yelp.ingest.common.entity.StringSetFilter;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class FileToStringSetFilter implements SerializableFunction<FileIO.ReadableFile, StringSetFilter>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FileToStringSetFilter.class);

    @Override
    public StringSetFilter apply(FileIO.ReadableFile input)
    {
        try
        {
            return new StringSetFilter(input.readFullyAsUTF8String());
        }
        catch (IOException e)
        {
            LOGGER.warn("Unable to read input file '{}' field in: {}", input.getMetadata().resourceId().toString(), e);
            return null;
        }
    }
}
