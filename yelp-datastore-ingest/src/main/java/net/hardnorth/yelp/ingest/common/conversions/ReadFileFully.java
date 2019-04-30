package net.hardnorth.yelp.ingest.common.conversions;

import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ReadFileFully implements SerializableFunction<FileIO.ReadableFile, String>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ReadFileFully.class);

    @Override
    public String apply(FileIO.ReadableFile input)
    {
        try
        {
            return input.readFullyAsUTF8String();
        }
        catch (IOException e)
        {
            LOGGER.warn("Unable to read input file '{}' field in: {}", input.getMetadata().resourceId().toString(), e);
            return null;
        }
    }
}
