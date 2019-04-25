package net.hardnorth.yelp.ingest.pgsql;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;

import static org.apache.beam.sdk.io.Compression.GZIP;
import static org.apache.beam.sdk.values.TypeDescriptors.*;

import java.io.IOException;

public class Ingest
{
    public static void main(String[] args) throws Exception{
        PipelineOptions options = PipelineOptionsFactory.create();

        Pipeline p = Pipeline.create(options);
//        p.apply(TextIO.read().from("gs://vh-yelp-dataset/*"));
        p.apply(FileIO.match().filepattern("gs://vh-yelp-dataset/*"))
                .apply(FileIO.readMatches())
                .apply(MapElements
                        // uses imports from TypeDescriptors
                        .into(kvs(strings(), strings()))
                        .via((FileIO.ReadableFile f) -> {
                                return KV.of(f.getMetadata().resourceId().toString(), "");
                        }));

    }
}
