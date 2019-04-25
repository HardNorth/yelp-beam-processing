package net.hardnorth.yelp.ingest.datastore;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Value;
import com.google.datastore.v1.client.DatastoreHelper;
import com.google.gson.reflect.TypeToken;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.grpc.v1p13p1.com.google.gson.Gson;
import org.apache.beam.vendor.grpc.v1p13p1.com.google.gson.GsonBuilder;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.beam.sdk.values.TypeDescriptors.maps;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;

public class IngestBusiness
{
    private static final Gson GSON = new GsonBuilder().create();
    private static final Type RAW_MAP_TYPE = new TypeToken<Map<String, String>>()
    {
    }.getType();

    public static void main(String[] args)
    {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        IngestOptions ingestOptions = options.as(IngestOptions.class);

        Pipeline p = Pipeline.create(options);
        p.apply(TextIO.read().from(ingestOptions.getDataSourceReference()))
                .apply(Filter.by((s) -> s.contains("\"state\":")))
                .apply(MapElements
                        // uses imports from TypeDescriptors
                        .into(maps(strings(), strings()))
                        .via((s) -> GSON.fromJson(s, RAW_MAP_TYPE)))
                .apply(MapElements
                        .into(TypeDescriptor.of(Entity.class))
                        .via(input -> {
                            Key keyField = DatastoreHelper.makeKey(input.get(ingestOptions.getKeyField())).build();
                            Map<String, Value> result = input.entrySet().stream()
                                    .filter(e -> !e.getKey().equals(ingestOptions.getKeyField()))
                                    .collect(Collectors.toMap(Map.Entry::getKey, v -> DatastoreHelper.makeValue(v.getValue()).build()));

                            return Entity.newBuilder().setKey(keyField).putAllProperties(result).build();
                        }))
                .apply(DatastoreIO.v1().write());
    }
}