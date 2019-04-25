package net.hardnorth.yelp.ingest.pgsql;

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
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static org.apache.beam.sdk.values.TypeDescriptors.maps;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;

public class IngestBusiness
{
    private static final Gson GSON = new GsonBuilder().create();
    private static final Type MAP_OF_STRINGS = new TypeToken<Map<String, String>>()
    {
    }.getType();

    private static final String PROJECT_ID = "MBT-ML";
    private static final String TABLE_ID = "business";

    private static final Key KEY_FIELD = DatastoreHelper.makeKey("business_id").build();

    public static void main(String[] args) throws Exception
    {
        PipelineOptions options = PipelineOptionsFactory.create();

        Pipeline p = Pipeline.create(options);
        p.apply(TextIO.read().from("gs://vh-yelp-dataset/" + TABLE_ID + ".json"))
                .apply(Filter.by((s) -> s.contains("\"state\":")))
                .apply(MapElements
                        // uses imports from TypeDescriptors
                        .into(maps(strings(), strings()))
                        .via((s) -> GSON.fromJson(s, MAP_OF_STRINGS)))
                .apply(MapElements
                        // uses imports from TypeDescriptors
                        .into(maps(strings(), TypeDescriptor.of(Value.class)))
                        .via((Map<String, String> m) -> m.entrySet().stream().collect(Collectors.toMap(e->e.getKey(), v->DatastoreHelper.makeValue(v.getValue())))))
                .apply(MapElements
                        .into(TypeDescriptor.of(Entity.class))
                        .via((Map<String, String> m) -> {
                            Entity.newBuilder().setKey(KEY_FIELD).p;
                        })
                .apply(DatastoreIO.v1().write().withProjectId(PROJECT_ID));
    }
}
