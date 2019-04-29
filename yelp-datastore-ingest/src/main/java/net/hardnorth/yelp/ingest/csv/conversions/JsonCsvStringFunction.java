package net.hardnorth.yelp.ingest.csv.conversions;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static net.hardnorth.yelp.ingest.common.CommonUtil.*;

public class JsonCsvStringFunction implements SerializableFunction<String, String>
{
    private final Map<String, Integer> schema;

    public JsonCsvStringFunction(List<String> csvSchema)
    {
        this.schema = IntStream.range(0, csvSchema.size()).boxed().collect(Collectors.toMap(csvSchema::get, v -> v));
    }


    @Override
    public String apply(String input)
    {
        JsonObject object = getJsonObject(getJson(input));
        if (object == null)
        {
            return null;
        }
        List<String> row = new ArrayList<>(schema.size());
        object.entrySet().forEach(e -> {
            String key = e.getKey();
            JsonElement value = e.getValue();
            if (value == null || value.isJsonNull())
            {
                return;
            }
            if (value.isJsonPrimitive())
            {
                JsonPrimitive primitive = value.getAsJsonPrimitive();
                row.add(schema.get(key), primitive.getAsString());
            }
            else
            {
                row.add(schema.get(key), escapeJson(value));
            }
        });

        return StringUtils.join(row.iterator(), ',');
    }
}
