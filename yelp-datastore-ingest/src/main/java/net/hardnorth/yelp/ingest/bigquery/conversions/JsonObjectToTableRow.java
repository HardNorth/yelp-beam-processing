package net.hardnorth.yelp.ingest.bigquery.conversions;

import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.beam.sdk.transforms.SerializableFunction;

import java.util.HashMap;
import java.util.Map;

public class JsonObjectToTableRow implements SerializableFunction<JsonObject, TableRow>
{
    @Override
    public TableRow apply(JsonObject input)
    {
        Map<String, Object> row = new HashMap<>();
        input.entrySet().forEach(e -> {
            String key = e.getKey();
            JsonElement value = e.getValue();
            if (value == null || value.isJsonNull())
            {
                row.put(key, null);
                return;
            }
            if (value.isJsonPrimitive())
            {
                JsonPrimitive primitive = value.getAsJsonPrimitive();
                if (primitive.isBoolean())
                {
                    row.put(key, primitive.getAsBoolean());
                    return;
                }
                if (primitive.isNumber())
                {
                    row.put(key, primitive.getAsDouble());
                    return;
                }
                if (primitive.isString())
                {
                    row.put(key, primitive.getAsString());
                    return;
                }
            }
            row.put(key, value.toString()
                    .replace("{", "\\{")
                    .replace("}", "\\}")
                    .replace("\"", "\\\"")
            );
        });
        TableRow result = new TableRow();
        result.putAll(row);
        return result;
    }
}
