package net.hardnorth.yelp.ingest.bigquery.conversions;

import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.beam.sdk.transforms.SerializableFunction;

import java.util.HashMap;
import java.util.Map;

import static net.hardnorth.yelp.ingest.common.CommonUtil.*;

public class JsonTableRowFunction implements SerializableFunction<String, TableRow>
{
    @Override
    public TableRow apply(String input)
    {
        JsonObject object = getJsonObject(getJson(input));
        if (object == null)
        {
            return null;
        }

        Map<String, Object> row = new HashMap<>();
        object.entrySet().forEach(e -> {
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
            row.put(key, escapeJson(value));
        });
        TableRow result = new TableRow();
        result.putAll(row);
        return result;
    }
}
