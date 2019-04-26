package net.hardnorth.yelp.ingest.bigquery.conversions;

import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.*;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class JsonTableRowFunction implements SerializableFunction<String, TableRow>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonTableRowFunction.class);
    private static final Gson GSON = new GsonBuilder().serializeNulls().create();

    @Override
    public TableRow apply(String input)
    {
        JsonObject object;
        try
        {
            object = GSON.fromJson(input, JsonElement.class).getAsJsonObject();
        }
        catch (Throwable e)
        {
            LOGGER.warn("Unable to deserialize Json: " + input, e);
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
