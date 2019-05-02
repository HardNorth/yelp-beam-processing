package net.hardnorth.yelp.ingest.bigquery.conversions;

import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.beam.sdk.transforms.SerializableFunction;

import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static net.hardnorth.yelp.ingest.common.CommonUtil.*;

public class JsonTableRowFunction implements SerializableFunction<String, TableRow>
{
    @Override
    public TableRow apply(String input)
    {
        JsonObject object = getJsonObject(getJson(input));
        Map<String, Object> row = Objects.requireNonNull(object).entrySet().stream()
                .filter(e -> !e.getValue().isJsonNull())
                .collect(Collectors.toMap(Map.Entry::getKey, e -> {
                    JsonElement value = e.getValue();
                    if (value.isJsonPrimitive())
                    {
                        JsonPrimitive primitive = value.getAsJsonPrimitive();
                        if (primitive.isBoolean())
                        {
                            return primitive.getAsBoolean();
                        }
                        if (primitive.isNumber())
                        {
                            return primitive.getAsDouble();
                        }
                        if (primitive.isString())
                        {
                            return primitive.getAsString();
                        }
                    }
                    return escapeJson(value);
                }));
        TableRow result = new TableRow();
        result.putAll(row);
        return result;
    }
}
