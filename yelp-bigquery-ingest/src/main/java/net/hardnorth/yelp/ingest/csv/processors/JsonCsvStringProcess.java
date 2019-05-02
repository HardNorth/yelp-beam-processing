package net.hardnorth.yelp.ingest.csv.processors;

import com.google.gson.JsonObject;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.commons.lang3.StringUtils;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static net.hardnorth.yelp.ingest.common.CommonUtil.*;

public class JsonCsvStringProcess implements SerializableFunction<String, String>
{
    private final Map<String, Integer> schema;

    public JsonCsvStringProcess(final List<String> csvSchema)
    {
        this.schema = IntStream.range(0, csvSchema.size()).boxed().collect(Collectors.toMap(csvSchema::get, v -> v));
    }

    @Override
    public String apply(String input)
    {
        JsonObject object = getJsonObject(getJson(input));

        // Should be already checked on JSON Object validity
        List<String> result = Objects.requireNonNull(object).entrySet().stream()
                .sorted(Comparator.comparing(e -> schema.get(e.getKey())))
                .map(Map.Entry::getValue)
                .map(v -> {
                    if (v == null || v.isJsonNull())
                    {
                        return null;
                    }
                    if (v.isJsonPrimitive())
                    {
                        return v.getAsJsonPrimitive().getAsString();
                    }
                    else
                    {
                        return escapeJson(v);
                    }
                })
                .collect(Collectors.toList());
        return StringUtils.join(result, ',');
    }
}
