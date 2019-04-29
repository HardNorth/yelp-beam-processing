package net.hardnorth.yelp.ingest.csv.processors;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static net.hardnorth.yelp.ingest.common.CommonUtil.*;

public class JsonCsvStringProcess extends DoFn<String, String>
{
    public static final TupleTag<String> CONVERTED = new TupleTag<String>()
    {
    };
    public static final TupleTag<String> INVALID_JSON = new TupleTag<String>()
    {
    };

    private final Map<String, Integer> schema;

    public JsonCsvStringProcess(final List<String> csvSchema)
    {
        this.schema = IntStream.range(0, csvSchema.size()).boxed().collect(Collectors.toMap(csvSchema::get, v -> v));
    }


    @ProcessElement
    public void processElement(ProcessContext c)
    {
        JsonObject object = getJsonObject(getJson(c.element()));
        if (object == null)
        {
            c.output(INVALID_JSON, c.element());
            return;
        }
        List<String> result = object.entrySet().stream()
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
        c.output(StringUtils.join(result.iterator(), ','));
    }
}
