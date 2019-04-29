package net.hardnorth.yelp.ingest.csv.processors;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
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

        c.output(StringUtils.join(row.iterator(), ','));
    }
}
