package net.hardnorth.yelp.ingest.bigquery.conversions;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StringToJsonObjectFunction implements SerializableFunction<String, JsonObject>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(StringToJsonObjectFunction.class);
    private static final Gson GSON = new GsonBuilder().serializeNulls().create();

    @Override
    public JsonObject apply(String input)
    {
        try
        {
            return GSON.fromJson(input, JsonElement.class).getAsJsonObject();
        }
        catch (Throwable e)
        {
            LOGGER.warn("Unable to deserialize Json: " + input, e);
            return null;
        }
    }
}
