package net.hardnorth.yelp.ingest.common;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonUtil
{
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonUtil.class);
    private static final Gson GSON = new GsonBuilder().serializeNulls().create();

    public static JsonElement getJson(String from)
    {
        if (from == null)
        {
            return null;
        }
        try
        {
            return GSON.fromJson(from, JsonElement.class);
        }
        catch (Throwable e)
        {
            LOGGER.warn("Unable to deserialize Json: " + from, e);
            return null;
        }
    }

    public static JsonObject getJsonObject(JsonElement from)
    {
        if (from == null)
        {
            return null;
        }
        try
        {
            return from.getAsJsonObject();
        }
        catch (Throwable e)
        {
            LOGGER.warn("Unable to convert JsonElement into JsonObject: " + from.toString(), e);
            return null;
        }
    }

    public static String escapeJson(JsonElement input)
    {
        if (input == null)
        {
            return null;
        }
        return input.toString()
                .replace("{", "\\{")
                .replace("}", "\\}")
                .replace("\"", "\\\"")
                .replace("[", "\\[")
                .replace("]", "\\]")
                .replace("/", "\\/");
    }
}
