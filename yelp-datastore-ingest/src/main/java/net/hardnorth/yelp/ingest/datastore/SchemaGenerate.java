package net.hardnorth.yelp.ingest.datastore;

import com.google.gson.*;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SchemaGenerate
{
    private static final String INPUT = "d:\\1\\yelp\\business.json";
    private static final Gson GSON = new GsonBuilder().serializeNulls().create();

    public static void main(String[] args) throws IOException
    {
        Map<String, Triple<String, String, String>> schema = new HashMap<>();

        BufferedReader reader = new BufferedReader(new FileReader(INPUT));
        String line;
        while ((line = reader.readLine()) != null)
        {
            JsonObject row = GSON.fromJson(line, JsonElement.class).getAsJsonObject();
            Set<String> rowKeys = row.keySet();
            Set<String> schemaKeys = row.keySet();
            if (!rowKeys.containsAll(schemaKeys))
            {
                Set<String> nullableKeys = new HashSet<>(schemaKeys);
                nullableKeys.removeAll(rowKeys);
                nullableKeys.forEach(k -> {
                    Triple<String, String, String> oldSchema = schema.get(k);
                    Triple<String, String, String> newSchema = ImmutableTriple.of(oldSchema.getLeft(), oldSchema.getMiddle(), "NULLABLE");
                    schema.put(k, newSchema);
                });
            }

            row.entrySet().forEach(c -> {
                Triple<String, String, String> potentialSchema = getColumnSchema(c);
                if (potentialSchema != null)
                {
                    if (schema.containsKey(c.getKey()))
                    {
                        Triple<String, String, String> actualSchema = schema.get(c.getKey());
                        if (!actualSchema.getMiddle().equals(potentialSchema.getMiddle()))
                        {
                            Triple<String, String, String> newSchema = ImmutableTriple.of(actualSchema.getLeft(), "STRING", actualSchema.getRight());
                            schema.put(c.getKey(), newSchema);
                        }
                    }
                    else
                    {
                        schema.put(c.getKey(), potentialSchema);
                    }

                }
                else
                {
                    if (schema.containsKey(c.getKey()))
                    {
                        Triple<String, String, String> actualSchema = schema.get(c.getKey());
                        Triple<String, String, String> newSchema = ImmutableTriple.of(actualSchema.getLeft(), actualSchema.getMiddle(), "NULLABLE");
                        schema.put(c.getKey(), newSchema);
                    }
                }
            });
        }

        System.out.print(schema.toString());
    }

    private static Triple<String, String, String> getColumnSchema(Map.Entry<String, JsonElement> column)
    {
        if (column.getValue() == null || column.getValue().isJsonNull())
        {
            return null;
        }

        String name = column.getKey();
        String type = getType(column.getValue());
        String mode = "REQUIRED";
        return ImmutableTriple.of(name, type, mode);
    }

    private static String getType(JsonElement e)
    {
        if (e == null || e.isJsonNull())
        {
            return null;
        }
        if (e.isJsonObject() || e.isJsonArray())
        {
            return "STRING";
        }
        JsonPrimitive p = e.getAsJsonPrimitive();
        if (p.isBoolean())
        {
            return "BOOL";
        }
        if (p.isNumber())
        {
            return "NUMERIC";
        }
        return "STRING";
    }
}
