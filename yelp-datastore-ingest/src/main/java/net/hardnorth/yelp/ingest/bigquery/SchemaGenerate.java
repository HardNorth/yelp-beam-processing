package net.hardnorth.yelp.ingest.bigquery;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SchemaGenerate
{
    private static final String INPUT = "d:\\1\\yelp\\business.json";
    private static final Gson GSON = new GsonBuilder().serializeNulls().create();
    private static final Type RAW_MAP_TYPE = new TypeToken<Map<String, Object>>()
    {
    }.getType();

    public static void main(String[] args) throws IOException
    {
        Map<String, Pair<String, String>> schema = new HashMap<>();

        BufferedReader reader = new BufferedReader(new FileReader(INPUT));
        String line;
        while ((line = reader.readLine()) != null)
        {
            Map<String, Object> row = GSON.fromJson(line, RAW_MAP_TYPE);
            Set<String> rowKeys = row.keySet();
            Set<String> schemaKeys = row.keySet();
            if (!rowKeys.containsAll(schemaKeys))
            {
                Set<String> nullableKeys = new HashSet<>(schemaKeys);
                nullableKeys.removeAll(rowKeys);
                nullableKeys.forEach(k -> {
                    Pair<String, String> oldSchema = schema.get(k);
                    Pair<String, String> newSchema = ImmutablePair.of(oldSchema.getLeft(), "NULLABLE");
                    schema.put(k, newSchema);
                });
            }

            row.entrySet().forEach(c -> {
                Pair<String, String> potentialSchema = getColumnSchema(c);
                if (potentialSchema != null)
                {
                    if (schema.containsKey(c.getKey()))
                    {
                        Pair<String, String> actualSchema = schema.get(c.getKey());
                        if (!actualSchema.getLeft().equals(potentialSchema.getLeft()))
                        {
                            Pair<String, String> newSchema = ImmutablePair.of("STRING", actualSchema.getRight());
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
                        Pair<String, String> actualSchema = schema.get(c.getKey());
                        Pair<String, String> newSchema = ImmutablePair.of(actualSchema.getLeft(), "NULLABLE");
                        schema.put(c.getKey(), newSchema);
                    }
                }
            });
        }

        System.out.print(schema.toString());
    }

    private static Pair<String, String> getColumnSchema(Map.Entry<String, Object> column)
    {
        if (column.getValue() == null)
        {
            return null;
        }

        String type = getType(column.getValue());
        String mode = "REQUIRED";
        return ImmutablePair.of(type, mode);
    }

    // Only two types supported: STRING, FLOAT64
    private static String getType(Object e)
    {
        if (e == null)
        {
            return null;
        }
        if (e instanceof String)
        {
            return "STRING";
        }
        if (e instanceof Double)
        {
            return "FLOAT64";
        }
        return "STRING";
    }
}