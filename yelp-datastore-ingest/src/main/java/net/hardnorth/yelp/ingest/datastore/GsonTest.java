package net.hardnorth.yelp.ingest.datastore;

import com.google.gson.reflect.TypeToken;
import org.apache.beam.vendor.grpc.v1p13p1.com.google.gson.Gson;
import org.apache.beam.vendor.grpc.v1p13p1.com.google.gson.GsonBuilder;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.stream.Collectors;

public class GsonTest
{
    private static final String INPUT = "d:\\1\\yelp\\business.json";
    private static final Gson GSON = new GsonBuilder().create();
    private static final Type RAW_MAP_TYPE = new TypeToken<Map<String, Object>>()
    {
    }.getType();


    public static void main(String[] args) throws IOException
    {
        BufferedReader reader = new BufferedReader(new FileReader(INPUT));
        String line;
        while ((line = reader.readLine()) != null)
        {
            Map<String, String> result = ((Map<String, Object>)GSON.fromJson(line, RAW_MAP_TYPE))
                    .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, v->String.valueOf(v.getValue())));
            System.out.println(result.get("business_id"));
        }
    }
}
