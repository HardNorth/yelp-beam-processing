package net.hardnorth.yelp.ingest.datastore;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Value;
import com.google.datastore.v1.client.DatastoreHelper;
import com.google.gson.reflect.TypeToken;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.grpc.v1p13p1.com.google.gson.Gson;
import org.apache.beam.vendor.grpc.v1p13p1.com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static org.apache.beam.sdk.values.TypeDescriptors.maps;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;
import static org.apache.beam.sdk.values.TypeDescriptor.of;

public class IngestBusiness
{
    private static final Logger LOGGER = LoggerFactory.getLogger(IngestBusiness.class);

    private static final Gson GSON = new GsonBuilder().create();
    private static final Type RAW_MAP_TYPE = new TypeToken<Map<String, Object>>()
    {
    }.getType();

    public static void main(String[] args)
    {
        LOGGER.info("Running with parameters:" + Arrays.asList(args).toString());
        IngestOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(IngestOptions.class);
        final TableReference tr = new TableReference().setProjectId(options.getProject()).setDatasetId(options.getDatasetId()).setTableId(options.getTableName());

        Pipeline p = Pipeline.create(options);
        p.apply("Read input file line-by-line", TextIO.read().from(options.getDataSourceReference()))
                .apply("Filter lines which do not contain 'state' field", Filter.by((s) -> s.contains("\"state\":")))
                .apply("Convert to Map<String, String> through JSON DOM", MapElements
                        // uses imports from TypeDescriptors
                        .into(maps(strings(), strings()))
                        .via((s) -> {
                            try
                            {
                                return ((Map<String, Object>)GSON.fromJson(s, RAW_MAP_TYPE))
                                        .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, v->String.valueOf(v.getValue())));
                            }
                            catch (Throwable e)
                            {
                                LOGGER.warn("Unable to deserialize Json: " + s, e);
                                return null;
                            }
                        }))
                .apply("Filter nulls", Filter.by(Objects::nonNull))
                .apply("Wrap as TableRow objects for BigQuery", MapElements
                        .into(of(TableRow.class))
                        .via((Map<String, String> input) -> {
                            TableRow row = new TableRow();
                            for(Map.Entry<String, String> entry : input.entrySet()){
                                row = row.set(entry.getKey(), entry.getValue());
                            }
                            return row;
                        }))
                .apply("Save to Datastore", BigQueryIO.writeTableRows().to(tr)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));


        p.run().waitUntilFinish();
    }
}