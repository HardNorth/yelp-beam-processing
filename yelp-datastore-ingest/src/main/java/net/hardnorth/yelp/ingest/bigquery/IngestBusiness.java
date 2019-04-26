package net.hardnorth.yelp.ingest.bigquery;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import net.hardnorth.yelp.ingest.bigquery.options.IngestOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.beam.sdk.values.TypeDescriptor.of;
import static org.apache.beam.sdk.values.TypeDescriptors.maps;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;

public class IngestBusiness
{
    private static final Logger LOGGER = LoggerFactory.getLogger(IngestBusiness.class);

    private static final Gson GSON = new GsonBuilder().serializeNulls().create();
    private static final Type RAW_MAP_TYPE = new TypeToken<Map<String, Object>>()
    {
    }.getType();

    private static final TableSchema SCHEMA = new TableSchema().setFields(
            ImmutableList.<TableFieldSchema>builder()
                    .add(new TableFieldSchema().setName("hours").setType("STRING").setMode("NULLABLE"))
                    .add(new TableFieldSchema().setName("address").setType("STRING").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("city").setType("STRING").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("is_open").setType("NUMERIC").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("latitude").setType("NUMERIC").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("review_count").setType("NUMERIC").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("stars").setType("NUMERIC").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("name").setType("STRING").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("attributes").setType("STRING").setMode("NULLABLE"))
                    .add(new TableFieldSchema().setName("state").setType("STRING").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("categories").setType("STRING").setMode("NULLABLE"))
                    .add(new TableFieldSchema().setName("postal_code").setType("STRING").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("business_id").setType("STRING").setMode("REQUIRED"))
                    .add(new TableFieldSchema().setName("longitude").setType("NUMERIC").setMode("REQUIRED"))
                    .build()
    );

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
                                return ((Map<String, Object>) GSON.fromJson(s, RAW_MAP_TYPE))
                                        .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, v -> String.valueOf(v.getValue())));
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
                            row.putAll(input);
                            return row;
                        }))
                .apply("Save to BigQuery", BigQueryIO.writeTableRows().to(tr)
                        .withSchema(SCHEMA)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        p.run().waitUntilFinish();
    }
}