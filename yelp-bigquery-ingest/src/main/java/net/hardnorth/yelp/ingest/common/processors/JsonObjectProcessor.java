package net.hardnorth.yelp.ingest.common.processors;

import com.google.gson.JsonObject;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

import static net.hardnorth.yelp.ingest.common.CommonUtil.getJson;
import static net.hardnorth.yelp.ingest.common.CommonUtil.getJsonObject;

public class JsonObjectProcessor extends DoFn<String, String>
{
    public static final TupleTag<String> VALID_JSON_OBJECT = new TupleTag<String>()
    {
    };
    public static final TupleTag<String> INVALID_JSON_OBJECT = new TupleTag<String>()
    {
    };

    @ProcessElement
    public void processElement(ProcessContext c)
    {
        JsonObject object = getJsonObject(getJson(c.element()));
        if (object != null)
        {
            c.output(c.element());
        }
        else
        {
            c.output(INVALID_JSON_OBJECT, c.element());
        }
    }
}
