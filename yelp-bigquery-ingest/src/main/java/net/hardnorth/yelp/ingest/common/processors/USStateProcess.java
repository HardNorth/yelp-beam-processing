package net.hardnorth.yelp.ingest.common.processors;

import com.google.common.collect.ImmutableSet;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class USStateProcess extends DoFn<String, String>
{
    public static final TupleTag<String> US = new TupleTag<String>()
    {
    };
    public static final TupleTag<String> NON_US = new TupleTag<String>()
    {
    };
    public static final TupleTag<String> CANNOT_DETERMINE = new TupleTag<String>()
    {
    };

    private static final Pattern STATE_EXTRACT_PATTERN = Pattern.compile("\"state\"\\s*:\\s*\"([\\w]+)\"");

    private static final Set<String> STATE_LIST = ImmutableSet.<String>builder()
            .add("AK", "AL", "AR", "AZ", "CA", "CO", "CT", "DC", "DE", "FL", "GA", "GU", "HI", "IA", "ID", "IL", "IN")
            .add("KS", "KY", "LA", "MA", "MD", "ME", "MH", "MI", "MN", "MO", "MS", "MT", "NC", "ND", "NE", "NH", "NJ")
            .add("NM", "NV", "NY", "OH", "OK", "OR", "PA", "PR", "PW", "RI", "SC", "SD", "TN", "TX", "UT", "VA", "VI")
            .add("VT", "WA", "WI", "WV", "WY")
            .build();

    public USStateProcess()
    {
    }

    @ProcessElement
    public void processElement(ProcessContext c)
    {
        Matcher m = STATE_EXTRACT_PATTERN.matcher(c.element());
        if (!m.find())
        {
            c.output(CANNOT_DETERMINE, c.element());
            return;
        }

        if (STATE_LIST.contains(m.group(1).toUpperCase(Locale.US)))
        {
            c.output(c.element());
        }
        else
        {
            c.output(NON_US, c.element());
        }
    }
}
