package net.hardnorth.yelp.ingest.common;

import com.google.common.collect.ImmutableSet;
import org.apache.beam.sdk.transforms.ProcessFunction;

import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class USStateFilter implements ProcessFunction<String, Boolean>
{
    private static final Pattern STATE_EXTRACT_PATTERN = Pattern.compile("\"state\"\\s*:\\s*\"([\\w]+)\"");

    private static final Set<String> STATE_LIST = ImmutableSet.<String>builder()
            .add("AK", "AL", "AR", "AZ", "CA", "CO", "CT", "DC", "DE", "FL", "GA", "GU", "HI", "IA", "ID", "IL", "IN")
            .add("KS", "KY", "LA", "MA", "MD", "ME", "MH", "MI", "MN", "MO", "MS", "MT", "NC", "ND", "NE", "NH", "NJ")
            .add("NM", "NV", "NY", "OH", "OK", "OR", "PA", "PR", "PW", "RI", "SC", "SD", "TN", "TX", "UT", "VA", "VI")
            .add("VT", "WA", "WI", "WV", "WY")
            .build();

    @Override
    public Boolean apply(String input)
    {
        Matcher m = STATE_EXTRACT_PATTERN.matcher(input);
        if (!m.find())
        {
            return null;
        }
        return STATE_LIST.contains(m.group(1).toUpperCase(Locale.US));
    }
}
