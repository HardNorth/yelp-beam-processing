package net.hardnorth.yelp.ingest.common.entity;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class StringSetFilter implements Serializable
{
    private static final long serialVersionUID = -8717426662769633800L;

    private final Set<String> values;

    public StringSetFilter(String from)
    {
        values = new HashSet<>();
        values.addAll(Arrays.asList(from.split("\\r?\\n")));
    }

    public StringSetFilter(Iterable<StringSetFilter> from)
    {
        values = new HashSet<>();
        from.forEach(f -> values.addAll(f.values));
    }

    public boolean contains(String str)
    {
        return values.contains(str);
    }
}
