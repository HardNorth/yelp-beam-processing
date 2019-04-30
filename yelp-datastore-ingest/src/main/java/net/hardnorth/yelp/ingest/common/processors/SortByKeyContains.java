package net.hardnorth.yelp.ingest.common.processors;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;

import java.util.Objects;

public class SortByKeyContains extends DoFn<KV<String, String>, KV<String, String>>
{
    public static final TupleTag<KV<String, String>> CONTAINS = new TupleTag<KV<String, String>>()
    {
    };
    public static final TupleTag<KV<String, String>> NOT_CONTAINS = new TupleTag<KV<String, String>>()
    {
    };

    private final PCollectionView<String> view;

    public SortByKeyContains(PCollectionView<String> sideView)
    {
        view = sideView;
    }

    @ProcessElement
    public void processElement(ProcessContext c)
    {
        String keyIds = c.sideInput(view);
        KV<String, String> element = c.element();
        String key = Objects.requireNonNull(element.getKey());
        if (keyIds.contains(key))
        {
            c.output(element);
        }
        else
        {
            c.output(NOT_CONTAINS, element);
        }
    }
}
