package org.apache.druid.query.aggregation.sketch.HdrHistogram;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.HdrHistogram.HistogramSketch;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;

public class HdrHistogramMergeAggregatorFactory extends HdrHistogramAggregatorFactory {
    public HdrHistogramMergeAggregatorFactory(
            @JsonProperty("name") String name,
            @JsonProperty("fieldName") String fieldName,
            @JsonProperty("lowestDiscernibleValue")  @Nullable Long lowestDiscernibleValue,
            @JsonProperty("highestTrackableValue") @Nullable Long highestTrackableValue,
            @JsonProperty("numberOfSignificantValueDigits") @Nullable Integer numberOfSignificantValueDigits,
            @JsonProperty("autoResize") @Nullable Boolean autoResize
    ) {
        super(name, fieldName, lowestDiscernibleValue, highestTrackableValue, numberOfSignificantValueDigits, autoResize);
    }

    @Override
    public Aggregator factorize(ColumnSelectorFactory metricFactory) {
        final ColumnValueSelector<HistogramSketch> selector = metricFactory.makeColumnValueSelector(getFieldName());
        return new HdrHistogramMergeAggregator(
                selector,
                lowestDiscernibleValue,
                highestTrackableValue,
                numberOfSignificantValueDigits,
                autoResize
        );
    }

    @Override
    public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory) {
        final ColumnValueSelector<HistogramSketch> selector = metricFactory.makeColumnValueSelector(getFieldName());
        return new HdrHistogramMergeBufferAggregator(
                selector,
                lowestDiscernibleValue,
                highestTrackableValue,
                numberOfSignificantValueDigits,
                autoResize,
                getMaxIntermediateSize()
        );
    }

    @Override
    public AggregatorFactory withName(String newName) {
        return new HdrHistogramMergeAggregatorFactory(newName, fieldName, lowestDiscernibleValue, highestTrackableValue, numberOfSignificantValueDigits, autoResize);
    }

    @Override
    public byte[] getCacheKey() {
        return new CacheKeyBuilder(HdrHistogramModule.CACHE_TYPE_ID_OFFSET).appendByte(HdrHistogramModule.QUANTILES_HDRHISTOGRAM_MERGE_CACHE_TYPE_ID)
                .appendString(name).appendString(fieldName)
                .appendDouble(lowestDiscernibleValue).appendDouble(highestTrackableValue)
                .appendInt(numberOfSignificantValueDigits).appendBoolean(autoResize)
                .build();
    }


}
