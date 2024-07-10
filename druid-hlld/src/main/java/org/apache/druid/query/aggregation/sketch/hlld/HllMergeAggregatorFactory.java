package org.apache.druid.query.aggregation.sketch.hlld;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.zdjz.galaxy.sketch.hlld.Hll;
import com.zdjz.galaxy.sketch.hlld.HllUnion;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;

public class HllMergeAggregatorFactory extends HllAggregatorFactory{
    public HllMergeAggregatorFactory(
            @JsonProperty("name") final String name,
            @JsonProperty("fieldName") final String fieldName,
            @JsonProperty("precision") @Nullable final Integer precision,
            @JsonProperty("round") @Nullable final Boolean round
    ) {
        super(name, fieldName, precision, round);
    }

    /*
    没这个方法了, 新版本需要实现getIntermediateType方法
    @Override
    public String getTypeName(){
        return HllModule.HLLD_TYPE_NAME;
    }*/

    @Override
    public ColumnType getIntermediateType() {
        return HllModule.TYPE;
    }

    @Override
    public Aggregator factorize(ColumnSelectorFactory metricFactory) {
        final ColumnValueSelector<Hll> selector = metricFactory.makeColumnValueSelector(getFieldName());
        return new HllMergeAggregator(
                selector,
                precision
        );
    }

    @Override
    public BufferAggregator factorizeBuffered(ColumnSelectorFactory columnSelectorFactory) {
        final ColumnValueSelector<Hll> selector = columnSelectorFactory.makeColumnValueSelector(getFieldName());
        return new HllMergeBufferAggregator(
                selector,
                precision
        );
    }

    @Override
    public AggregatorFactory withName(String newName) {
        return new HllMergeAggregatorFactory(newName, fieldName, precision, round);
    }

    @Override
    public byte[] getCacheKey() {
        return new CacheKeyBuilder(HllModule.CACHE_TYPE_ID_OFFSET).appendByte(HllModule.HLLD_MERGE_CACHE_TYPE_ID)
                .appendString(name).appendString(fieldName)
                .appendInt(precision).appendBoolean(round)
                .build();
    }

    @Override
    protected int getUpdatableSerializationBytes() {
        return HllUnion.getUpdatableSerializationBytes(precision);
    }
}
