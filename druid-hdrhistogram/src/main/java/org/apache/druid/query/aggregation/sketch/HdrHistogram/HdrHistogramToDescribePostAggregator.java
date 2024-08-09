package org.apache.druid.query.aggregation.sketch.HdrHistogram;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Sets;
import org.HdrHistogram.HistogramSketch;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;
import java.util.*;

public class HdrHistogramToDescribePostAggregator implements PostAggregator {
    private final String name;
    private final String fieldName;

    @JsonCreator
    public HdrHistogramToDescribePostAggregator(
            @JsonProperty("name") String name,
            @JsonProperty("fieldName") String fieldName
    ){
        this.name = name;
        this.fieldName = fieldName;
    }

    @Override
    public ColumnType getType(ColumnInspector signature){
        return ColumnType.STRING;
    }

    @Override
    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonProperty
    public String getFieldName() {
        return fieldName;
    }

    @Nullable
    @Override
    public Object compute(Map<String, Object> values) {
        HistogramSketch histogram = (HistogramSketch) values.get(fieldName);
        if(histogram == null){
            return "{}"; //"[]"
        }
        return HdrHistogramModule.toJson(histogram.describe());
    }

    @Override
    public Comparator<double[]> getComparator()
    {
        throw new IAE("Comparing arrays of quantiles is not supported");
    }

    @Override
    public Set<String> getDependentFields()
    {
        return Sets.newHashSet(fieldName);
    }

    @Override
    public PostAggregator decorate(Map<String, AggregatorFactory> aggregators) {
        return this;
    }

    @Override
    public byte[] getCacheKey() {
        CacheKeyBuilder builder = new CacheKeyBuilder(HdrHistogramModule.CACHE_TYPE_ID_OFFSET).appendByte(HdrHistogramModule.QUANTILES_HDRHISTOGRAM_TO_DESCRIBE_CACHE_TYPE_ID)
                .appendString(fieldName);
        return builder.build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HdrHistogramToDescribePostAggregator that = (HdrHistogramToDescribePostAggregator) o;

        return name.equals(that.name) &&
                fieldName.equals(that.fieldName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, fieldName);
    }

    @Override
    public String toString() {
        return "HdrHistogramToDescribePostAggregator{" +
                "name='" + name + '\'' +
                ", fieldName='" + fieldName + '\'' +
                '}';
    }


}
