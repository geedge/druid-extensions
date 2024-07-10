package org.apache.druid.query.aggregation.sketch.HdrHistogram;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Sets;
import org.HdrHistogram.HistogramSketch;
import org.HdrHistogram.Percentile;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;
import java.util.*;

public class HdrHistogramToPercentilesPostAggregator implements PostAggregator {
    private final String name;
    private final String fieldName;
    private final int percentileTicksPerHalfDistance;

    @JsonCreator
    public HdrHistogramToPercentilesPostAggregator(
            @JsonProperty("name") String name,
            @JsonProperty("fieldName") String fieldName,
            @JsonProperty("percentileTicksPerHalfDistance") int percentileTicksPerHalfDistance
    ){
        this.name = name;
        this.fieldName = fieldName;
        this.percentileTicksPerHalfDistance = percentileTicksPerHalfDistance;
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

    @JsonProperty
    public int getPercentileTicksPerHalfDistance() {
        return percentileTicksPerHalfDistance;
    }

    @Nullable
    @Override
    public Object compute(Map<String, Object> values) {
        HistogramSketch histogram = (HistogramSketch) values.get(fieldName);
        if(histogram == null){
            return "[]"; //"[]"
        }
        List<Percentile> percentiles = histogram.percentileList(percentileTicksPerHalfDistance);
        return HdrHistogramModule.toJson(percentiles);
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
        CacheKeyBuilder builder = new CacheKeyBuilder(HdrHistogramModule.CACHE_TYPE_ID_OFFSET).appendByte(HdrHistogramModule.QUANTILES_HDRHISTOGRAM_TO_PERCENTILES_CACHE_TYPE_ID)
                .appendString(fieldName);
        builder.appendInt(percentileTicksPerHalfDistance);
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
        HdrHistogramToPercentilesPostAggregator that = (HdrHistogramToPercentilesPostAggregator) o;

        return percentileTicksPerHalfDistance == that.percentileTicksPerHalfDistance &&
                name.equals(that.name) &&
                fieldName.equals(that.fieldName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, fieldName, percentileTicksPerHalfDistance);
    }

    @Override
    public String toString() {
        return "HdrHistogramToPercentilesPostAggregator{" +
                "name='" + name + '\'' +
                ", fieldName='" + fieldName + '\'' +
                ", probabilitys=" + percentileTicksPerHalfDistance +
                '}';
    }


}
