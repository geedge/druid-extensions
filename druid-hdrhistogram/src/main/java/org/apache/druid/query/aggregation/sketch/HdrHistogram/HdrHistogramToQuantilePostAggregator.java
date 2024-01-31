package org.apache.druid.query.aggregation.sketch.HdrHistogram;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Sets;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramSketch;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class HdrHistogramToQuantilePostAggregator implements PostAggregator {
    private final String name;
    private final String fieldName;
    private final float probability;

    @JsonCreator
    public HdrHistogramToQuantilePostAggregator(
            @JsonProperty("name") String name,
            @JsonProperty("fieldName") String fieldName,
            @JsonProperty("probability") float probability
    ){
        this.name = name;
        this.fieldName = fieldName;
        this.probability = probability;

        if (probability < 0 || probability > 1) {
            throw new IAE("Illegal probability[%s], must be strictly between 0 and 1", probability);
        }
    }

    @Override
    public ColumnType getType(ColumnInspector signature){
        return ColumnType.LONG;
    }

    @Override
    public Set<String> getDependentFields() {
        return Sets.newHashSet(fieldName);
    }

    @Override
    public Comparator getComparator() {
        return new Comparator<Long>(){
            @Override
            public int compare(final Long a, final Long b){
                return Long.compare(a, b);
            }
        };
    }

    @Nullable
    @Override
    public Object compute(Map<String, Object> values) {
        HistogramSketch histogram = (HistogramSketch) values.get(fieldName);
        if(histogram == null){
            return null;
        }
        return histogram.getValueAtPercentile(probability * 100);
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
    public double getProbability() {
        return probability;
    }

    @Override
    public PostAggregator decorate(Map<String, AggregatorFactory> aggregators) {
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HdrHistogramToQuantilePostAggregator that = (HdrHistogramToQuantilePostAggregator) o;

        return Float.compare(that.probability, probability) == 0 &&
                name.equals(that.name) &&
                fieldName.equals(that.fieldName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, fieldName, probability);
    }

    @Override
    public String toString() {
        return "HdrHistogramToQuantilePostAggregator{" +
                "name='" + name + '\'' +
                ", fieldName='" + fieldName + '\'' +
                ", probability=" + probability +
                '}';
    }

    @Override
    public byte[] getCacheKey() {
        return new CacheKeyBuilder(HdrHistogramModule.CACHE_TYPE_ID_OFFSET).appendByte(HdrHistogramModule.QUANTILES_HDRHISTOGRAM_TO_QUANTILE_CACHE_TYPE_ID)
                .appendString(fieldName)
                .appendFloat(probability)
                .build();
    }
}
