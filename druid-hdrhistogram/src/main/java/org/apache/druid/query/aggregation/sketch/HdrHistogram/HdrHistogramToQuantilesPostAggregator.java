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
import java.util.*;

public class HdrHistogramToQuantilesPostAggregator implements PostAggregator {
    private final String name;
    private final String fieldName;
    private final float[] probabilitys;

    @JsonCreator
    public HdrHistogramToQuantilesPostAggregator(
            @JsonProperty("name") String name,
            @JsonProperty("fieldName") String fieldName,
            @JsonProperty("probabilitys") float[] probabilitys
    ){
        this.name = name;
        this.fieldName = fieldName;
        this.probabilitys = probabilitys;
    }

    @Override
    public ColumnType getType(ColumnInspector signature){
        return ColumnType.LONG_ARRAY;
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
    public float[] getProbabilitys() {
        return probabilitys;
    }

    @Nullable
    @Override
    public Object compute(Map<String, Object> values) {
        HistogramSketch histogram = (HistogramSketch) values.get(fieldName);
        if(histogram == null){
            //return null;
            return new Long[probabilitys.length];
        }
        final Long[] counts = new Long[probabilitys.length];
        for (int i = 0; i < probabilitys.length; i++) {
            counts[i] = histogram.getValueAtPercentile(probabilitys[i] * 100);
        }
        return counts;
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
        CacheKeyBuilder builder = new CacheKeyBuilder(HdrHistogramModule.CACHE_TYPE_ID_OFFSET).appendByte(HdrHistogramModule.QUANTILES_HDRHISTOGRAM_TO_QUANTILES_CACHE_TYPE_ID)
                .appendString(fieldName);
        for (float probability : probabilitys) {
            builder.appendFloat(probability);
        }
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
        HdrHistogramToQuantilesPostAggregator that = (HdrHistogramToQuantilesPostAggregator) o;

        return Arrays.equals(probabilitys, that.probabilitys) &&
                name.equals(that.name) &&
                fieldName.equals(that.fieldName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, fieldName, Arrays.hashCode(probabilitys));
    }

    @Override
    public String toString() {
        return "HdrHistogramToQuantilesPostAggregator{" +
                "name='" + name + '\'' +
                ", fieldName='" + fieldName + '\'' +
                ", probabilitys=" + Arrays.toString(probabilitys) +
                '}';
    }
}
