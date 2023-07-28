package org.apache.druid.query.aggregation.sketch.hlld;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.zdjz.galaxy.sketch.hlld.Hll;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.cache.CacheKeyBuilder;

import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class HllToEstimatePostAggregator implements PostAggregator {
    private final String name;
    private final PostAggregator field;
    private final boolean round;

    @JsonCreator
    public HllToEstimatePostAggregator(
            @JsonProperty("name") final String name,
            @JsonProperty("field") final PostAggregator field,
            @JsonProperty("round") boolean round
    ) {
        this.name = name;
        this.field = field;
        this.round = round;
    }

    @Override
    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonProperty
    public PostAggregator getField() {
        return field;
    }

    @JsonProperty
    public boolean isRound() {
        return round;
    }

    @Override
    public Set<String> getDependentFields() {
        return field.getDependentFields();
    }

    @Override
    public Comparator<Double> getComparator() {
        return ArithmeticPostAggregator.DEFAULT_COMPARATOR;
    }

    @Override
    public Object compute(final Map<String, Object> combinedAggregators) {
        final Hll sketch = (Hll) field.compute(combinedAggregators);
        return round ? Math.round(sketch.size()) : sketch.size();
    }

    @Override
    public PostAggregator decorate(final Map<String, AggregatorFactory> aggregators) {
        return this;
    }

    @Override
    public String toString() {
        return "HllToEstimatePostAggregator{" +
                "name='" + name + '\'' +
                ", field=" + field +
                ", round=" + round +
                '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HllToEstimatePostAggregator)) {
            return false;
        }

        final HllToEstimatePostAggregator that = (HllToEstimatePostAggregator) o;
        return name.equals(that.name) && field.equals(that.field) && round == that.round;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, field, round);
    }

    @Override
    public byte[] getCacheKey() {
        CacheKeyBuilder builder = new CacheKeyBuilder(HllModule.CACHE_TYPE_ID_OFFSET).appendByte(HllModule.HLLD_TO_ESTIMATE_CACHE_TYPE_ID)
                .appendCacheable(field).appendBoolean(round);
        return builder.build();
    }

}
