package org.apache.druid.query.aggregation.sketch.hlld;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.zdjz.galaxy.sketch.hlld.Hll;
import com.zdjz.galaxy.sketch.hlld.HllUnion;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.*;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class HllAggregatorFactory extends AggregatorFactory {
    private static final Logger LOG = new Logger(HllAggregatorFactory.class);
    public static final boolean DEFAULT_ROUND = false;
    public static final int DEFAULT_PRECISION = 12;

    static final Comparator<Hll> COMPARATOR = Comparator.nullsFirst(Comparator.comparingDouble(Hll::size));

    protected final String name;
    protected final String fieldName;
    protected final int precision;
    protected final boolean round;

    public HllAggregatorFactory(
            @JsonProperty("name") final String name,
            @JsonProperty("fieldName") final String fieldName,
            @JsonProperty("precision") @Nullable final Integer precision,
            @JsonProperty("round") @Nullable final Boolean round
    ) {
        if (name == null) {
            throw new IAE("Must have a valid, non-null aggregator name");
        }
        if (fieldName == null) {
            throw new IAE("Parameter fieldName must be specified");
        }
        this.name = name;
        this.fieldName = fieldName;
        this.precision = precision == null ? DEFAULT_PRECISION : precision;
        this.round = round == null ? DEFAULT_ROUND : round;
    }

    @Override
    public Aggregator factorize(ColumnSelectorFactory columnSelectorFactory) {
        final ColumnValueSelector<Object> selector = columnSelectorFactory.makeColumnValueSelector(fieldName);
        return new HllAggregator(selector, precision);
    }

    @Override
    public BufferAggregator factorizeBuffered(ColumnSelectorFactory columnSelectorFactory) {
        final ColumnValueSelector<Object> selector = columnSelectorFactory.makeColumnValueSelector(fieldName);
        return new HllBufferAggregator(
                selector,
                precision
        );
    }

    @Override
    public Comparator getComparator() {
        return COMPARATOR;
    }

    @Override
    public Object combine(Object lhs, Object rhs) {
        if(lhs == null){
            return rhs;
        }else if(rhs == null){
            return lhs;
        }else{
            final HllUnion union = new HllUnion(precision);
            union.update((Hll) lhs);
            union.update((Hll) rhs);
            Hll result = union.getResult();
            return result;
        }
    }

    @Override
    public AggregateCombiner makeAggregateCombiner() {
        return new ObjectAggregateCombiner<Hll>() {
            private HllUnion union = null;

            @Override
            public void reset(ColumnValueSelector selector) {
                //LOG.error("HllAggregateCombiner reset:"  + "-" + Thread.currentThread().getId() + "-" + this);
                //union.reset();
                union = null;
                fold(selector);
            }

            @Override
            public void fold(ColumnValueSelector selector) {
                //LOG.error("HllAggregateCombiner fold:"  + "-" + Thread.currentThread().getId() + "-" + this);
                final Hll hll = (Hll) selector.getObject();
                if(hll != null){
                    if(union == null){
                        union = new HllUnion(precision);
                    }
                    union.update(hll);
                }else{
                    //LOG.error("HllAggregateCombiner fold_null:"  + "-" + Thread.currentThread().getId() + "-" + this);
                }
            }

            @Override
            public Class<Hll> classOfObject() {
                return Hll.class;
            }

            @Nullable
            @Override
            public Hll getObject() {
                //LOG.error("HllAggregateCombiner get:"  + "-" + Thread.currentThread().getId() + "-" + this);
                if(union == null){
                    return null;
                }else{
                    Hll result = union.getResult();
                    /*if(result.size() == 0){
                        return null;
                    }*/
                    return result;
                }
            }
        };
    }

    @Override
    public AggregatorFactory getCombiningFactory() {
        // 千万不能写错，好大一个坑
        return new HllMergeAggregatorFactory(name, name, precision, round);
    }

    @Override
    public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException {
        if (other.getName().equals(this.getName()) && other instanceof HllAggregatorFactory) {
            HllAggregatorFactory castedOther = (HllAggregatorFactory) other;

            return new HllMergeAggregatorFactory(name, name,
                    Math.max(precision, castedOther.precision),
                    round || castedOther.round
            );
        } else {
            throw new AggregatorFactoryNotMergeableException(this, other);
        }
    }

    @Override
    public List<AggregatorFactory> getRequiredColumns() {
        return Collections.singletonList(
                new HllAggregatorFactory(fieldName, fieldName, precision, round)
        );
    }

    @Override
    public Object deserialize(Object object) {
        return HllUtils.deserializeHll(object);
    }

    @Nullable
    @Override
    public Object finalizeComputation(@Nullable Object object) {
        if (object == null) {
            return null;
        }
        final Hll hll = (Hll) object;
        final double estimate = hll.size();

        if (round) {
            return Math.round(estimate);
        } else {
            return estimate;
        }
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
    public int getPrecision() {
        return precision;
    }

    @JsonProperty
    public boolean isRound() {
        return round;
    }

    @Override
    public String getTypeName() {
        return HllModule.HLLD_BUILD_TYPE_NAME;
    }

    @Override
    public List<String> requiredFields() {
        return Collections.singletonList(fieldName);
    }

    @Override
    public int getMaxIntermediateSize() {
        return Hll.getUpdatableSerializationBytes(precision);
    }

    @Override
    public byte[] getCacheKey() {
        return new CacheKeyBuilder(HllModule.CACHE_TYPE_ID_OFFSET).appendByte(HllModule.HLLD_BUILD_CACHE_TYPE_ID)
                .appendString(name).appendString(fieldName)
                .appendInt(precision).appendBoolean(round)
                .build();
    }

    @Override
    public boolean equals(final Object o){
        if (this == o) {
            return true;
        }
        if (o == null || !getClass().equals(o.getClass())) {
            return false;
        }

        HllAggregatorFactory that = (HllAggregatorFactory) o;
        return name.equals(that.name) && fieldName.equals(that.fieldName) &&
                precision == that.precision &&
                round == that.round
                ;
    }

    @Override
    public int hashCode(){
        return Objects.hash(name, fieldName, precision, round);
    }


    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                "name='" + name + '\'' +
                ", fieldName='" + fieldName + '\'' +
                ", precision=" + precision +
                ", round=" + round +
                '}';
    }
}
