package org.apache.druid.query.aggregation.sketch.HdrHistogram;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.HdrHistogram.HistogramSketch;
import org.HdrHistogram.HistogramUnion;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.aggregation.*;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class HdrHistogramAggregatorFactory extends AggregatorFactory {
    public static final long DEFAULT_LOWEST = 1;
    public static final long DEFAULT_HIGHEST = 2;
    public static final int DEFAULT_SIGNIFICANT = 1;
    public static final boolean DEFAULT_AUTO_RESIZE = true;
    public static final long BUFFER_AUTO_RESIZE_HIGHEST = 100000000L * 1000000L;
    public static final Comparator<HistogramSketch> COMPARATOR =
            Comparator.nullsFirst(Comparator.comparingLong(HistogramSketch::getTotalCount));

    protected final String name;
    protected final String fieldName;
    protected final long lowestDiscernibleValue;
    protected final long highestTrackableValue;
    protected final int numberOfSignificantValueDigits;
    protected final boolean autoResize; //默认是false
    protected final int updatableSerializationBytes;

    public HdrHistogramAggregatorFactory(
            @JsonProperty("name") String name,
            @JsonProperty("fieldName") String fieldName,
            @JsonProperty("lowestDiscernibleValue") @Nullable Long lowestDiscernibleValue,
            @JsonProperty("highestTrackableValue") @Nullable Long highestTrackableValue,
            @JsonProperty("numberOfSignificantValueDigits") @Nullable Integer numberOfSignificantValueDigits,
            @JsonProperty("autoResize") @Nullable Boolean autoResize
    ) {
        if (name == null) {
            throw new IAE("Must have a valid, non-null aggregator name");
        }
        if (fieldName == null) {
            throw new IAE("Parameter fieldName must be specified");
        }

        if(lowestDiscernibleValue == null){
            lowestDiscernibleValue = DEFAULT_LOWEST;
        }
        // Verify argument validity
        if (lowestDiscernibleValue < 1) {
            throw new IAE("lowestDiscernibleValue must be >= 1");
        }
        if (lowestDiscernibleValue > Long.MAX_VALUE / 2) {
            // prevent subsequent multiplication by 2 for highestTrackableValue check from overflowing
            throw new IAE("lowestDiscernibleValue must be <= Long.MAX_VALUE / 2");
        }
        if(highestTrackableValue == null){
            highestTrackableValue = DEFAULT_HIGHEST;
        }
        if (highestTrackableValue < 2L * lowestDiscernibleValue) {
            throw new IAE("highestTrackableValue must be >= 2 * lowestDiscernibleValue");
        }
        if(numberOfSignificantValueDigits == null){
            numberOfSignificantValueDigits = DEFAULT_SIGNIFICANT;
        }
        if ((numberOfSignificantValueDigits < 0) || (numberOfSignificantValueDigits > 5)) {
            throw new IAE("numberOfSignificantValueDigits must be between 0 and 5");
        }
        if(autoResize == null){
            autoResize = DEFAULT_AUTO_RESIZE;
        }

        this.name = name;
        this.fieldName = fieldName;
        this.lowestDiscernibleValue = lowestDiscernibleValue;
        this.highestTrackableValue = highestTrackableValue;
        this.numberOfSignificantValueDigits = numberOfSignificantValueDigits;
        this.autoResize = autoResize;
        this.updatableSerializationBytes = getUpdatableSerializationBytes();
    }

    @Override
    public Aggregator factorize(ColumnSelectorFactory metricFactory) {
        return new HdrHistogramAggregator(
                metricFactory.makeColumnValueSelector(fieldName),
                lowestDiscernibleValue,
                highestTrackableValue,
                numberOfSignificantValueDigits,
                autoResize
        );
    }

    @Override
    public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory) {
        return new HdrHistogramBufferAggregator(
                metricFactory.makeColumnValueSelector(fieldName),
                lowestDiscernibleValue,
                highestTrackableValue,
                numberOfSignificantValueDigits,
                autoResize,
                getMaxIntermediateSize()
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
            final HistogramUnion union = new HistogramUnion(lowestDiscernibleValue,highestTrackableValue,numberOfSignificantValueDigits,autoResize);
            union.update((HistogramSketch) lhs);
            union.update((HistogramSketch) rhs);
            HistogramSketch result = union.getResult();
            return result;
        }
    }

    @Override
    public AggregateCombiner makeAggregateCombiner() {
        return new ObjectAggregateCombiner<HistogramSketch>() {
            private HistogramUnion union = null;

            @Override
            public void reset(ColumnValueSelector selector) {
                //union.reset();
                union = null;
                fold(selector);
            }

            @Override
            public void fold(ColumnValueSelector selector) {
                HistogramSketch h = (HistogramSketch) selector.getObject();
                if(h != null){
                    if(union == null){
                        union = new HistogramUnion(lowestDiscernibleValue,highestTrackableValue,numberOfSignificantValueDigits,autoResize);
                    }
                    union.update(h);
                }
            }

            @Override
            public Class<HistogramSketch> classOfObject() {
                return HistogramSketch.class;
            }

            @Nullable
            @Override
            public HistogramSketch getObject() {
                if(union == null){
                    return null;
                }else{
                    HistogramSketch result = union.getResult();
                    /*if(result.getTotalCount() == 0){
                        return null;
                    }*/
                    return result;
                }
            }
        };
    }

    /*public Histogram geneHistogram() {
        Histogram histogram = new Histogram(lowestDiscernibleValue, highestTrackableValue, numberOfSignificantValueDigits);
        histogram.setAutoResize(autoResize);
        return histogram;
    }*/

    @Override
    public AggregatorFactory getCombiningFactory() {
        return new HdrHistogramMergeAggregatorFactory(name, name, lowestDiscernibleValue, highestTrackableValue, numberOfSignificantValueDigits, autoResize);
    }

    @Override
    public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException {
        if (other.getName().equals(this.getName()) && other instanceof HdrHistogramAggregatorFactory) {
            HdrHistogramAggregatorFactory castedOther = (HdrHistogramAggregatorFactory) other;

            return new HdrHistogramMergeAggregatorFactory(name, name,
                    Math.min(lowestDiscernibleValue, castedOther.lowestDiscernibleValue),
                    Math.max(highestTrackableValue, castedOther.highestTrackableValue),
                    Math.max(numberOfSignificantValueDigits, castedOther.numberOfSignificantValueDigits),
                    autoResize || castedOther.autoResize
            );
        } else {
            throw new AggregatorFactoryNotMergeableException(this, other);
        }
    }

    @Override
    public List<AggregatorFactory> getRequiredColumns() {
        return Collections.singletonList(
                new HdrHistogramAggregatorFactory(
                        fieldName,
                        fieldName,
                        lowestDiscernibleValue, highestTrackableValue, numberOfSignificantValueDigits, autoResize
                )
        );
    }

    @Override
    public AggregatorFactory withName(String newName) {
        return new HdrHistogramAggregatorFactory(newName, fieldName, lowestDiscernibleValue, highestTrackableValue, numberOfSignificantValueDigits, autoResize);
    }

    @Override
    public Object deserialize(Object object) {
        if (object == null) {
            return null;
        }
        return HistogramUtils.deserializeHistogram(object);
    }

    @Override
    public ColumnType getResultType() {
        //return ColumnType.LONG;
        return getIntermediateType();
    }

    @Nullable
    @Override
    public Object finalizeComputation(@Nullable Object object) {
        //return object == null ? null : ((HistogramSketch) object).getTotalCount();
        return object;
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
    public long getLowestDiscernibleValue() {
        return lowestDiscernibleValue;
    }

    @JsonProperty
    public long getHighestTrackableValue() {
        return highestTrackableValue;
    }

    @JsonProperty
    public int getNumberOfSignificantValueDigits() {
        return numberOfSignificantValueDigits;
    }

    @JsonProperty
    public boolean isAutoResize() {
        return autoResize;
    }

    /*
    没这个方法了, 新版本需要实现getIntermediateType方法
    @Override
    public String getTypeName() {
        return HdrHistogramModule.HDRHISTOGRAM_TYPE_NAME;
    }*/

    @Override
    public ColumnType getIntermediateType() {
        return HdrHistogramModule.TYPE;
    }

    @Override
    public List<String> requiredFields() {
        return Collections.singletonList(fieldName);
    }


    @Override
    public int getMaxIntermediateSize() {
        return updatableSerializationBytes == 0? getUpdatableSerializationBytes():updatableSerializationBytes;
    }

    private int getUpdatableSerializationBytes(){
        if(!autoResize){
            /*Histogram histogram = new Histogram(lowestDiscernibleValue, highestTrackableValue, numberOfSignificantValueDigits);
            histogram.setAutoResize(autoResize);
            return histogram.getNeededByteBufferCapacity();*/
            return  HistogramSketch.getUpdatableSerializationBytes(lowestDiscernibleValue, highestTrackableValue, numberOfSignificantValueDigits);
        }else{
            //return (1 << 10) * 512;
            return HistogramSketch.getUpdatableSerializationBytes(lowestDiscernibleValue, BUFFER_AUTO_RESIZE_HIGHEST, numberOfSignificantValueDigits);
        }
    }

    @Override
    public byte[] getCacheKey() {
        return new CacheKeyBuilder(HdrHistogramModule.CACHE_TYPE_ID_OFFSET).appendByte(HdrHistogramModule.QUANTILES_HDRHISTOGRAM_BUILD_CACHE_TYPE_ID)
                .appendString(name).appendString(fieldName)
                .appendDouble(lowestDiscernibleValue).appendDouble(highestTrackableValue)
                .appendInt(numberOfSignificantValueDigits).appendBoolean(autoResize)
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

        HdrHistogramAggregatorFactory that = (HdrHistogramAggregatorFactory) o;
        return name.equals(that.name) && fieldName.equals(that.fieldName) &&
                lowestDiscernibleValue == that.lowestDiscernibleValue &&
                highestTrackableValue == that.highestTrackableValue &&
                numberOfSignificantValueDigits == that.numberOfSignificantValueDigits &&
                autoResize == that.autoResize
                ;
    }

    @Override
    public int hashCode(){
        return Objects.hash(name, fieldName, lowestDiscernibleValue, highestTrackableValue, numberOfSignificantValueDigits, autoResize);
    }


    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                "name='" + name + '\'' +
                ", fieldName='" + fieldName + '\'' +
                ", lowestDiscernibleValue=" + lowestDiscernibleValue +
                ", highestTrackableValue=" + highestTrackableValue +
                ", numberOfSignificantValueDigits=" + numberOfSignificantValueDigits +
                ", autoResize=" + autoResize +
                '}';
    }
}
