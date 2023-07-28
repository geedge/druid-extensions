package org.apache.druid.query.aggregation.sketch.HdrHistogram;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramSketch;
import org.HdrHistogram.HistogramUnion;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.BaseObjectColumnValueSelector;

import javax.annotation.Nullable;

public class HdrHistogramMergeAggregator implements Aggregator {
    private static final Logger LOG = new Logger(HdrHistogramMergeAggregator.class);
    private long lastTs = 0L;
    private final BaseObjectColumnValueSelector<HistogramSketch> selector;
    private final long lowestDiscernibleValue;
    private final long highestTrackableValue;
    private final int numberOfSignificantValueDigits;
    private final boolean autoResize;
    private HistogramUnion union;

    public HdrHistogramMergeAggregator(
            BaseObjectColumnValueSelector<HistogramSketch> selector,
            long lowestDiscernibleValue,
            long highestTrackableValue,
            int numberOfSignificantValueDigits,
            boolean autoResize
    ) {
        this.selector = selector;
        this.lowestDiscernibleValue = lowestDiscernibleValue;
        this.highestTrackableValue = highestTrackableValue;
        this.numberOfSignificantValueDigits = numberOfSignificantValueDigits;
        this.autoResize = autoResize;
    }

    /*
     * This method is synchronized because it can be used during indexing,
     * and Druid can call aggregate() and get() concurrently.
     * See https://github.com/druid-io/druid/pull/3956
     */
    @Override
    public void aggregate() {
        /*long ts = System.currentTimeMillis();
        if(ts - lastTs > 2000){
            //LOG.warn("HdrHistogramMergeAggregator call");
            LOG.error("HdrHistogramMergeAggregator call");
            lastTs = ts;
        }*/
        HistogramSketch h = selector.getObject();
        if (h == null) {
            return;
        }

        if(union == null){
            this.union = new HistogramUnion(lowestDiscernibleValue, highestTrackableValue, numberOfSignificantValueDigits, autoResize);
        }

        synchronized (this) {
            union.update(h);
        }
    }

    /*
     * This method is synchronized because it can be used during indexing,
     * and Druid can call aggregate() and get() concurrently.
     * See https://github.com/druid-io/druid/pull/3956
     */
    @Nullable
    @Override
    public synchronized HistogramSketch get() {
        if(union == null){
            return null;
        }
        HistogramSketch result = union.getResult();
        /*if(result.getTotalCount() == 0){
            return null;
        }*/
        return result;
    }

    @Override
    public float getFloat() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public long getLong() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void close() {
        union = null;
    }
}
