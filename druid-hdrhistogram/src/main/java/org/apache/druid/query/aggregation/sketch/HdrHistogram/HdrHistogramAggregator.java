package org.apache.druid.query.aggregation.sketch.HdrHistogram;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramSketch;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.BaseLongColumnValueSelector;

import javax.annotation.Nullable;

public class HdrHistogramAggregator implements Aggregator {
    private static final Logger LOG = new Logger(HdrHistogramAggregator.class);
    private long lastTs = 0L;
    private final BaseLongColumnValueSelector selector;
    private final long lowestDiscernibleValue;
    private final long highestTrackableValue;
    private final int numberOfSignificantValueDigits;
    private final boolean autoResize;
    private HistogramSketch histogram;

    public HdrHistogramAggregator(
            BaseLongColumnValueSelector selector,
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
        long ts = System.currentTimeMillis();
        if(ts - lastTs > 2000){
            //LOG.warn("HdrHistogramAggregator call");
            //LOG.error("HdrHistogramAggregator call");
            lastTs = ts;
        }
        if(selector.isNull()){
            return;
        }

        long value = selector.getLong();
        if(value < 0){
            return;
        }

        if(histogram == null){
            this.histogram = new HistogramSketch(lowestDiscernibleValue, highestTrackableValue, numberOfSignificantValueDigits, autoResize);
        }

        synchronized (this) {
            histogram.recordValue(value);
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
        if(histogram == null){
            return null;
        }
        return histogram.copy();
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
        histogram = null;
    }
}