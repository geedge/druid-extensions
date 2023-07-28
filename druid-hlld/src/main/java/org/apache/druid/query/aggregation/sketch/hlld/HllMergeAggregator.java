package org.apache.druid.query.aggregation.sketch.hlld;

import com.zdjz.galaxy.sketch.hlld.Hll;
import com.zdjz.galaxy.sketch.hlld.HllUnion;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.ColumnValueSelector;

public class HllMergeAggregator implements Aggregator {
    private static final Logger LOG = new Logger(HllMergeAggregator.class);
    private long lastTs = 0L;
    private int aggCount = 0;
    private final ColumnValueSelector<Hll> selector;
    protected final int precision;
    private HllUnion union;

    public HllMergeAggregator(
            final ColumnValueSelector<Hll> selector,
            final int precision
    ) {
        this.selector = selector;
        this.precision = precision;
        //LOG.error("HllMergeAggregator gene:" + Thread.currentThread().getName() + "-" + Thread.currentThread().getId() + "-" + this);
    }

    /**
     * This method is synchronized because it can be used during indexing,
     * and Druid can call aggregate() and get() concurrently.
     * See https://github.com/druid-io/druid/pull/3956
     */
    @Override
    public void aggregate() {
        /*aggCount++;
        long ts = System.currentTimeMillis();
        if(ts - lastTs > 100){
            //LOG.warn("HllMergeAggregator call");
            //LOG.error("HllMergeAggregator call:"  + "-" + Thread.currentThread().getId() + "-" + this);
            lastTs = ts;
        }*/
        final Hll hll = selector.getObject();
        if (hll == null) {
            return;
        }

        if(union == null){
            this.union = new HllUnion(precision);
        }

        synchronized (this) {
            union.update(hll);
        }
    }

    /**
     * This method is synchronized because it can be used during indexing,
     * and Druid can call aggregate() and get() concurrently.
     * See https://github.com/druid-io/druid/pull/3956
     */
    @Override
    public synchronized Object get() {
        //LOG.error("HllMergeAggregator get:" +  aggCount  + "-" + Thread.currentThread().getId() + "-" + this);
        if(union == null){
            return null;
        }
        Hll result = union.getResult();
        /*if(result.size() == 0){
            return null;
        }*/
        return result;
    }

    @Override
    public void close() {
        union = null;
    }

    @Override
    public float getFloat() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public long getLong() {
        throw new UnsupportedOperationException("Not implemented");
    }
}
