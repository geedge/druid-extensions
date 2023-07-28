package org.apache.druid.query.aggregation.sketch.hlld;

import com.zdjz.galaxy.sketch.hlld.Hll;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;
import java.util.List;

public class HllAggregator implements Aggregator {
    private static final Logger LOG = new Logger(HllAggregator.class);
    private long lastTs = 0L;
    private final ColumnValueSelector<Object> selector;
    protected final int precision;
    private Hll hll;

    public HllAggregator(
            final ColumnValueSelector<Object> selector,
            final int precision
    ) {
        this.selector = selector;
        this.precision = precision;
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
            //LOG.warn("HllAggregator call");
            LOG.error("HllAggregator call");
            lastTs = ts;
        }*/

        final Object value = selector.getObject();
        if (value == null) {
            return;
        }
        if(value instanceof String && ((String) value).isEmpty()){
            return;
        }

        if(hll == null){
            this.hll = new Hll(precision);
        }

        synchronized (this) {
            updateHll(hll, value);
        }
    }

    /*
     * This method is synchronized because it can be used during indexing,
     * and Druid can call aggregate() and get() concurrently.
     * See https://github.com/druid-io/druid/pull/3956
     */
    @Nullable
    @Override
    public synchronized Object get() {
        if(hll == null){
            return null;
        }
        return hll.copy();
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
        hll = null;
    }

    static void updateHll(final Hll hll, final Object value) {
        if (value instanceof Integer || value instanceof Long) {
            hll.add(((Number) value).longValue());
        } else if (value instanceof Float || value instanceof Double) {
            hll.add(((Number) value).doubleValue());
        } else if (value instanceof String) {
            hll.add((String) value);
        } else if (value instanceof List) {
            // noinspection unchecked
            List<String> list = (List<String>) value;
            for (String v : list) {
                hll.add(v);
            }
        } else if (value instanceof byte[]) {
            hll.add((byte[]) value);
        } else {
            throw new IAE("Unsupported type " + value.getClass());
        }
    }
}
