package org.apache.druid.query.aggregation.sketch.hlld;

import com.zdjz.galaxy.sketch.hlld.Hll;
import com.zdjz.galaxy.sketch.hlld.HllUnion;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class HllMergeBufferAggregator implements BufferAggregator {
    private static final Logger LOG = new Logger(HllMergeBufferAggregator.class);
    private long lastTs = 0L;
    private int aggCount = 0;
    private final ColumnValueSelector<Hll> selector;
    private final int precision;
    private HllUnion union;
    private final byte[] emptyBytes;

    public HllMergeBufferAggregator(
            final ColumnValueSelector<Hll> selector,
            final int precision
    ){
        this.selector = selector;
        this.precision = precision;
        this.emptyBytes = new byte[HllUnion.getUpdatableSerializationBytes(precision)];
        union = new HllUnion(precision, ByteBuffer.wrap(emptyBytes));
        //LOG.error("HllMergeBufferAggregator gene:" + Thread.currentThread().getName() + "-" + Thread.currentThread().getId() + "-" + this);
    }

    @Override
    public void init(ByteBuffer buf, int position) {
        //LOG.error("HllMergeBufferAggregator init:" + Thread.currentThread().getName() + "-" + Thread.currentThread().getId() + "-" + this);
        final int oldPosition = buf.position();
        try {
            buf.position(position);
            buf.put(emptyBytes);
        }finally {
            buf.position(oldPosition);
        }
    }

    @Override
    public synchronized void aggregate(ByteBuffer buf, int position) {
        /*aggCount++;
        long ts = System.currentTimeMillis();
        if(ts - lastTs > 100){
            //LOG.warn("HllMergeBufferAggregator call");
            //LOG.error("HllMergeBufferAggregator call:" +  aggCount + "-" + Thread.currentThread().getId() + "-" + this);
            lastTs = ts;
        }*/
        final Hll hll = selector.getObject();
        if (hll == null) {
            return;
        }

        ByteBuffer mutationBuffer = buf.duplicate();
        mutationBuffer.position(position);

        union.resetByteBuffer(mutationBuffer);
        union.update(hll);
    }

    @Nullable
    @Override
    public synchronized Object get(ByteBuffer buf, int position) {
        //LOG.error("HllMergeBufferAggregator get:" +  aggCount  + "-" + Thread.currentThread().getId() + "-" + this);
        ByteBuffer mutationBuffer = buf.duplicate();
        mutationBuffer.position(position);

        HllUnion union = HllUnion.fromByteBuffer(mutationBuffer);
        return union.getResult();
    }

    @Override
    public float getFloat(ByteBuffer buf, int position) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public long getLong(ByteBuffer buf, int position) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void close() {

    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector){
        inspector.visit("selector", selector);
        inspector.visit("precision", precision);
    }
}
