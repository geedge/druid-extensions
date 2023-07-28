package org.apache.druid.query.aggregation.sketch.hlld;

import com.zdjz.galaxy.sketch.hlld.Hll;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class HllBufferAggregator implements BufferAggregator {
    private static final Logger LOG = new Logger(HllBufferAggregator.class);
    private long lastTs = 0L;
    private final ColumnValueSelector<Object> selector;
    private final int precision;
    private Hll hll;
    private final byte[] emptyBytes;

    public HllBufferAggregator(
            final ColumnValueSelector<Object> selector,
            final int precision
    ){
        this.selector = selector;
        this.precision = precision;
        this.emptyBytes = new byte[Hll.getUpdatableSerializationBytes(precision)];
        hll = new Hll(precision, ByteBuffer.wrap(emptyBytes));
        //LOG.error("HllBufferAggregator gene:" + Thread.currentThread().getName() + "-" + Thread.currentThread().getId());
    }

    @Override
    public void init(ByteBuffer buf, int position) {
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
        /*long ts = System.currentTimeMillis();
        if(ts - lastTs > 2000){
            //LOG.warn("HllBufferAggregator call");
            //LOG.error("HllBufferAggregator call");
            lastTs = ts;
        }*/

        final Object value = selector.getObject();
        if (value == null) {
            return;
        }

        ByteBuffer mutationBuffer = buf.duplicate();
        mutationBuffer.position(position);

        hll.resetByteBuffer(mutationBuffer);
        HllAggregator.updateHll(hll, value);
    }

    @Nullable
    @Override
    public synchronized Object get(ByteBuffer buf, int position) {
        ByteBuffer mutationBuffer = buf.duplicate();
        mutationBuffer.position(position);

        Hll hll = Hll.fromByteBuffer(mutationBuffer);
        return hll;
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
