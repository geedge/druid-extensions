package org.apache.druid.query.aggregation.sketch.HdrHistogram;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.HdrHistogram.*;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.IdentityHashMap;

public class HdrHistogramMergeBufferAggregator implements BufferAggregator {
    private static final Logger LOG = new Logger(HdrHistogramAggregator.class);
    private long lastTs = 0L;
    private final BaseObjectColumnValueSelector<HistogramSketch> selector;
    private final long lowestDiscernibleValue;
    private final long highestTrackableValue;
    private final int numberOfSignificantValueDigits;
    private final boolean autoResize;
    private final int size;
    private final IdentityHashMap<ByteBuffer, Int2ObjectMap<HistogramUnion>> histograms = new IdentityHashMap<>();

    public HdrHistogramMergeBufferAggregator(
            BaseObjectColumnValueSelector<HistogramSketch> selector,
            long lowestDiscernibleValue,
            long highestTrackableValue,
            int numberOfSignificantValueDigits,
            boolean autoResize,
            int size
    ) {
        this.selector = selector;
        this.lowestDiscernibleValue = lowestDiscernibleValue;
        this.highestTrackableValue = highestTrackableValue;
        this.numberOfSignificantValueDigits = numberOfSignificantValueDigits;
        this.autoResize = autoResize;
        this.size = size;
        //LOG.error("HdrHistogramMergeBufferAggregator gene:" + Thread.currentThread().getName() + "-" + Thread.currentThread().getId());
    }

    @Override
    public synchronized void init(ByteBuffer buf, int position) {
        final int oldPosition = buf.position();
        try {
            buf.position(position);

            long highest = autoResize?HdrHistogramAggregatorFactory.BUFFER_AUTO_RESIZE_HIGHEST: highestTrackableValue;
            final DirectArrayHistogram histogram = new DirectArrayHistogram(lowestDiscernibleValue, highest, numberOfSignificantValueDigits, buf);
            histogram.reset();
            HistogramUnion union = new HistogramUnion(new HistogramSketch(histogram));
            putUnion(buf, position, union);
        }finally {
            buf.position(oldPosition);
        }
    }

    @Override
    public synchronized void aggregate(ByteBuffer buf, int position) {
        /*long ts = System.currentTimeMillis();
        if(ts - lastTs > 2000){
            //LOG.warn("HdrHistogramMergeBufferAggregator call");
            LOG.error("HdrHistogramMergeBufferAggregator call");
            lastTs = ts;
        }*/
        HistogramSketch h = selector.getObject();
        if (h == null) {
            return;
        }

        final int oldPosition = buf.position();
        try {
            buf.position(position);

            HistogramUnion union = histograms.get(buf).get(position);
            union.update(h);
        }finally{
            buf.position(oldPosition);
        }
    }

    @Nullable
    @Override
    public synchronized HistogramSketch get(ByteBuffer buf, int position) {
        //LOG.error("HdrHistogramMergeBufferAggregator get:" +  0  + "-" + Thread.currentThread().getId() + "-" + this);
        HistogramUnion union = histograms.get(buf).get(position);
        //return histogram.copy();
        return union.getResult().copy();
    }

    @Override
    public synchronized void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer) {
        HistogramUnion union = histograms.get(oldBuffer).get(oldPosition);

        Int2ObjectMap<HistogramUnion> map = histograms.get(oldBuffer);
        map.remove(oldPosition);
        if (map.isEmpty()) {
            histograms.remove(oldBuffer);
        }

        try {
            newBuffer.position(newPosition);
            union.resetByteBuffer(newBuffer);
            putUnion(newBuffer, newPosition, union);
        }finally {
            newBuffer.position(newPosition);
        }
    }

    private void putUnion(final ByteBuffer buffer, final int position, final HistogramUnion union) {
        Int2ObjectMap<HistogramUnion> map = histograms.computeIfAbsent(buffer, buf -> new Int2ObjectOpenHashMap<>());
        map.put(position, union);
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
    }
}
