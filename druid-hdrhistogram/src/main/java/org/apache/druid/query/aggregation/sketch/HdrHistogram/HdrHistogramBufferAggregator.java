package org.apache.druid.query.aggregation.sketch.HdrHistogram;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.HdrHistogram.DirectArrayHistogram;
import org.HdrHistogram.HistogramSketch;
import org.HdrHistogram.HistogramUnion;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.BaseLongColumnValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.IdentityHashMap;

public class HdrHistogramBufferAggregator implements BufferAggregator {
    private static final Logger LOG = new Logger(HdrHistogramAggregator.class);
    private long lastTs = 0L;
    private final BaseLongColumnValueSelector selector;
    private final long lowestDiscernibleValue;
    private final long highestTrackableValue;
    private final int numberOfSignificantValueDigits;
    private final boolean autoResize;
    private final int size;
    private final IdentityHashMap<ByteBuffer, Int2ObjectMap<DirectArrayHistogram>> histograms = new IdentityHashMap<>();

    public HdrHistogramBufferAggregator(
            BaseLongColumnValueSelector selector,
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
        //LOG.error("HdrHistogramBufferAggregator gene:" + Thread.currentThread().getName() + "-" + Thread.currentThread().getId());
    }

    @Override
    public synchronized void init(ByteBuffer buf, int position) {
        final int oldPosition = buf.position();
        try {
            buf.position(position);

            long highest = autoResize?HdrHistogramAggregatorFactory.BUFFER_AUTO_RESIZE_HIGHEST: highestTrackableValue;
            final DirectArrayHistogram histogram = new DirectArrayHistogram(lowestDiscernibleValue, highest, numberOfSignificantValueDigits, buf);
            histogram.reset();
            putUnion(buf, position, histogram);
        }finally {
            buf.position(oldPosition);
        }
    }

    @Override
    public synchronized void aggregate(ByteBuffer buf, int position) {
        long ts = System.currentTimeMillis();
        if(ts - lastTs > 2000){
            //LOG.warn("HdrHistogramBufferAggregator call");
            //LOG.error("HdrHistogramBufferAggregator call");
            lastTs = ts;
        }
        if(selector.isNull()){
            return;
        }

        final int oldPosition = buf.position();
        try {
            buf.position(position);

            DirectArrayHistogram histogram = histograms.get(buf).get(position);
            histogram.recordValue(selector.getLong());
        }finally{
            buf.position(oldPosition);
        }
    }

    @Nullable
    @Override
    public synchronized HistogramSketch get(ByteBuffer buf, int position) {
        DirectArrayHistogram histogram = histograms.get(buf).get(position);
        //return histogram.copy();
        return new HistogramSketch(histogram.miniCopy());
    }

    @Override
    public synchronized void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer) {
        DirectArrayHistogram histogram = histograms.get(oldBuffer).get(oldPosition);

        Int2ObjectMap<DirectArrayHistogram> map = histograms.get(oldBuffer);
        map.remove(oldPosition);
        if (map.isEmpty()) {
            histograms.remove(oldBuffer);
        }

        try {
            newBuffer.position(newPosition);
            histogram.resetByteBuffer(newBuffer);
            putUnion(newBuffer, newPosition, histogram);
        }finally {
            newBuffer.position(newPosition);
        }
    }

    private void putUnion(final ByteBuffer buffer, final int position, final DirectArrayHistogram histogram) {
        Int2ObjectMap<DirectArrayHistogram> map = histograms.computeIfAbsent(buffer, buf -> new Int2ObjectOpenHashMap<>());
        map.put(position, histogram);
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
        histograms.clear();
    }
}
