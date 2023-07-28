package org.apache.druid.query.aggregation.sketch.HdrHistogram;

import it.unimi.dsi.fastutil.bytes.ByteArrays;
import org.HdrHistogram.HistogramSketch;
import org.apache.druid.segment.data.ObjectStrategy;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class HdrHistogramObjectStrategy implements ObjectStrategy<HistogramSketch> {

    @Override
    public Class<HistogramSketch> getClazz() {
        return HistogramSketch.class;
    }

    @Nullable
    @Override
    public HistogramSketch fromByteBuffer(ByteBuffer buffer, int numBytes) {
        buffer.limit(buffer.position() + numBytes);
        return HistogramSketch.wrapByteBuffer(buffer);
    }

    @Nullable
    @Override
    public byte[] toBytes(@Nullable HistogramSketch h) {
        if (h == null) {
            return ByteArrays.EMPTY_ARRAY;
        }

        return h.toBytes();
    }

    @Override
    public int compare(HistogramSketch o1, HistogramSketch o2) {
        return HdrHistogramAggregatorFactory.COMPARATOR.compare(o1, o2);
    }
}
