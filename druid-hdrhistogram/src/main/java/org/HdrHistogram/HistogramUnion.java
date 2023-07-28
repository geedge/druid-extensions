package org.HdrHistogram;

import java.nio.ByteBuffer;

public class HistogramUnion {
    public HistogramSketch impl;

    public HistogramUnion(final int numberOfSignificantValueDigits){
        impl = new HistogramSketch(numberOfSignificantValueDigits);
    }

    public HistogramUnion(final long lowestDiscernibleValue, final long highestTrackableValue,
                           final int numberOfSignificantValueDigits, final boolean autoResize){
        impl = new HistogramSketch(lowestDiscernibleValue, highestTrackableValue, numberOfSignificantValueDigits, autoResize);
    }

    public HistogramUnion(final long lowestDiscernibleValue, final long highestTrackableValue,
                          final int numberOfSignificantValueDigits, ByteBuffer byteBuffer) {
        impl = new HistogramSketch(new DirectArrayHistogram(lowestDiscernibleValue, highestTrackableValue, numberOfSignificantValueDigits, byteBuffer));
    }

    public HistogramUnion(HistogramSketch his) {
        impl = his;
    }

    // druid内部使用
    public void resetByteBuffer(ByteBuffer byteBuffer){
        ((DirectArrayHistogram)impl.hisImpl).resetByteBuffer(byteBuffer);
    }

    public void reset() {
        impl.reset();
    }

    public HistogramSketch getResult() {
        return impl;
    }

    public void update(final HistogramSketch his) {
        if(his != null){
            impl.hisImpl = unionImpl(his, impl);
        }
    }

    private static Histogramer unionImpl(HistogramSketch source, HistogramSketch dest) {
        return dest.hisImpl.merge(source.hisImpl);
    }
}
