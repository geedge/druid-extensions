package org.HdrHistogram;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class HistogramSketch {
    public Histogramer hisImpl = null;

    public HistogramSketch(final int numberOfSignificantValueDigits){
        hisImpl = new ArrayHistogram(numberOfSignificantValueDigits);
    }

    public HistogramSketch(final long lowestDiscernibleValue, final long highestTrackableValue,
                     final int numberOfSignificantValueDigits, final boolean autoResize){
        ArrayHistogram histogram = new ArrayHistogram(lowestDiscernibleValue, highestTrackableValue, numberOfSignificantValueDigits);
        histogram.setAutoResize(autoResize);
        hisImpl = histogram;
    }

    public HistogramSketch(final Histogramer that) {
        hisImpl = that;
    }

    /**
     * Copy constructor used by copy().
     */
    HistogramSketch(final HistogramSketch that) {
        hisImpl = that.hisImpl.makeCopy();
    }

    /**
     * 复制hisImpl到堆内存实例hisImpl
     */
    public HistogramSketch copy() {
        return new HistogramSketch(this);
    }

    public void reset() {
        hisImpl = hisImpl.resetHistogram();
    }

    public long getTotalCount(){
        return hisImpl.getTotalCount();
    }

    public void recordValue(long value){
        hisImpl.recordValue(value);
    }

    public void recordValueWithCount(long value, long count){
        hisImpl.recordValueWithCount(value, count);
    }

    public long getValueAtPercentile(double percentile){
        return hisImpl.getValueAtPercentile(percentile);
    }

    public List<Percentile> percentileList(int percentileTicksPerHalfDistance){
        return hisImpl.percentileList(percentileTicksPerHalfDistance);
    }

    public Map<String, Object> describe(){
        return hisImpl.describe();
    }

    public static final int getUpdatableSerializationBytes(long lowestDiscernibleValue, long highestTrackableValue, int numberOfSignificantValueDigits){
        return DirectArrayHistogram.getUpdatableSerializationBytes(lowestDiscernibleValue, highestTrackableValue, numberOfSignificantValueDigits);
    }

    public byte[] toBytes() {
        return hisImpl.toBytes();
    }

    public static HistogramSketch fromBytes(byte[] bytes) {
        return new HistogramSketch(ArrayHistogram.fromBytes(bytes));
    }

    public static HistogramSketch fromByteBuffer(ByteBuffer byteBuffer) {
        return new HistogramSketch(ArrayHistogram.fromByteBuffer(byteBuffer));
    }

    public static HistogramSketch wrapBytes(byte[] bytes) {
        return new HistogramSketch(DirectMapHistogram.wrapBytes(bytes));
    }

    public static HistogramSketch wrapByteBuffer(ByteBuffer byteBuffer) {
        return new HistogramSketch(DirectMapHistogram.wrapByteBuffer(byteBuffer));
    }
}
