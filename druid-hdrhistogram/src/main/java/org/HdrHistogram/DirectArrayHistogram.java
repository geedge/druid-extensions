package org.HdrHistogram;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DirectArrayHistogram extends AbstractHistogram implements Histogramer{
    long totalCount;
    int normalizingIndexOffset;
    private ByteBuffer byteBuffer;
    private int initPosition;

    public DirectArrayHistogram(final long lowestDiscernibleValue, final long highestTrackableValue,
                                final int numberOfSignificantValueDigits, ByteBuffer byteBuffer) {
        super(lowestDiscernibleValue, highestTrackableValue, numberOfSignificantValueDigits);
        this.byteBuffer = byteBuffer;
        this.initPosition = byteBuffer.position();
        wordSizeInBytes = 8;
    }

    // druid内部使用
    public void resetByteBuffer(ByteBuffer byteBuffer){
        this.byteBuffer = byteBuffer;
        this.initPosition = byteBuffer.position();
    }

    @Override
    long getCountAtIndex(int index) {
        int i = normalizeIndex(index, normalizingIndexOffset, countsArrayLength);
        return byteBuffer.getLong(initPosition + i * 8);
    }

    @Override
    long getCountAtNormalizedIndex(int index) {
        return byteBuffer.getLong(initPosition + index * 8);
    }

    @Override
    void incrementCountAtIndex(int index) {
        int i = normalizeIndex(index, normalizingIndexOffset, countsArrayLength);
        int pos = initPosition + i * 8;
        long val = byteBuffer.getLong(pos);
        byteBuffer.putLong(pos, val + 1);
    }

    @Override
    void addToCountAtIndex(int index, long value) {
        int i = normalizeIndex(index, normalizingIndexOffset, countsArrayLength);
        int pos = initPosition + i * 8;
        long val = byteBuffer.getLong(pos);
        byteBuffer.putLong(pos, val + value);
    }

    @Override
    void setCountAtIndex(int index, long value) {
        int i = normalizeIndex(index, normalizingIndexOffset, countsArrayLength);
        int pos = initPosition + i * 8;
        byteBuffer.putLong(pos, value);
    }

    @Override
    void setCountAtNormalizedIndex(int index, long value) {
        int pos = initPosition + index * 8;
        byteBuffer.putLong(pos, value);
    }

    @Override
    int getNormalizingIndexOffset() {
        return normalizingIndexOffset;
    }

    @Override
    void setNormalizingIndexOffset(int normalizingIndexOffset) {
        if(normalizingIndexOffset == 0){
            this.normalizingIndexOffset = normalizingIndexOffset;
        }else{
            throw new RuntimeException("cant not setNormalizingIndexOffset");
        }
    }

    @Override
    void setIntegerToDoubleValueConversionRatio(double integerToDoubleValueConversionRatio) {
        nonConcurrentSetIntegerToDoubleValueConversionRatio(integerToDoubleValueConversionRatio);
    }

    @Override
    void shiftNormalizingIndexByOffset(int offsetToAdd, boolean lowestHalfBucketPopulated, double newIntegerToDoubleValueConversionRatio) {
        nonConcurrentNormalizingIndexShift(offsetToAdd, lowestHalfBucketPopulated);
    }

    @Override
    void clearCounts() {
        for (int i = 0; i < countsArrayLength; i++) {
            byteBuffer.putLong(initPosition + i * 8, 0L);
        }
        totalCount = 0;
    }

    @Override
    public Histogramer makeCopy() {
        return miniCopy();
    }

    @Override
    public ArrayHistogram copy() {
        ArrayHistogram copy = new ArrayHistogram(this);
        copy.add(this);
        return copy;
    }

    public ArrayHistogram miniCopy() {
        ArrayHistogram copy = new ArrayHistogram(lowestDiscernibleValue, maxValue < highestTrackableValue ? Math.max(maxValue, lowestDiscernibleValue * 2) : highestTrackableValue, numberOfSignificantValueDigits);
        copy.add(this);
        return copy;
    }

    @Override
    public AbstractHistogram copyCorrectedForCoordinatedOmission(long expectedIntervalBetweenValueSamples) {
        Histogram copy = new Histogram(this);
        copy.addWhileCorrectingForCoordinatedOmission(this, expectedIntervalBetweenValueSamples);
        return copy;
    }

    @Override
    public long getTotalCount() {
        return totalCount;
    }

    @Override
    void setTotalCount(final long totalCount) {
        this.totalCount = totalCount;
    }

    @Override
    void incrementTotalCount() {
        totalCount++;
    }

    @Override
    void addToTotalCount(long value) {
        totalCount += value;
    }


    @Override
    int _getEstimatedFootprintInBytes() {
        return (512 + (8 * countsArrayLength));
    }

    @Override
    void resize(long newHighestTrackableValue) {
        throw new RuntimeException("cant not resize");
    }

    public static int getCountsArrayLength(long lowestDiscernibleValue, long highestTrackableValue, int numberOfSignificantValueDigits){
        Histogram his = new Histogram(lowestDiscernibleValue, highestTrackableValue, numberOfSignificantValueDigits, false);
        return his.countsArrayLength;
    }

    public static final int getUpdatableSerializationBytes(long lowestDiscernibleValue, long highestTrackableValue, int numberOfSignificantValueDigits){
        return getCountsArrayLength(lowestDiscernibleValue, highestTrackableValue, numberOfSignificantValueDigits) * 8;
    }

    @Override
    public List<Percentile> percentileList(int percentileTicksPerHalfDistance) {
        List<Percentile> percentiles = new ArrayList<>();
        for (HistogramIterationValue percentile : this.percentiles(percentileTicksPerHalfDistance)) {
            if(percentile.getCountAddedInThisIterationStep() > 0){
                percentiles.add(new Percentile(percentile.getValueIteratedTo(), percentile.getCountAddedInThisIterationStep(), percentile.getPercentile()));
            }
        }
        return percentiles;
    }

    @Override
    public Histogramer resetHistogram() {
        throw new UnsupportedOperationException("unsupported method");
    }

    @Override
    public Histogramer merge(Histogramer histogram) {
        if(histogram instanceof AbstractHistogram){
            this.add((AbstractHistogram)histogram);
            return this;
        }else if(histogram instanceof DirectMapHistogram){
            try {
                ((DirectMapHistogram)histogram).mergeInto(this);
                return this;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }else{
            throw new UnsupportedOperationException("unsupported method");
        }
    }

    @Override
    public byte[] toBytes() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(this.getNeededByteBufferCapacity());
        this.encodeIntoByteBuffer(byteBuffer);
        return byteBuffer2Bytes(byteBuffer);
    }
}
