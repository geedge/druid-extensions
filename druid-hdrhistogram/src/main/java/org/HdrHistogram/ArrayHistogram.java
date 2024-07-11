package org.HdrHistogram; /**
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 *
 * @author Gil Tene
 */

import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.zip.DataFormatException;

/**
 * <h3>A High Dynamic Range (HDR) Histogram</h3>
 * <p>
 * {@link ArrayHistogram} supports the recording and analyzing sampled data value counts across a configurable integer value
 * range with configurable value precision within the range. Value precision is expressed as the number of significant
 * digits in the value recording, and provides control over value quantization behavior across the value range and the
 * subsequent value resolution at any given level.
 * <p>
 * For example, a Histogram could be configured to track the counts of observed integer values between 0 and
 * 3,600,000,000 while maintaining a value precision of 3 significant digits across that range. Value quantization
 * within the range will thus be no larger than 1/1,000th (or 0.1%) of any value. This example Histogram could
 * be used to track and analyze the counts of observed response times ranging between 1 microsecond and 1 hour
 * in magnitude, while maintaining a value resolution of 1 microsecond up to 1 millisecond, a resolution of
 * 1 millisecond (or better) up to one second, and a resolution of 1 second (or better) up to 1,000 seconds. At its
 * maximum tracked value (1 hour), it would still maintain a resolution of 3.6 seconds (or better).
 * <p>
 * Histogram tracks value counts in <b><code>long</code></b> fields. Smaller field types are available in the
 * {@link IntCountsHistogram} and {@link ShortCountsHistogram} implementations of
 * {@link AbstractHistogram}.
 * <p>
 * Auto-resizing: When constructed with no specified value range range (or when auto-resize is turned on with {@link
 * ArrayHistogram#setAutoResize}) a {@link ArrayHistogram} will auto-resize its dynamic range to include recorded values as
 * they are encountered. Note that recording calls that cause auto-resizing may take longer to execute, as resizing
 * incurs allocation and copying of internal data structures.
 * <p>
 * See package description for {@link org.HdrHistogram} for details.
 */

public class ArrayHistogram extends AbstractHistogram implements Histogramer{
    long totalCount;
    long[] counts;
    int normalizingIndexOffset;

    @Override
    long getCountAtIndex(final int index) {
        return counts[normalizeIndex(index, normalizingIndexOffset, countsArrayLength)];
    }

    @Override
    long getCountAtNormalizedIndex(final int index) {
        return counts[index];
    }

    @Override
    void incrementCountAtIndex(final int index) {
        counts[normalizeIndex(index, normalizingIndexOffset, countsArrayLength)]++;
    }

    @Override
    void addToCountAtIndex(final int index, final long value) {
        // 正常情况下normalizingIndexOffset = 0, index不用偏移
        counts[normalizeIndex(index, normalizingIndexOffset, countsArrayLength)] += value;
    }

    @Override
    void setCountAtIndex(int index, long value) {
        counts[normalizeIndex(index, normalizingIndexOffset, countsArrayLength)] = value;
    }

    @Override
    void setCountAtNormalizedIndex(int index, long value) {
        counts[index] = value;
    }

    @Override
    int getNormalizingIndexOffset() {
        return normalizingIndexOffset;
    }

    @Override
    void setNormalizingIndexOffset(int normalizingIndexOffset) {
        this.normalizingIndexOffset = normalizingIndexOffset;
    }

    @Override
    void setIntegerToDoubleValueConversionRatio(double integerToDoubleValueConversionRatio) {
        nonConcurrentSetIntegerToDoubleValueConversionRatio(integerToDoubleValueConversionRatio);
    }

    @Override
    void shiftNormalizingIndexByOffset(int offsetToAdd,
                                       boolean lowestHalfBucketPopulated,
                                       double newIntegerToDoubleValueConversionRatio) {
        nonConcurrentNormalizingIndexShift(offsetToAdd, lowestHalfBucketPopulated);
    }

    @Override
    void clearCounts() {
        Arrays.fill(counts, 0);
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
    public ArrayHistogram copyCorrectedForCoordinatedOmission(final long expectedIntervalBetweenValueSamples) {
        ArrayHistogram copy = new ArrayHistogram(this);
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
    void addToTotalCount(final long value) {
        totalCount += value;
    }

    @Override
    int _getEstimatedFootprintInBytes() {
        return (512 + (8 * counts.length));
    }

    @Override
    void resize(long newHighestTrackableValue) {
        int oldNormalizedZeroIndex = normalizeIndex(0, normalizingIndexOffset, countsArrayLength);

        establishSize(newHighestTrackableValue);

        int countsDelta = countsArrayLength - counts.length;

        counts = Arrays.copyOf(counts, countsArrayLength);

        if (oldNormalizedZeroIndex != 0) {
            // We need to shift the stuff from the zero index and up to the end of the array:
            int newNormalizedZeroIndex = oldNormalizedZeroIndex + countsDelta;
            int lengthToCopy = (countsArrayLength - countsDelta) - oldNormalizedZeroIndex;
            System.arraycopy(counts, oldNormalizedZeroIndex, counts, newNormalizedZeroIndex, lengthToCopy);
            Arrays.fill(counts, oldNormalizedZeroIndex, newNormalizedZeroIndex, 0);
        }
    }

    /**
     * Construct an auto-resizing histogram with a lowest discernible value of 1 and an auto-adjusting
     * highestTrackableValue. Can auto-resize up to track values up to (Long.MAX_VALUE / 2).
     *
     * @param numberOfSignificantValueDigits Specifies the precision to use. This is the number of significant
     *                                       decimal digits to which the histogram will maintain value resolution
     *                                       and separation. Must be a non-negative integer between 0 and 5.
     */
    public ArrayHistogram(final int numberOfSignificantValueDigits) {
        this(1, 2, numberOfSignificantValueDigits);
        setAutoResize(true);
    }

    /**
     * Construct a Histogram given the Highest value to be tracked and a number of significant decimal digits. The
     * histogram will be constructed to implicitly track (distinguish from 0) values as low as 1.
     *
     * @param highestTrackableValue          The highest value to be tracked by the histogram. Must be a positive
     *                                       integer that is {@literal >=} 2.
     * @param numberOfSignificantValueDigits Specifies the precision to use. This is the number of significant
     *                                       decimal digits to which the histogram will maintain value resolution
     *                                       and separation. Must be a non-negative integer between 0 and 5.
     */
    public ArrayHistogram(final long highestTrackableValue, final int numberOfSignificantValueDigits) {
        this(1, highestTrackableValue, numberOfSignificantValueDigits);
    }

    /**
     * Construct a Histogram given the Lowest and Highest values to be tracked and a number of significant
     * decimal digits. Providing a lowestDiscernibleValue is useful is situations where the units used
     * for the histogram's values are much smaller that the minimal accuracy required. E.g. when tracking
     * time values stated in nanosecond units, where the minimal accuracy required is a microsecond, the
     * proper value for lowestDiscernibleValue would be 1000.
     *
     * @param lowestDiscernibleValue         The lowest value that can be discerned (distinguished from 0) by the
     *                                       histogram. Must be a positive integer that is {@literal >=} 1. May be
     *                                       internally rounded down to nearest power of 2.
     * @param highestTrackableValue          The highest value to be tracked by the histogram. Must be a positive
     *                                       integer that is {@literal >=} (2 * lowestDiscernibleValue).
     * @param numberOfSignificantValueDigits Specifies the precision to use. This is the number of significant
     *                                       decimal digits to which the histogram will maintain value resolution
     *                                       and separation. Must be a non-negative integer between 0 and 5.
     */
    public ArrayHistogram(final long lowestDiscernibleValue, final long highestTrackableValue,
                          final int numberOfSignificantValueDigits) {
        this(lowestDiscernibleValue, highestTrackableValue, numberOfSignificantValueDigits, true);
    }

    /**
     * Construct a histogram with the same range settings as a given source histogram,
     * duplicating the source's start/end timestamps (but NOT its contents)
     * @param source The source histogram to duplicate
     */
    public ArrayHistogram(final AbstractHistogram source) {
        this(source, true);
    }

    ArrayHistogram(final AbstractHistogram source, boolean allocateCountsArray) {
        super(source);
        if (allocateCountsArray) {
            counts = new long[countsArrayLength];
        }
        wordSizeInBytes = 8;
    }

    ArrayHistogram(final long lowestDiscernibleValue, final long highestTrackableValue,
                   final int numberOfSignificantValueDigits, boolean allocateCountsArray) {
        super(lowestDiscernibleValue, highestTrackableValue, numberOfSignificantValueDigits);
        if (allocateCountsArray) {
            counts = new long[countsArrayLength];
        }
        // 写死 = 8
        wordSizeInBytes = 8;
    }

    /**
     * Construct a new histogram by decoding it from a ByteBuffer.
     * @param buffer The buffer to decode from
     * @param minBarForHighestTrackableValue Force highestTrackableValue to be set at least this high
     * @return The newly constructed histogram
     */
    public static ArrayHistogram decodeFromByteBuffer(final ByteBuffer buffer,
                                                      final long minBarForHighestTrackableValue) {
        return decodeFromByteBuffer(buffer, ArrayHistogram.class, minBarForHighestTrackableValue);
    }

    /**
     * Construct a new histogram by decoding it from a compressed form in a ByteBuffer.
     * @param buffer The buffer to decode from
     * @param minBarForHighestTrackableValue Force highestTrackableValue to be set at least this high
     * @return The newly constructed histogram
     * @throws DataFormatException on error parsing/decompressing the buffer
     */
    public static ArrayHistogram decodeFromCompressedByteBuffer(final ByteBuffer buffer,
                                                                final long minBarForHighestTrackableValue)
            throws DataFormatException {
        return decodeFromCompressedByteBuffer(buffer, ArrayHistogram.class, minBarForHighestTrackableValue);
    }

    private void readObject(final ObjectInputStream o)
            throws IOException, ClassNotFoundException {
        o.defaultReadObject();
    }

    /**
     * Construct a new Histogram by decoding it from a String containing a base64 encoded
     * compressed histogram representation.
     *
     * @param base64CompressedHistogramString A string containing a base64 encoding of a compressed histogram
     * @return A Histogream decoded from the string
     * @throws DataFormatException on error parsing/decompressing the input
     */
    public static ArrayHistogram fromString(final String base64CompressedHistogramString)
            throws DataFormatException {
        // 这还有个base64字符串的解析
        return decodeFromCompressedByteBuffer(
                ByteBuffer.wrap(Base64Helper.parseBase64Binary(base64CompressedHistogramString)),
                0);
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
    public Map<String, Object> describe() {
        long min = getMinValue();
        long max = getMaxValue(); // max = this.maxValue;
        long count = getTotalCount();
        double mean = getMean();
        long sum = (long) (mean * count);
        mean = Math.round(mean * 100.0) / 100.0;
        long p25 = getValueAtPercentile(25);
        long p50 = getValueAtPercentile(50);
        long p75 = getValueAtPercentile(75);
        long p90 = getValueAtPercentile(90);
        long p95 = getValueAtPercentile(95);
        long p99 = getValueAtPercentile(99);
        Map<String, Object> rst = new LinkedHashMap<>();
        rst.put("count", count);
        rst.put("mean", mean);
        rst.put("sum", sum);
        rst.put("min", min);
        rst.put("p25", p25);
        rst.put("p50", p50);
        rst.put("p75", p75);
        rst.put("p90", p90);
        rst.put("p95", p95);
        rst.put("p99", p99);
        rst.put("max", max);
        return rst;
    }

    @Override
    public Histogramer resetHistogram() {
        if(isAutoResize()){
            return new ArrayHistogram(this.numberOfSignificantValueDigits);
        }else{
            this.reset();
            return this;
        }
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

    public static ArrayHistogram fromBytes(byte[] bytes) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        return fromByteBuffer(byteBuffer);
    }

    public static ArrayHistogram fromByteBuffer(ByteBuffer byteBuffer) {
        int initPosition = byteBuffer.position();
        int cookie = byteBuffer.getInt(initPosition);
        if(DirectMapHistogram.getCookieBase(cookie) == DirectMapHistogram.V2CompressedEncodingCookieBase){
            try {
                return ArrayHistogram.decodeFromCompressedByteBuffer(byteBuffer, 2);
            } catch (DataFormatException e) {
                throw new RuntimeException(e);
            }
        }else if(DirectMapHistogram.getCookieBase(cookie) == DirectMapHistogram.V2EncodingCookieBase){
            return ArrayHistogram.decodeFromByteBuffer(byteBuffer, 2);
        }
        throw new UnsupportedOperationException("unsupported method");
    }
}
