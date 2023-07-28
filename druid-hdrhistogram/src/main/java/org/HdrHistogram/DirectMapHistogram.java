package org.HdrHistogram;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import static java.nio.ByteOrder.BIG_ENDIAN;

/**
 * 直接映射字节数组到Histogram，只读的Histogram，用于druid查询，减少gc减少计算，序列化后的是稀疏数组的形式
 */
public class DirectMapHistogram implements Histogramer{
    static final int V2maxWordSizeInBytes = 9; // LEB128-64b9B + ZigZag require up to 9 bytes per word
    static final int V2EncodingCookieBase = 0x1c849303;
    static final int V2CompressedEncodingCookieBase = 0x1c849304;

    final ByteBuffer byteBuffer;
    final int initPosition;
    long totalCount;

    private DirectMapHistogram(ByteBuffer byteBuffer) {
        int initPosition = byteBuffer.position();
        this.byteBuffer = byteBuffer;
        this.initPosition = initPosition;
        this.totalCount = -1;
    }

    public static boolean byteBufferCanToDirectMapHistogram(ByteBuffer byteBuffer) {
        int initPosition = byteBuffer.position();
        int cookie = byteBuffer.getInt(initPosition);
        return getCookieBase(cookie) == V2EncodingCookieBase || getCookieBase(cookie) == V2CompressedEncodingCookieBase;
    }

    public static DirectMapHistogram wrapBytes(byte[] bytes) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        return wrapByteBuffer(byteBuffer);
    }

    public static DirectMapHistogram wrapByteBuffer(ByteBuffer byteBuffer) {
        if(byteBufferCanToDirectMapHistogram(byteBuffer)){
            DirectMapHistogram hll = new DirectMapHistogram(byteBuffer);
            return hll;
        }
        throw  new RuntimeException("can not wrapByteBuffer");
    }

    public void mergeInto(AbstractHistogram histogram) throws Exception{
        int cookie = byteBuffer.getInt(initPosition);
        if(getCookieBase(cookie) == V2CompressedEncodingCookieBase){
            final int lengthOfCompressedContents = byteBuffer.getInt(initPosition + 4);
            final Inflater decompressor = new Inflater();

            if (byteBuffer.hasArray()) {
                decompressor.setInput(byteBuffer.array(), initPosition + 8, lengthOfCompressedContents);
            } else {
                byte[] compressedContents = new byte[lengthOfCompressedContents];
                byteBuffer.position(initPosition + 8);
                try {
                    byteBuffer.get(compressedContents);
                    decompressor.setInput(compressedContents);
                }finally {
                    byteBuffer.position(initPosition);
                }
            }
            final int headerSize = 40;
            final ByteBuffer headerBuffer = ByteBuffer.allocate(headerSize).order(BIG_ENDIAN);
            decompressor.inflate(headerBuffer.array());

            cookie = headerBuffer.getInt();
            final int payloadLengthInBytes;
            final int normalizingIndexOffset;
            final int numberOfSignificantValueDigits;
            final long lowestTrackableUnitValue;
            long highestTrackableValue;
            final double integerToDoubleValueConversionRatio;

            assert getCookieBase(cookie) == V2EncodingCookieBase;

            payloadLengthInBytes = headerBuffer.getInt(4);
            normalizingIndexOffset = headerBuffer.getInt(8);
            numberOfSignificantValueDigits = headerBuffer.getInt( 12);
            lowestTrackableUnitValue = headerBuffer.getLong(16);
            highestTrackableValue = headerBuffer.getLong(24);
            integerToDoubleValueConversionRatio = headerBuffer.getDouble(32);

            highestTrackableValue = Math.max(highestTrackableValue, 2);

            final long largestValueWithSingleUnitResolution = 2 * (long) Math.pow(10, numberOfSignificantValueDigits);
            final int unitMagnitude = (int) (Math.log(lowestTrackableUnitValue)/Math.log(2));
            final long unitMagnitudeMask = (1 << unitMagnitude) - 1;
            int subBucketCountMagnitude = (int) Math.ceil(Math.log(largestValueWithSingleUnitResolution)/Math.log(2));
            final int subBucketHalfCountMagnitude = subBucketCountMagnitude - 1;
            final int subBucketCount = 1 << subBucketCountMagnitude;
            final int subBucketHalfCount = subBucketCount / 2;
            final long subBucketMask = ((long)subBucketCount - 1) << unitMagnitude;
            if (subBucketCountMagnitude + unitMagnitude > 62) {
                // subBucketCount entries can't be represented, with unitMagnitude applied, in a positive long.
                // Technically it still sort of works if their sum is 63: you can represent all but the last number
                // in the shifted subBucketCount. However, the utility of such a histogram vs ones whose magnitude here
                // fits in 62 bits is debatable, and it makes it harder to work through the logic.
                // Sums larger than 64 are totally broken as leadingZeroCountBase would go negative.
                throw new IllegalArgumentException("Cannot represent numberOfSignificantValueDigits worth of values " +
                        "beyond lowestDiscernibleValue");
            }

            final int expectedCapacity = payloadLengthInBytes;

            ByteBuffer sourceBuffer = ByteBuffer.allocate(expectedCapacity).order(BIG_ENDIAN);
            int decompressedByteCount = decompressor.inflate(sourceBuffer.array());
            decompressor.end(); // 必须手动调用，否则快速调用可能内存溢出(堆外内存)
            if ((payloadLengthInBytes != Integer.MAX_VALUE) && (decompressedByteCount < payloadLengthInBytes)) {
                throw new IllegalArgumentException("The buffer does not contain the indicated payload amount");
            }
            assert decompressedByteCount == expectedCapacity;

            int dstIndex = 0;
            int endPosition = sourceBuffer.position() + expectedCapacity; //期望的结束读取的索引
            while (sourceBuffer.position() < endPosition) {
                long count;
                int zerosCount = 0;
                // V2 encoding format uses a long encoded in a ZigZag LEB128 format (up to V2maxWordSizeInBytes):
                count = ZigZagEncoding.getLong(sourceBuffer);
                if (count < 0) {
                    long zc = -count; // 0值的连续个数
                    if (zc > Integer.MAX_VALUE) {
                        throw new IllegalArgumentException(
                                "An encoded zero count of > Integer.MAX_VALUE was encountered in the source");
                    }
                    zerosCount = (int) zc;
                }
                if (zerosCount > 0) {
                    dstIndex += zerosCount; // No need to set zeros in array. Just skip them.
                } else {
                    // 单个非连续的0也会被输出
                    if(count > 0){
                        long value = valueFromIndex(dstIndex, subBucketHalfCountMagnitude, subBucketHalfCount, unitMagnitude);
                        histogram.recordValueWithCount(value, count);
                    }
                    dstIndex++;
                }
            }

        }else if(getCookieBase(cookie) == V2EncodingCookieBase){
            final int payloadLengthInBytes;
            final int normalizingIndexOffset;
            final int numberOfSignificantValueDigits;
            final long lowestTrackableUnitValue;
            long highestTrackableValue;
            final double integerToDoubleValueConversionRatio;

            payloadLengthInBytes = byteBuffer.getInt(initPosition + 4);
            normalizingIndexOffset = byteBuffer.getInt(initPosition + 8);
            numberOfSignificantValueDigits = byteBuffer.getInt(initPosition + 12);
            lowestTrackableUnitValue = byteBuffer.getLong(initPosition + 16);
            highestTrackableValue = byteBuffer.getLong(initPosition + 24);
            integerToDoubleValueConversionRatio = byteBuffer.getDouble(initPosition + 32);

            highestTrackableValue = Math.max(highestTrackableValue, 2);

            final long largestValueWithSingleUnitResolution = 2 * (long) Math.pow(10, numberOfSignificantValueDigits);
            final int unitMagnitude = (int) (Math.log(lowestTrackableUnitValue)/Math.log(2));
            final long unitMagnitudeMask = (1 << unitMagnitude) - 1;
            int subBucketCountMagnitude = (int) Math.ceil(Math.log(largestValueWithSingleUnitResolution)/Math.log(2));
            final int subBucketHalfCountMagnitude = subBucketCountMagnitude - 1;
            final int subBucketCount = 1 << subBucketCountMagnitude;
            final int subBucketHalfCount = subBucketCount / 2;
            final long subBucketMask = ((long)subBucketCount - 1) << unitMagnitude;
            if (subBucketCountMagnitude + unitMagnitude > 62) {
                // subBucketCount entries can't be represented, with unitMagnitude applied, in a positive long.
                // Technically it still sort of works if their sum is 63: you can represent all but the last number
                // in the shifted subBucketCount. However, the utility of such a histogram vs ones whose magnitude here
                // fits in 62 bits is debatable, and it makes it harder to work through the logic.
                // Sums larger than 64 are totally broken as leadingZeroCountBase would go negative.
                throw new IllegalArgumentException("Cannot represent numberOfSignificantValueDigits worth of values " +
                        "beyond lowestDiscernibleValue");
            }

            final int expectedCapacity =payloadLengthInBytes;
            assert expectedCapacity == payloadLengthInBytes;
            if(expectedCapacity > byteBuffer.limit() - 40){
                throw new IllegalArgumentException("The buffer does not contain the full Histogram payload");
            }
            final int position = initPosition + 40;
            final int lengthInBytes = expectedCapacity;
            final int wordSizeInBytes = V2maxWordSizeInBytes;
            // fillCountsArrayFromSourceBuffer

            ByteBuffer sourceBuffer = byteBuffer.duplicate();
            sourceBuffer.position(position);
            final long maxAllowableCountInHistigram = Long.MAX_VALUE;
            int dstIndex = 0;
            int endPosition = sourceBuffer.position() + lengthInBytes; //期望的结束读取的索引
            while (sourceBuffer.position() < endPosition) {
                long count;
                int zerosCount = 0;
                // V2 encoding format uses a long encoded in a ZigZag LEB128 format (up to V2maxWordSizeInBytes):
                count = ZigZagEncoding.getLong(sourceBuffer);
                if (count < 0) {
                    long zc = -count; // 0值的连续个数
                    if (zc > Integer.MAX_VALUE) {
                        throw new IllegalArgumentException(
                                "An encoded zero count of > Integer.MAX_VALUE was encountered in the source");
                    }
                    zerosCount = (int) zc;
                }
                if (zerosCount > 0) {
                    dstIndex += zerosCount; // No need to set zeros in array. Just skip them.
                } else {
                    // 单个非连续的0也会被输出
                    if(count > 0){
                        long value = valueFromIndex(dstIndex, subBucketHalfCountMagnitude, subBucketHalfCount, unitMagnitude);
                        histogram.recordValueWithCount(value, count);
                    }
                    dstIndex++;
                }
            }
        }else{
            throw  new RuntimeException("can not wrapByteBuffer");
        }
    }

    final long valueFromIndex(final int index, int subBucketHalfCountMagnitude, int subBucketHalfCount, int unitMagnitude) {
        int bucketIndex = (index >> subBucketHalfCountMagnitude) - 1;
        int subBucketIndex = (index & (subBucketHalfCount - 1)) + subBucketHalfCount;
        if (bucketIndex < 0) {
            subBucketIndex -= subBucketHalfCount;
            bucketIndex = 0;
        }
        return valueFromIndex(bucketIndex, subBucketIndex, unitMagnitude);
    }

    private long valueFromIndex(final int bucketIndex, final int subBucketIndex, int unitMagnitude) {
        return ((long) subBucketIndex) << (bucketIndex + unitMagnitude);
    }

    static int getCookieBase(final int cookie) {
        return (cookie & ~0xf0);
    }

    @Override
    public long getTotalCount() {
        if(totalCount >= 0){
            return totalCount;
        }
        try {
            totalCount = 0;
            int cookie = byteBuffer.getInt(initPosition);
            if(getCookieBase(cookie) == V2CompressedEncodingCookieBase){
                final int lengthOfCompressedContents = byteBuffer.getInt(initPosition + 4);
                final Inflater decompressor = new Inflater();

                if (byteBuffer.hasArray()) {
                    decompressor.setInput(byteBuffer.array(), initPosition + 8, lengthOfCompressedContents);
                } else {
                    byte[] compressedContents = new byte[lengthOfCompressedContents];
                    byteBuffer.position(initPosition + 8);
                    try {
                        byteBuffer.get(compressedContents);
                        decompressor.setInput(compressedContents);
                    }finally {
                        byteBuffer.position(initPosition);
                    }
                }
                final int headerSize = 40;
                final ByteBuffer headerBuffer = ByteBuffer.allocate(headerSize).order(BIG_ENDIAN);
                decompressor.inflate(headerBuffer.array());

                cookie = headerBuffer.getInt();
                final int payloadLengthInBytes;
                final int normalizingIndexOffset;
                final int numberOfSignificantValueDigits;
                final long lowestTrackableUnitValue;
                long highestTrackableValue;
                final double integerToDoubleValueConversionRatio;

                assert getCookieBase(cookie) == V2EncodingCookieBase;

                payloadLengthInBytes = headerBuffer.getInt(4);
                normalizingIndexOffset = headerBuffer.getInt(8);
                numberOfSignificantValueDigits = headerBuffer.getInt( 12);
                lowestTrackableUnitValue = headerBuffer.getLong(16);
                highestTrackableValue = headerBuffer.getLong(24);
                integerToDoubleValueConversionRatio = headerBuffer.getDouble(32);

                highestTrackableValue = Math.max(highestTrackableValue, 2);

                final long largestValueWithSingleUnitResolution = 2 * (long) Math.pow(10, numberOfSignificantValueDigits);
                final int unitMagnitude = (int) (Math.log(lowestTrackableUnitValue)/Math.log(2));
                final long unitMagnitudeMask = (1 << unitMagnitude) - 1;
                int subBucketCountMagnitude = (int) Math.ceil(Math.log(largestValueWithSingleUnitResolution)/Math.log(2));
                final int subBucketHalfCountMagnitude = subBucketCountMagnitude - 1;
                final int subBucketCount = 1 << subBucketCountMagnitude;
                final int subBucketHalfCount = subBucketCount / 2;
                final long subBucketMask = ((long)subBucketCount - 1) << unitMagnitude;
                if (subBucketCountMagnitude + unitMagnitude > 62) {
                    // subBucketCount entries can't be represented, with unitMagnitude applied, in a positive long.
                    // Technically it still sort of works if their sum is 63: you can represent all but the last number
                    // in the shifted subBucketCount. However, the utility of such a histogram vs ones whose magnitude here
                    // fits in 62 bits is debatable, and it makes it harder to work through the logic.
                    // Sums larger than 64 are totally broken as leadingZeroCountBase would go negative.
                    throw new IllegalArgumentException("Cannot represent numberOfSignificantValueDigits worth of values " +
                            "beyond lowestDiscernibleValue");
                }

                final int expectedCapacity = payloadLengthInBytes;

                ByteBuffer sourceBuffer = ByteBuffer.allocate(expectedCapacity).order(BIG_ENDIAN);
                int decompressedByteCount = decompressor.inflate(sourceBuffer.array());
                decompressor.end(); // 必须手动调用，否则快速调用可能内存溢出(堆外内存)
                if ((payloadLengthInBytes != Integer.MAX_VALUE) && (decompressedByteCount < payloadLengthInBytes)) {
                    throw new IllegalArgumentException("The buffer does not contain the indicated payload amount");
                }
                assert decompressedByteCount == expectedCapacity;

                int dstIndex = 0;
                int endPosition = sourceBuffer.position() + expectedCapacity; //期望的结束读取的索引
                while (sourceBuffer.position() < endPosition) {
                    long count;
                    int zerosCount = 0;
                    // V2 encoding format uses a long encoded in a ZigZag LEB128 format (up to V2maxWordSizeInBytes):
                    count = ZigZagEncoding.getLong(sourceBuffer);
                    if (count < 0) {
                        long zc = -count; // 0值的连续个数
                        if (zc > Integer.MAX_VALUE) {
                            throw new IllegalArgumentException(
                                    "An encoded zero count of > Integer.MAX_VALUE was encountered in the source");
                        }
                        zerosCount = (int) zc;
                    }
                    if (zerosCount > 0) {
                        dstIndex += zerosCount; // No need to set zeros in array. Just skip them.
                    } else {
                        // 单个非连续的0也会被输出
                        if(count > 0){
                            //long value = valueFromIndex(dstIndex, subBucketHalfCountMagnitude, subBucketHalfCount, unitMagnitude);
                            //histogram.recordValueWithCount(value, count);
                            totalCount += count;
                        }
                        dstIndex++;
                    }
                }
                return totalCount;
            }else if(getCookieBase(cookie) == V2EncodingCookieBase){
                final int payloadLengthInBytes;
                final int normalizingIndexOffset;
                final int numberOfSignificantValueDigits;
                final long lowestTrackableUnitValue;
                long highestTrackableValue;
                final double integerToDoubleValueConversionRatio;

                payloadLengthInBytes = byteBuffer.getInt(initPosition + 4);
                normalizingIndexOffset = byteBuffer.getInt(initPosition + 8);
                numberOfSignificantValueDigits = byteBuffer.getInt(initPosition + 12);
                lowestTrackableUnitValue = byteBuffer.getLong(initPosition + 16);
                highestTrackableValue = byteBuffer.getLong(initPosition + 24);
                integerToDoubleValueConversionRatio = byteBuffer.getDouble(initPosition + 32);

                highestTrackableValue = Math.max(highestTrackableValue, 2);

                final long largestValueWithSingleUnitResolution = 2 * (long) Math.pow(10, numberOfSignificantValueDigits);
                final int unitMagnitude = (int) (Math.log(lowestTrackableUnitValue)/Math.log(2));
                final long unitMagnitudeMask = (1 << unitMagnitude) - 1;
                int subBucketCountMagnitude = (int) Math.ceil(Math.log(largestValueWithSingleUnitResolution)/Math.log(2));
                final int subBucketHalfCountMagnitude = subBucketCountMagnitude - 1;
                final int subBucketCount = 1 << subBucketCountMagnitude;
                final int subBucketHalfCount = subBucketCount / 2;
                final long subBucketMask = ((long)subBucketCount - 1) << unitMagnitude;
                if (subBucketCountMagnitude + unitMagnitude > 62) {
                    // subBucketCount entries can't be represented, with unitMagnitude applied, in a positive long.
                    // Technically it still sort of works if their sum is 63: you can represent all but the last number
                    // in the shifted subBucketCount. However, the utility of such a histogram vs ones whose magnitude here
                    // fits in 62 bits is debatable, and it makes it harder to work through the logic.
                    // Sums larger than 64 are totally broken as leadingZeroCountBase would go negative.
                    throw new IllegalArgumentException("Cannot represent numberOfSignificantValueDigits worth of values " +
                            "beyond lowestDiscernibleValue");
                }

                final int expectedCapacity =payloadLengthInBytes;
                assert expectedCapacity == payloadLengthInBytes;
                if(expectedCapacity > byteBuffer.limit() - 40){
                    throw new IllegalArgumentException("The buffer does not contain the full Histogram payload");
                }
                final int position = initPosition + 40;
                final int lengthInBytes = expectedCapacity;
                final int wordSizeInBytes = V2maxWordSizeInBytes;
                // fillCountsArrayFromSourceBuffer

                ByteBuffer sourceBuffer = byteBuffer.duplicate();
                sourceBuffer.position(position);
                final long maxAllowableCountInHistigram = Long.MAX_VALUE;
                int dstIndex = 0;
                int endPosition = sourceBuffer.position() + lengthInBytes; //期望的结束读取的索引
                while (sourceBuffer.position() < endPosition) {
                    long count;
                    int zerosCount = 0;
                    // V2 encoding format uses a long encoded in a ZigZag LEB128 format (up to V2maxWordSizeInBytes):
                    count = ZigZagEncoding.getLong(sourceBuffer);
                    if (count < 0) {
                        long zc = -count; // 0值的连续个数
                        if (zc > Integer.MAX_VALUE) {
                            throw new IllegalArgumentException(
                                    "An encoded zero count of > Integer.MAX_VALUE was encountered in the source");
                        }
                        zerosCount = (int) zc;
                    }
                    if (zerosCount > 0) {
                        dstIndex += zerosCount; // No need to set zeros in array. Just skip them.
                    } else {
                        // 单个非连续的0也会被输出
                        if(count > 0){
                            //long value = valueFromIndex(dstIndex, subBucketHalfCountMagnitude, subBucketHalfCount, unitMagnitude);
                            //histogram.recordValueWithCount(value, count);
                            totalCount += count;
                        }
                        dstIndex++;
                    }
                }
                return totalCount;
            }else{
                throw new UnsupportedOperationException("unsupported method");
            }
        } catch (DataFormatException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void recordValue(long value) throws RuntimeException {
        throw new UnsupportedOperationException("unsupported method");
    }

    @Override
    public void recordValueWithCount(long value, long count) throws RuntimeException {
        throw new UnsupportedOperationException("unsupported method");
    }

    @Override
    public long getValueAtPercentile(double percentile) {
        throw new UnsupportedOperationException("unsupported method");
    }

    @Override
    public List<Percentile> percentileList(int percentileTicksPerHalfDistance) {
        throw new UnsupportedOperationException("unsupported method");
    }

    @Override
    public Histogramer resetHistogram() {
        throw new UnsupportedOperationException("unsupported method");
    }

    @Override
    public Histogramer merge(Histogramer histogram) {
        throw new UnsupportedOperationException("unsupported method");
    }

    @Override
    public Histogramer makeCopy() throws RuntimeException{
        int cookie = byteBuffer.getInt(initPosition);
        if(getCookieBase(cookie) == V2CompressedEncodingCookieBase){
            try {
                return ArrayHistogram.decodeFromCompressedByteBuffer(byteBuffer, 2);
            } catch (DataFormatException e) {
                throw new RuntimeException(e);
            }
        }else if(getCookieBase(cookie) == V2EncodingCookieBase){
            return ArrayHistogram.decodeFromByteBuffer(byteBuffer, 2);
        }
        throw new UnsupportedOperationException("unsupported method");
    }

    @Override
    public byte[] toBytes() {
        int size = byteBuffer.limit() - initPosition;
        byte[] bytes = new byte[size];
        assert byteBuffer.order() == ByteOrder.BIG_ENDIAN;
        int oldPosition = byteBuffer.position();
        byteBuffer.position(initPosition);
        byteBuffer.get(bytes, 0, size);
        byteBuffer.position(oldPosition);
        return bytes;
    }
}

