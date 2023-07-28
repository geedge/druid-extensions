package org.apache.druid.query.aggregation.sketch.HdrHistogram;

import com.google.common.collect.ImmutableMap;
import org.HdrHistogram.*;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.TestLongColumnSelector;
import org.apache.druid.query.aggregation.TestObjectColumnSelector;
import org.apache.druid.query.aggregation.datasketches.theta.SketchHolder;
import org.apache.druid.query.aggregation.datasketches.theta.SketchMergeAggregatorFactory;
import org.apache.druid.query.groupby.epinephelinae.GrouperTestUtil;
import org.apache.druid.query.groupby.epinephelinae.TestColumnSelectorFactory;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;

public class HdrHistogramBufferAggregatorTest {

    private void aggregateBuffer(TestLongColumnSelector selector, BufferAggregator agg, ByteBuffer buf, int position) {
        agg.aggregate(buf, position);
        selector.increment();
    }

    private void aggregateBuffer(TestObjectColumnSelector selector, BufferAggregator agg, ByteBuffer buf, int position) {
        agg.aggregate(buf, position);
        selector.increment();
    }

    @Test
    public void testBufferAggregate() {
        long[] values = new long[100000];
        for (int i = 0; i < values.length; i++) {
            values[i] = (long)i;
        }
        TestLongColumnSelector selector = new TestLongColumnSelector(values);

        HdrHistogramAggregatorFactory factory = new HdrHistogramAggregatorFactory("billy","billy",1L,2L, 3, true);
        HdrHistogramBufferAggregator agg = new HdrHistogramBufferAggregator(selector, 1L,2L, 3, true, factory.getMaxIntermediateSize());

        //ByteBuffer buf = ByteBuffer.allocate(factory.getMaxIntermediateSizeWithNulls());
        System.out.println(factory.getMaxIntermediateSize());
        ByteBuffer buf = ByteBuffer.allocate(factory.getMaxIntermediateSize());
        int position = 0;

        agg.init(buf, position);
        for (int i = 0; i < values.length; i++) {
            aggregateBuffer(selector, agg, buf, position);
        }

        HistogramSketch histogram =  agg.get(buf, position);
        System.out.println(histogram.getValueAtPercentile(50) + "," + histogram.getValueAtPercentile(75));
        System.out.println(histogram.getTotalCount());

        System.out.println("*****************************");

        histogram = new HistogramSketch(3);
        for (int i = 0; i < values.length; i++) {
            histogram.recordValue(i);
        }

        System.out.println(histogram.getValueAtPercentile(50) + "," + histogram.getValueAtPercentile(75));
        System.out.println(histogram.getTotalCount());
    }

    @Test
    public void testBufferAggregatePosOffset() {
        long[] values = new long[100000];
        for (int i = 0; i < values.length; i++) {
            values[i] = (long)i;
        }
        TestLongColumnSelector selector = new TestLongColumnSelector(values);

        HdrHistogramAggregatorFactory factory = new HdrHistogramAggregatorFactory("billy","billy",1L,2L, 3, true);
        HdrHistogramBufferAggregator agg = new HdrHistogramBufferAggregator(selector, 1L,2L, 3, true, factory.getMaxIntermediateSize());

        //ByteBuffer buf = ByteBuffer.allocate(factory.getMaxIntermediateSizeWithNulls());
        System.out.println(factory.getMaxIntermediateSize());
        int position = 1024;
        ByteBuffer buf = ByteBuffer.allocate(position + factory.getMaxIntermediateSize());

        agg.init(buf, position);
        for (int i = 0; i < values.length; i++) {
            aggregateBuffer(selector, agg, buf, position);
        }

        HistogramSketch histogram = agg.get(buf, position);
        System.out.println(histogram.getValueAtPercentile(50) + "," + histogram.getValueAtPercentile(75));
        System.out.println(histogram.getTotalCount());

        System.out.println("*****************************");

        histogram = new HistogramSketch(3);
        for (int i = 0; i < values.length; i++) {
            histogram.recordValue(i);
        }

        System.out.println(histogram.getValueAtPercentile(50) + "," + histogram.getValueAtPercentile(75));
        System.out.println(histogram.getTotalCount());
    }

    @Test
    public void testBufferAggregatePosOffset2() {
        long[] values = new long[100000];
        for (int i = 0; i < values.length; i++) {
            values[i] = (long)i;
        }
        TestLongColumnSelector selector = new TestLongColumnSelector(values);

        HdrHistogramAggregatorFactory factory = new HdrHistogramAggregatorFactory("billy","billy",1L,2L, 3, true);
        HdrHistogramBufferAggregator agg = new HdrHistogramBufferAggregator(selector, 1L,2L, 3, true, factory.getMaxIntermediateSize());

        //ByteBuffer buf = ByteBuffer.allocate(factory.getMaxIntermediateSizeWithNulls());
        System.out.println(factory.getMaxIntermediateSize());
        int position = 1024;
        ByteBuffer buf = ByteBuffer.allocate(position + factory.getMaxIntermediateSize() + 1024);

        agg.init(buf, position);
        for (int i = 0; i < values.length; i++) {
            aggregateBuffer(selector, agg, buf, position);
        }

        HistogramSketch histogram = agg.get(buf, position);
        System.out.println(histogram.getValueAtPercentile(50) + "," + histogram.getValueAtPercentile(75));
        System.out.println(histogram.getTotalCount());

        System.out.println("*****************************");

        histogram = new HistogramSketch(3);
        for (int i = 0; i < values.length; i++) {
            histogram.recordValue(i);
        }

        System.out.println(histogram.getValueAtPercentile(50) + "," + histogram.getValueAtPercentile(75));
        System.out.println(histogram.getTotalCount());
    }

    @Test
    public void testMergeBufferMergeAggregator() {
        Object[] values = new Object[10];
        for (int i = 0; i < 10; i++) {
            HistogramSketch histogram = new HistogramSketch(3);
            for (int j = i * 100000; j < 100000 * (i + 1); j++) {
                histogram.recordValue(j);
            }
            values[i] = histogram;
        }

        TestObjectColumnSelector selector = new TestObjectColumnSelector(values);

        HdrHistogramMergeAggregatorFactory factory = new HdrHistogramMergeAggregatorFactory("billy","billy",1L,2L, 3, true);
        HdrHistogramMergeBufferAggregator agg = new HdrHistogramMergeBufferAggregator(selector, 1L,2L, 3, true, factory.getMaxIntermediateSize());

        //ByteBuffer buf = ByteBuffer.allocate(factory.getMaxIntermediateSizeWithNulls());
        System.out.println(factory.getMaxIntermediateSize());
        ByteBuffer buf = ByteBuffer.allocate(factory.getMaxIntermediateSize());
        int position = 0;

        agg.init(buf, position);
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < values.length; i++) {
            aggregateBuffer(selector, agg, buf, position);
        }

        HistogramSketch histogram = agg.get(buf, position);
        System.out.println(histogram.getValueAtPercentile(50) + "," + histogram.getValueAtPercentile(75));
        System.out.println(histogram.getTotalCount());

        System.out.println("*****************************");



        HistogramUnion union = new HistogramUnion(3);
        for (int i = 0; i < values.length; i++) {
            union.update((HistogramSketch) values[i]);
        }
        histogram = union.getResult();
        System.out.println(histogram.getValueAtPercentile(50) + "," + histogram.getValueAtPercentile(75));
        System.out.println(histogram.getTotalCount());
    }

    @Test
    public void testMergeBufferMergeAggregatorPosOffset() {
        Object[] values = new Object[10];
        for (int i = 0; i < 10; i++) {
            HistogramSketch histogram = new HistogramSketch(3);
            for (int j = i * 100000; j < 100000 * (i + 1); j++) {
                histogram.recordValue(j);
            }
            values[i] = histogram;
        }

        TestObjectColumnSelector selector = new TestObjectColumnSelector(values);

        HdrHistogramMergeAggregatorFactory factory = new HdrHistogramMergeAggregatorFactory("billy","billy",1L,2L, 3, true);
        HdrHistogramMergeBufferAggregator agg = new HdrHistogramMergeBufferAggregator(selector, 1L,2L, 3, true, factory.getMaxIntermediateSize());

        //ByteBuffer buf = ByteBuffer.allocate(factory.getMaxIntermediateSizeWithNulls());
        System.out.println(factory.getMaxIntermediateSize());
        int position = 1024;
        ByteBuffer buf = ByteBuffer.allocate(position + factory.getMaxIntermediateSize());

        agg.init(buf, position);
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < values.length; i++) {
            aggregateBuffer(selector, agg, buf, position);
        }

        HistogramSketch histogram = ((HistogramSketch) agg.get(buf, position));
        System.out.println(histogram.getValueAtPercentile(50) + "," + histogram.getValueAtPercentile(75));
        System.out.println(histogram.getTotalCount());

        System.out.println("*****************************");



        HistogramUnion union = new HistogramUnion(3);
        for (int i = 0; i < values.length; i++) {
            union.update((HistogramSketch) values[i]);
        }
        histogram = union.getResult();
        System.out.println(histogram.getValueAtPercentile(50) + "," + histogram.getValueAtPercentile(75));
        System.out.println(histogram.getTotalCount());
    }

    @Test
    public void testMergeAggregatorRelocate() {
        final TestColumnSelectorFactory columnSelectorFactory = GrouperTestUtil.newColumnSelectorFactory();
        HistogramSketch histogram = new HistogramSketch(3);
        for (int i = 0; i < 100000; i++) {
            histogram.recordValue(i);
        }

        columnSelectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("sketch", histogram)));
        HistogramSketch[] histograms = runRelocateVerificationTest(
                new HdrHistogramMergeAggregatorFactory("sketch", "sketch", 1L,2L, 3, true),
                columnSelectorFactory,
                HistogramSketch.class
        );
        Assert.assertEquals(histograms[0].getValueAtPercentile(50), histograms[1].getValueAtPercentile(50), 0);

        ((ArrayHistogram)histograms[0].hisImpl).outputPercentileDistribution(System.out, 1D);
        System.out.println("*****************************");
        ((ArrayHistogram)histograms[1].hisImpl).outputPercentileDistribution(System.out, 1D);

    }

    @Test
    public void testAggregatorRelocate() {
        final TestColumnSelectorFactory columnSelectorFactory = GrouperTestUtil.newColumnSelectorFactory();
        HistogramSketch histogram = new HistogramSketch(3);
        for (int i = 0; i < 100000; i++) {
            histogram.recordValue(i);
        }

        columnSelectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("sketch", 10)));
        HistogramSketch[] histograms = runRelocateVerificationTest(
                new HdrHistogramAggregatorFactory("sketch", "sketch", 1L,2L, 3, true),
                columnSelectorFactory,
                HistogramSketch.class
        );
        Assert.assertEquals(histograms[0].getValueAtPercentile(50), histograms[1].getValueAtPercentile(50), 0);

        ((ArrayHistogram)histograms[0].hisImpl).outputPercentileDistribution(System.out, 1D);
        System.out.println("*****************************");
        ((ArrayHistogram)histograms[1].hisImpl).outputPercentileDistribution(System.out, 1D);

    }

    public <T> T[] runRelocateVerificationTest(
            AggregatorFactory factory,
            ColumnSelectorFactory selector,
            Class<T> clazz
    ){
        // 测试buf重新Relocate
        T[] results = (T[]) Array.newInstance(clazz, 2);
        BufferAggregator agg = factory.factorizeBuffered(selector);
        ByteBuffer myBuf = ByteBuffer.allocate(10040902);
        agg.init(myBuf, 0);
        agg.aggregate(myBuf, 0);
        results[0] = (T) agg.get(myBuf, 0);

        byte[] theBytes = new byte[factory.getMaxIntermediateSizeWithNulls()];
        myBuf.get(theBytes);

        ByteBuffer newBuf = ByteBuffer.allocate(941209);
        newBuf.position(7574);
        newBuf.put(theBytes);
        newBuf.position(0);
        agg.relocate(0, 7574, myBuf, newBuf);
        results[1] = (T) agg.get(newBuf, 7574);
        return results;
    }

    @Test
    public void testMaxIntermediateSize() {
        System.out.println(DirectHistogram.getUpdatableSerializationBytes(1, HdrHistogramAggregatorFactory.BUFFER_AUTO_RESIZE_HIGHEST, 3));
        System.out.println(DirectHistogram.getUpdatableSerializationBytes(1, 600000, 3));
        System.out.println(DirectHistogram.getUpdatableSerializationBytes(1, 3600000, 3));
        System.out.println(DirectHistogram.getUpdatableSerializationBytes(1, 36000000, 3));
    }


    @Test
    public void testMaxIntermediateSize2() {
        int[] significantValues = new int[]{1, 2, 3, 4, 5};
        long[] values = new long[]{100L, 10000L, 10000L * 100, 10000L * 1000, 10000L * 10000 , 100000000L * 10, 100000000L * 100, 100000000L * 1000, 100000000L * 10000, 100000000L * 1000000, 100000000L * 1000000};
        for (int i = 0; i < values.length; i++) {
            long value = values[i];
            for (int j = 0; j < significantValues.length; j++) {
                int significantValue = significantValues[j];
                int bytes = DirectHistogram.getUpdatableSerializationBytes(1, value, significantValue);
                System.out.println(value + ":" +significantValue + ":" + bytes+ ":" + bytes/ 1000);
            }
            System.out.println("#######################");
        }

    }

    @Test
    public void test1() {
        HistogramSketch histogram = new HistogramSketch(2);
        histogram.recordValue(3);
        HistogramSketch his = HistogramSketch.wrapByteBuffer(ByteBuffer.wrap(histogram.toBytes()));
        histogram = new HistogramSketch(2);
        histogram.hisImpl.merge(his.hisImpl);
        System.out.println(histogram.getTotalCount());
    }
}

