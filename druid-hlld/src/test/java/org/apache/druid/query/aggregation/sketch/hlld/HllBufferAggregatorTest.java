package org.apache.druid.query.aggregation.sketch.hlld;

import com.zdjz.galaxy.sketch.hlld.Hll;
import com.zdjz.galaxy.sketch.hlld.HllUnion;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.TestFloatColumnSelector;
import org.apache.druid.query.aggregation.TestObjectColumnSelector;
import org.junit.Test;

import java.nio.ByteBuffer;

public class HllBufferAggregatorTest {
    private void aggregateBuffer(TestFloatColumnSelector selector, BufferAggregator agg, ByteBuffer buf, int position) {
        agg.aggregate(buf, position);
        selector.increment();
    }

    private void aggregateBuffer(TestObjectColumnSelector selector, BufferAggregator agg, ByteBuffer buf, int position) {
        agg.aggregate(buf, position);
        selector.increment();
    }

    @Test
    public void testBufferAggregate() {
        Object[] values = new Object[100000];
        for (int i = 0; i < values.length; i++) {
            values[i] = (long)i;
        }
        TestObjectColumnSelector selector = new TestObjectColumnSelector(values);

        HllAggregatorFactory factory = new HllAggregatorFactory("billy","billy",12,false);
        HllBufferAggregator agg = new HllBufferAggregator(selector, 12);

        System.out.println(factory.getMaxIntermediateSize());
        ByteBuffer buf = ByteBuffer.allocate(factory.getMaxIntermediateSize());
        int position = 0;

        agg.init(buf, position);
        for (int i = 0; i < values.length; i++) {
            aggregateBuffer(selector, agg, buf, position);
        }

        Hll hll = (Hll) (agg.get(buf, position));
        System.out.println(hll.getEstimate());

        hll = new Hll(hll.getP());
        for (int i = 0; i < values.length; i++) {
            hll.add((Long) values[i]);
        }

        System.out.println(hll.getEstimate());
    }


    @Test
    public void testBufferAggregatePosOffset() {
        Object[] values = new Object[100000];
        for (int i = 0; i < values.length; i++) {
            values[i] = (long)i;
        }
        TestObjectColumnSelector selector = new TestObjectColumnSelector(values);

        HllAggregatorFactory factory = new HllAggregatorFactory("billy","billy",12,false);
        HllBufferAggregator agg = new HllBufferAggregator(selector, 12);

        System.out.println(factory.getMaxIntermediateSize());
        int position = 1024;
        ByteBuffer buf = ByteBuffer.allocate(position + factory.getMaxIntermediateSize());


        agg.init(buf, position);
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < values.length; i++) {
            aggregateBuffer(selector, agg, buf, position);
        }

        Hll hll = (Hll) (agg.get(buf, position));
        System.out.println(hll.getEstimate());

        hll = new Hll(hll.getP());
        for (int i = 0; i < values.length; i++) {
            hll.add((Long) values[i]);
        }
        System.out.println(hll.getEstimate());
    }

    @Test
    public void testBufferAggregatePosOffset2() {
        Object[] values = new Object[100000];
        for (int i = 0; i < values.length; i++) {
            values[i] = (long)i;
        }
        TestObjectColumnSelector selector = new TestObjectColumnSelector(values);

        HllAggregatorFactory factory = new HllAggregatorFactory("billy","billy",12,false);
        HllBufferAggregator agg = new HllBufferAggregator(selector, 12);

        System.out.println(factory.getMaxIntermediateSize());
        int position = 1024;
        ByteBuffer buf = ByteBuffer.allocate(position + factory.getMaxIntermediateSize() + 1024);


        agg.init(buf, position);
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < values.length; i++) {
            aggregateBuffer(selector, agg, buf, position);
        }

        Hll hll = (Hll) (agg.get(buf, position));
        System.out.println(hll.getEstimate());

        hll = new Hll(hll.getP());
        for (int i = 0; i < values.length; i++) {
            hll.add((Long) values[i]);
        }
        System.out.println(hll.getEstimate());
    }

    @Test
    public void testMergeBufferMergeAggregator() {
        Object[] values = new Object[10];
        for (int i = 0; i < 10; i++) {
            Hll hll = new Hll(12);
            for (int j = i * 100000; j < 100000 * (i + 1); j++) {
                hll.add(j);
            }
            values[i] = hll;
        }

        TestObjectColumnSelector selector = new TestObjectColumnSelector(values);

        HllMergeAggregatorFactory factory = new HllMergeAggregatorFactory("billy","billy",12,false);
        HllMergeBufferAggregator agg = new HllMergeBufferAggregator(selector, 12);

        System.out.println(factory.getMaxIntermediateSize());
        ByteBuffer buf = ByteBuffer.allocate(factory.getMaxIntermediateSize());
        int position = 0;

        agg.init(buf, position);
        for (int i = 0; i < values.length; i++) {
            aggregateBuffer(selector, agg, buf, position);
        }

        Hll hll = (Hll) (agg.get(buf, position));
        System.out.println(hll.getEstimate());

        System.out.println("*****************************");

        HllUnion union = new HllUnion(12);
        for (int i = 0; i < values.length; i++) {
            union.update((Hll) values[i]);
        }
        hll = union.getResult();
        System.out.println(hll.getEstimate());
    }

    @Test
    public void testMergeBufferAggregatorPosOffset() {
        Object[] values = new Object[10];
        for (int i = 0; i < 10; i++) {
            Hll hll = new Hll(12);
            for (int j = i * 100000; j < 100000 * (i + 1); j++) {
                hll.add(j);
            }
            values[i] = hll;
        }

        TestObjectColumnSelector selector = new TestObjectColumnSelector(values);

        HllMergeAggregatorFactory factory = new HllMergeAggregatorFactory("billy","billy",12,false);
        HllMergeBufferAggregator agg = new HllMergeBufferAggregator(selector, 12);


        System.out.println(factory.getMaxIntermediateSize());
        int position = 64;
        ByteBuffer buf = ByteBuffer.allocate(position + factory.getMaxIntermediateSize());

        agg.init(buf, position);
        for (int i = 0; i < values.length; i++) {
            aggregateBuffer(selector, agg, buf, position);
        }

        Hll hll = (Hll) (agg.get(buf, position));
        System.out.println(hll.getEstimate());

        System.out.println("*****************************");

        HllUnion union = new HllUnion(12);
        for (int i = 0; i < values.length; i++) {
            union.update((Hll) values[i]);
        }
        hll = union.getResult();
        System.out.println(hll.getEstimate());
    }
}
