package org.apache.druid.query.aggregation.sketch.hlld;

import com.zdjz.galaxy.sketch.hlld.Hll;
import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.query.aggregation.TestObjectColumnSelector;
import org.junit.Test;

public class HllAggregatorFactoryTest {

    @Test
    public void testAggregateCombiner() {
        Object[] values = new Object[10];
        for (int i = 0; i < 10; i++) {
            Hll hll = new Hll(12);
            for (int j = i * 100000; j < 100000 * (i + 1); j++) {
                hll.add(j);
            }
            values[i] = hll;
        }

        HllAggregatorFactory factory = new HllAggregatorFactory("a", "a", 12, false);
        TestObjectColumnSelector selector = new TestObjectColumnSelector(values);
        AggregateCombiner combiner = factory.makeAggregateCombiner();
        for (int i = 0; i < 10; i++) {
            combiner.reset(selector);
            System.out.println(((Hll) combiner.getObject()).size());
        }
    }

}
