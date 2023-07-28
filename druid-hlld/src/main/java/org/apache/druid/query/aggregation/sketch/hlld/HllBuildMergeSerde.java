package org.apache.druid.query.aggregation.sketch.hlld;

import org.apache.druid.query.aggregation.sketch.RawInputValueExtractor;
import org.apache.druid.segment.serde.ComplexMetricExtractor;

public class HllBuildMergeSerde extends HllMergeSerde{
    @Override
    public ComplexMetricExtractor getExtractor() {
        return RawInputValueExtractor.getInstance();
    }
}
