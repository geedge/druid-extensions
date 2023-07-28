package org.apache.druid.query.aggregation.sketch.hlld;

import com.zdjz.galaxy.sketch.hlld.Hll;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.segment.GenericColumnSerializer;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.serde.ComplexColumnPartSupplier;
import org.apache.druid.segment.serde.ComplexMetricExtractor;
import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.druid.segment.serde.LargeColumnSupportedComplexColumnSerializer;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class HllMergeSerde extends ComplexMetricSerde {
    private static final HllObjectStrategy STRATEGY = new HllObjectStrategy();

    @Override
    public String getTypeName() {
        return HllModule.HLLD_TYPE_NAME;
    }

    @Override
    public ObjectStrategy getObjectStrategy() {
        return STRATEGY;
    }

    @Override
    public ComplexMetricExtractor getExtractor() {
        return new ComplexMetricExtractor() {
            @Override
            public Class extractedClass() {
                return Hll.class;
            }

            @Nullable
            @Override
            public Object extractValue(InputRow inputRow, String metricName) {
                final Object object = inputRow.getRaw(metricName);
                if (object == null || object instanceof Hll) {
                    return object;
                }
                return HllUtils.deserializeHll(object);
            }
        };
    }


    @Override
    public void deserializeColumn(ByteBuffer buffer, ColumnBuilder builder) {
        final GenericIndexed<Hll> column = GenericIndexed.read(buffer, STRATEGY, builder.getFileMapper());
        builder.setComplexColumnSupplier(new ComplexColumnPartSupplier(getTypeName(), column));
    }

    // support large columns
    @Override
    public GenericColumnSerializer getSerializer(SegmentWriteOutMedium segmentWriteOutMedium, String column) {
        return LargeColumnSupportedComplexColumnSerializer.create(segmentWriteOutMedium, column, this.getObjectStrategy());
    }
}
