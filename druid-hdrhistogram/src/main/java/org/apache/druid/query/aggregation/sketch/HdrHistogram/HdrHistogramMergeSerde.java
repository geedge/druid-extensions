package org.apache.druid.query.aggregation.sketch.HdrHistogram;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramSketch;
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

public class HdrHistogramMergeSerde extends ComplexMetricSerde {
    private static final HdrHistogramObjectStrategy STRATEGY = new HdrHistogramObjectStrategy();

    @Override
    public String getTypeName() {
        return HdrHistogramModule.HDRHISTOGRAM_TYPE_NAME;
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
                return Histogram.class;
            }

            @Nullable
            @Override
            public Object extractValue(InputRow inputRow, String metricName) {
                final Object object = inputRow.getRaw(metricName);
                if(object instanceof String && ((String) object).isEmpty()){
                    return null;
                }

                if (object instanceof Number) {
                    HistogramSketch histogram = new HistogramSketch(2);
                    histogram.recordValue(((Number) object).longValue());
                    return histogram;
                }

                if (object == null || object instanceof Histogram) {
                    return object;
                }

                return deserializeHistogram(object);
            }
        };
    }

    static HistogramSketch deserializeHistogram(final Object object){
        return HistogramUtils.deserializeHistogram(object);
    }

    @Override
    public void deserializeColumn(ByteBuffer buffer, ColumnBuilder builder) {
        final GenericIndexed<HistogramSketch> column = GenericIndexed.read(buffer, STRATEGY, builder.getFileMapper());
        builder.setComplexColumnSupplier(new ComplexColumnPartSupplier(getTypeName(), column));
    }

    // support large columns
    @Override
    public GenericColumnSerializer getSerializer(SegmentWriteOutMedium segmentWriteOutMedium, String column) {
        return LargeColumnSupportedComplexColumnSerializer.create(segmentWriteOutMedium, column, this.getObjectStrategy());
    }

}
