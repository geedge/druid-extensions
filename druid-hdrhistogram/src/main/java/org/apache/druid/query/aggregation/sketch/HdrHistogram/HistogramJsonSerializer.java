package org.apache.druid.query.aggregation.sketch.HdrHistogram;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.HdrHistogram.HistogramSketch;

import java.io.IOException;

public class HistogramJsonSerializer extends JsonSerializer<HistogramSketch> {
    @Override
    public void serialize(HistogramSketch histogram, JsonGenerator generator, SerializerProvider serializerProvider) throws IOException {
        generator.writeBinary(histogram.toBytes());
    }
}
