package org.apache.druid.query.aggregation.sketch.hlld;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.zdjz.galaxy.sketch.hlld.Hll;

import java.io.IOException;

public class HllJsonSerializer extends JsonSerializer<Hll> {
    @Override
    public void serialize(Hll hll, JsonGenerator generator, SerializerProvider serializerProvider) throws IOException {
        generator.writeBinary(HllUtils.toBytes(hll));
    }

}
