package org.apache.druid.query.aggregation.sketch.HdrHistogram;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramSketch;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;

import java.nio.ByteBuffer;

public class HistogramUtils {

    public static HistogramSketch deserializeHistogram(final Object object) {
        if (object instanceof String) {
            byte[] bytes = StringUtils.decodeBase64(StringUtils.toUtf8((String) object));
            return HistogramSketch.wrapBytes(bytes);
        }else if (object instanceof byte[]) {
            return HistogramSketch.fromBytes((byte[]) object);
        } else if (object instanceof ByteBuffer) {
            return HistogramSketch.fromByteBuffer((ByteBuffer) object);
        }else if (object instanceof HistogramSketch) {
            return (HistogramSketch) object;
        }

        throw new IAE("Object is not of a type that can be deserialized to an Histogram:" + object.getClass().getName());
    }
}
