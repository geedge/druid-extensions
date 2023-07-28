package org.apache.druid.query.aggregation.sketch.hlld;

import com.zdjz.galaxy.sketch.hlld.Hll;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;

import java.nio.ByteBuffer;

public class HllUtils {
    private static final Logger LOG = new Logger(HllUtils.class);
    private static long lastTs = 0L;
    private static int cnt = 0;

    static Hll deserializeHll(final Object object) {
        /*long ts = System.currentTimeMillis();
        cnt++;
        if(ts - lastTs > 200){
            LOG.error("deserializeHll call:" + cnt +  "-" + object.getClass());
            lastTs = ts;
        }*/
        if (object instanceof String) {
            byte[] bytes = StringUtils.decodeBase64(StringUtils.toUtf8((String) object));
            return Hll.wrapBytes(bytes);
        } else if (object instanceof byte[]) {
            return Hll.wrapBytes((byte[]) object);
        } else if (object instanceof ByteBuffer) {
            return Hll.wrapByteBuffer((ByteBuffer) object);
        } else if (object instanceof Hll) {
            return (Hll) object;
        }
        throw new IAE("Object is not of a type that can be deserialized to an Hll:" + object.getClass().getName());
    }

    public static byte[] toBytes(Hll hll) {
        return hll.toBytes();
    }
}
