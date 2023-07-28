package org.apache.druid.query.aggregation.sketch.hlld;

import com.zdjz.galaxy.sketch.hlld.Hll;
import it.unimi.dsi.fastutil.bytes.ByteArrays;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.data.ObjectStrategy;

import java.nio.ByteBuffer;

public class HllObjectStrategy implements ObjectStrategy<Hll> {
    private static final Logger LOG = new Logger(HllObjectStrategy.class);
    private static long lastTs = 0L;
    private static int cnt = 0;

    @Override
    public Class<Hll> getClazz() {
        return Hll.class;
    }

    @Override
    public Hll fromByteBuffer(final ByteBuffer buffer, final int numBytes) {
        /*long ts = System.currentTimeMillis();
        cnt++;
        if(ts - lastTs > 200){
            LOG.error("HllObjectStrategy fromByteBuffer call:" + cnt);
            lastTs = ts;
        }*/

        buffer.limit(buffer.position() + numBytes);
        return Hll.wrapByteBuffer(buffer);
    }

    @Override
    public byte[] toBytes(final Hll hll) {
        if (hll == null) {
            return ByteArrays.EMPTY_ARRAY;
        }

        return HllUtils.toBytes(hll);
    }

    @Override
    public int compare(final Hll o1, final Hll o2) {
        return HllAggregatorFactory.COMPARATOR.compare(o1, o2);
    }
}
