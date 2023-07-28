package org.apache.druid.query.aggregation.sketch.hlld;

import com.zdjz.galaxy.sketch.hlld.HllUnion;
import org.junit.Test;

import java.nio.ByteBuffer;

public class HllBufferMergeTest {

    @Test
    public void test() {
        byte[] emptyBytes = new byte[HllUnion.getUpdatableSerializationBytes(14)];
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(HllUnion.getUpdatableSerializationBytes(14) * 2);
        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            byteBuffer.position(0);
            byteBuffer.put(emptyBytes);
        }
        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }

}
