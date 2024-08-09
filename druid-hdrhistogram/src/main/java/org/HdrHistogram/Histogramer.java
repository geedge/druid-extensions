package org.HdrHistogram;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public interface Histogramer {
    long getTotalCount();

    void recordValue(long value) throws RuntimeException;

    void recordValueWithCount(long value, long count) throws RuntimeException;

    long getValueAtPercentile(double percentile);

    List<Percentile> percentileList(int percentileTicksPerHalfDistance);

    Map<String, Object> describe();

    Histogramer resetHistogram();

    Histogramer merge(Histogramer histogram);

    // 复制到堆内存实例ArrayHistogram
    Histogramer makeCopy();

    byte[] toBytes();

    default byte[] byteBuffer2Bytes(ByteBuffer byteBuffer){
        //必须调用完后flip()才可以调用此方法
        byteBuffer.flip();
        int len = byteBuffer.limit() - byteBuffer.position();
        byte[] bytes = new byte[len];
        byteBuffer.get(bytes);
        return bytes;
    }
}
