package org.apache.druid.query.aggregation.sketch.HdrHistogram;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.HdrHistogram.*;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.common.utils.SerializerUtils;
import org.apache.druid.java.util.common.io.smoosh.Smoosh;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.segment.column.ColumnDescriptor;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.GenericIndexedUtils;
import org.apache.druid.segment.data.ObjectStrategy;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.File;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;

public class FileColReadTest {
    static final byte VERSION_ONE = 0x1;
    static final byte REVERSE_LOOKUP_ALLOWED = 0x1;


/*    @Test
    public void testColReadHdr() throws Exception{
        NullHandling.initializeForTests();
        SerializerUtils SERIALIZER_UTILS = new SerializerUtils();
        ObjectMapper mapper = new ObjectMapper();
        File file = new File("D:/doc/datas/100_index");
        SmooshedFileMapper smooshedFiles = Smoosh.map(file);
        ByteBuffer colBuffer = smooshedFiles.mapFile("latency_ms_sketch");
        String json = SERIALIZER_UTILS.readString(colBuffer);
        ColumnDescriptor serde = mapper.readValue(
                json, ColumnDescriptor.class
        );
        System.out.println(json);

        ByteBuffer buffer = colBuffer;
        byte versionFromBuffer = buffer.get();
        assert VERSION_ONE == versionFromBuffer;

        Field field = Histogram.class.getDeclaredField("counts");
        field.setAccessible(true);

        HdrHistogramObjectStrategy STRATEGY = new HdrHistogramObjectStrategy();
        GenericIndexed<Histogram> indexed = GenericIndexedUtils.createGenericIndexedVersionOne(buffer, STRATEGY);
        int size = indexed.size();

        int count = 0;
        int zreoCount = 0;
        long totalSize = 0;
        long totalCnt = 0;
        long totalCnt2 = 0;
        long totalCount = 0;
        Histogram histogram = new Histogram(2);
        for (int i = 0; i < size; i++) {
            Histogram hdr = indexed.get(i);
            if(hdr == null){
                continue;
            }
            count++;

            histogram.add(hdr);
            *//*long[] counts = (long[]) field.get(hdr);
            int cnt = 0;
            int cnt2 = 0;
            for (int j = 0; j < counts.length; j++) {
                if(counts[j] > 0){
                    cnt++;
                    cnt2 += counts[j];
                }
            }
            totalSize += counts.length;
            totalCnt += cnt;
            totalCnt2 += cnt2;
            totalCount += hdr.getTotalCount();
            if(hdr.getTotalCount() == 0){
                zreoCount++;
            }
            System.out.println(hdr.getHighestTrackableValue() + "-"+ hdr.getMaxValue()+ ",size:" + counts.length + ", cnt:" + cnt+ ", cnt2:" + cnt2+ ", totalCount:" + hdr.getTotalCount());
        *//*
        }

        System.out.println("size:" + size +",count:" + count  +",zreoCount:" + zreoCount+ ", totalSize:" + totalSize + ", totalCnt:" + totalCnt+ ", totalCnt2:" + totalCnt2 + ", totalCount:" + totalCount);

        histogram.outputPercentileDistribution(System.out, 1D);
    }*/

    @Test
    public void testColReadHdrSketchCount() throws Exception{
        NullHandling.initializeForTests();
        SerializerUtils SERIALIZER_UTILS = new SerializerUtils();
        ObjectMapper mapper = new ObjectMapper();
        File file = new File("D:/doc/datas/100_index");
        SmooshedFileMapper smooshedFiles = Smoosh.map(file);
        ByteBuffer colBuffer = smooshedFiles.mapFile("latency_ms_sketch");
        String json = SERIALIZER_UTILS.readString(colBuffer);
        ColumnDescriptor serde = mapper.readValue(
                json, ColumnDescriptor.class
        );
        System.out.println(json);

        ByteBuffer buffer = colBuffer;
        byte versionFromBuffer = buffer.get();
        assert VERSION_ONE == versionFromBuffer;

        HistogramSketchObjectStrategy STRATEGY = new HistogramSketchObjectStrategy();
        GenericIndexed<HistogramSketch> indexed = GenericIndexedUtils.createGenericIndexedVersionOne(buffer, STRATEGY);
        int size = indexed.size();

        int count = 0;
        int noZeroCount = 0;
        HistogramUnion union = new HistogramUnion(2);
        for (int i = 0; i < size; i++) {
            HistogramSketch hdr = indexed.get(i);
            if(hdr == null){
                continue;
            }
            count++;
            if(hdr.getTotalCount() != 0){
                noZeroCount++;
            }

            System.out.println("size:" + hdr.toBytes().length +",totalCount:" + hdr.getTotalCount());
            union.update(hdr);
        }

        System.out.println("size:" + size +",count:" + count +",noZeroCount:" + noZeroCount);
        ((ArrayHistogram)(union.getResult().hisImpl)).outputPercentileDistribution(System.out, 1D);
    }

    @Test
    public void testColReadHdrSketchCount2() throws Exception{
        NullHandling.initializeForTests();
        SerializerUtils SERIALIZER_UTILS = new SerializerUtils();
        ObjectMapper mapper = new ObjectMapper();
        File file = new File("D:/doc/datas/3_index");
        SmooshedFileMapper smooshedFiles = Smoosh.map(file);
        ByteBuffer colBuffer = smooshedFiles.mapFile("latency_ms_sketch");
        String json = SERIALIZER_UTILS.readString(colBuffer);
        ColumnDescriptor serde = mapper.readValue(
                json, ColumnDescriptor.class
        );
        System.out.println(json);

        ByteBuffer buffer = colBuffer;
        byte versionFromBuffer = buffer.get();
        assert VERSION_ONE == versionFromBuffer;

        HistogramSketchObjectStrategy STRATEGY = new HistogramSketchObjectStrategy();
        GenericIndexed<HistogramSketch> indexed = GenericIndexedUtils.createGenericIndexedVersionOne(buffer, STRATEGY);
        int size = indexed.size();

        int count = 0;
        int noZeroCount = 0;
        HistogramUnion union = new HistogramUnion(2);
        for (int i = 0; i < size; i++) {
            HistogramSketch hdr = indexed.get(i);
            if(hdr == null){
                continue;
            }
            count++;
            if(hdr.getTotalCount() != 0){
                noZeroCount++;
            }

            //System.out.println("size:" + hdr.toBytes().length +",totalCount:" + hdr.getTotalCount());
            union.update(hdr);
        }

        System.out.println("size:" + size +",count:" + count +",noZeroCount:" + noZeroCount);
        System.out.println(union.getResult().getTotalCount());
        //((ArrayHistogram)(union.getResult().hisImpl)).outputPercentileDistribution(System.out, 1D);
    }

    @Test
    public void testColReadHdrSketch() throws Exception{
        NullHandling.initializeForTests();
        SerializerUtils SERIALIZER_UTILS = new SerializerUtils();
        ObjectMapper mapper = new ObjectMapper();
        File file = new File("D:/doc/datas/100_index");
        SmooshedFileMapper smooshedFiles = Smoosh.map(file);
        ByteBuffer colBuffer = smooshedFiles.mapFile("latency_ms_sketch");
        String json = SERIALIZER_UTILS.readString(colBuffer);
        ColumnDescriptor serde = mapper.readValue(
                json, ColumnDescriptor.class
        );
        System.out.println(json);

        ByteBuffer buffer = colBuffer;
        byte versionFromBuffer = buffer.get();
        assert VERSION_ONE == versionFromBuffer;

        Field field = Histogram.class.getDeclaredField("counts");
        field.setAccessible(true);

        HistogramSketchObjectStrategy STRATEGY = new HistogramSketchObjectStrategy();
        GenericIndexed<HistogramSketch> indexed = GenericIndexedUtils.createGenericIndexedVersionOne(buffer, STRATEGY);
        int size = indexed.size();

        int count = 0;
        HistogramUnion union = new HistogramUnion(2);
        for (int i = 0; i < size; i++) {
            HistogramSketch hdr = indexed.get(i);
            if(hdr == null){
                continue;
            }
            count++;

            union.update(hdr);
        }

        System.out.println("size:" + size +",count:" + count);
        ((ArrayHistogram)(union.getResult().hisImpl)).outputPercentileDistribution(System.out, 1D);
    }

    @Test
    public void testColReadHdrSketch2() throws Exception{
        NullHandling.initializeForTests();
        SerializerUtils SERIALIZER_UTILS = new SerializerUtils();
        ObjectMapper mapper = new ObjectMapper();
        File file = new File("D:/doc/datas/100_index");
        SmooshedFileMapper smooshedFiles = Smoosh.map(file);
        ByteBuffer colBuffer = smooshedFiles.mapFile("latency_ms_sketch");
        String json = SERIALIZER_UTILS.readString(colBuffer);
        ColumnDescriptor serde = mapper.readValue(
                json, ColumnDescriptor.class
        );
        System.out.println(json);

        ByteBuffer buffer = colBuffer;
        byte versionFromBuffer = buffer.get();
        assert VERSION_ONE == versionFromBuffer;

        Field field = Histogram.class.getDeclaredField("counts");
        field.setAccessible(true);

        HistogramSketchObjectStrategy STRATEGY = new HistogramSketchObjectStrategy();
        GenericIndexed<HistogramSketch> indexed = GenericIndexedUtils.createGenericIndexedVersionOne(buffer, STRATEGY);
        int size = indexed.size();

        int count = 0;
        HistogramUnion union = new HistogramUnion(1, 241663, 3,
                ByteBuffer.allocate(HistogramSketch.getUpdatableSerializationBytes(1, 241663, 3)));
        for (int i = 0; i < size; i++) {
            HistogramSketch hdr = indexed.get(i);
            if(hdr == null){
                continue;
            }
            count++;

            union.update(hdr);
        }

        System.out.println("size:" + size +",count:" + count);
        ((DirectArrayHistogram)(union.getResult().hisImpl)).outputPercentileDistribution(System.out, 1D);
    }

    public static class HistogramSketchObjectStrategy implements ObjectStrategy<HistogramSketch> {

        @Override
        public Class<? extends HistogramSketch> getClazz() {
            return HistogramSketch.class;
        }

        @Nullable
        @Override
        public HistogramSketch fromByteBuffer(ByteBuffer buffer, int numBytes) {
            buffer.limit(buffer.position() + numBytes);
            return HistogramSketch.wrapByteBuffer(buffer);
        }

        @Nullable
        @Override
        public byte[] toBytes(@Nullable HistogramSketch val) {
            return new byte[0];
        }

        @Override
        public int compare(HistogramSketch o1, HistogramSketch o2) {
            return 0;
        }
    }
}
