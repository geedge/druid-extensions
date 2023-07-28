package org.apache.druid.query.aggregation.sketch.hlld;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zdjz.galaxy.sketch.hlld.Hll;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.common.utils.SerializerUtils;
import org.apache.druid.java.util.common.io.smoosh.Smoosh;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.segment.column.ColumnDescriptor;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.GenericIndexedUtils;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;

public class FileColReadTest {
    static final byte VERSION_ONE = 0x1;
    static final byte REVERSE_LOOKUP_ALLOWED = 0x1;

    @Test
    public void testColReadHllCount() throws Exception{
        NullHandling.initializeForTests();
        SerializerUtils SERIALIZER_UTILS = new SerializerUtils();
        ObjectMapper mapper = new ObjectMapper();
        File file = new File("D:/doc/datas/9_index");
        SmooshedFileMapper smooshedFiles = Smoosh.map(file);
        // 每个null大概占用8个字节
        ByteBuffer colBuffer = smooshedFiles.mapFile("client_ip_sketch");
        String json = SERIALIZER_UTILS.readString(colBuffer);
        ColumnDescriptor serde = mapper.readValue(
                json, ColumnDescriptor.class
        );
        System.out.println(json);

        ByteBuffer buffer = colBuffer;
        byte versionFromBuffer = buffer.get();
        assert VERSION_ONE == versionFromBuffer;

        HllObjectStrategy STRATEGY = new HllObjectStrategy();
        GenericIndexed<Hll> indexed = GenericIndexedUtils.createGenericIndexedVersionOne(buffer, STRATEGY);
        int size = indexed.size();

        int count = 0;
        int noZeroCount = 0;
        for (int i = 0; i < size; i++) {
            Hll hll = indexed.get(i);
            if(hll == null){
                continue;
            }
            count++;
            if(hll.size() > 0){
                noZeroCount++;
            }

            System.out.println("size:" + hll.toBytes().length +",totalCount:" + hll.size());
        }

        System.out.println("size:" + size +",count:" + count +",noZeroCount:" + noZeroCount);
    }
}
