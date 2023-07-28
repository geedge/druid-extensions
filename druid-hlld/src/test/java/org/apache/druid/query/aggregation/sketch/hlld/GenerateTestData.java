package org.apache.druid.query.aggregation.sketch.hlld;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zdjz.galaxy.sketch.hlld.Hll;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.Test;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class GenerateTestData {

    @Test
    public void geneJsonFile() throws Exception{
        long ts = System.currentTimeMillis() / 1000;
        Path path = FileSystems.getDefault().getPath("hlld.json");
        try (BufferedWriter out = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            Random rand = ThreadLocalRandom.current();
            for (int i = 0; i < 100; i++) {
                String dim = Integer.toString(rand.nextInt(5) + 1);
                Hll hll = new Hll(12);
                for (int j = 0; j < 20; j++) {
                    hll.add(rand.nextInt(100) + 1);
                }
                writeSketchRecordJson(out, ts, dim, hll);
            }
        }
    }

    @Test
    public void geneTsvFile() throws Exception{
        long ts = System.currentTimeMillis() / 1000;
        Path path = FileSystems.getDefault().getPath("ds_cpc.tsv");
        try (BufferedWriter out = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            out.write("ts");
            out.write("\t");
            out.write("dim");
            out.write("\t");
            out.write("hll");
            out.newLine();
            Random rand = ThreadLocalRandom.current();
            for (int i = 0; i < 100; i++) {
                String dim = Integer.toString(rand.nextInt(5) + 1);
                Hll hll = new Hll(12);
                for (int j = 0; j < 20; j++) {
                    hll.add(rand.nextInt(100) + 1);
                }
                writeSketchRecord(out, ts+"", dim, hll);
            }
        }
    }

    private static void writeSketchRecord(BufferedWriter out, String ts, String dim, Hll hll) throws Exception{
        out.write(ts);
        out.write("\t");
        out.write(dim);
        out.write("\t");
        out.write(StringUtils.encodeBase64String(HllUtils.toBytes(hll)));
        out.newLine();
    }

    private static void writeSketchRecordJson(BufferedWriter out, long ts, String dim, Hll hll) throws Exception{
        Map<String, Object> map = new HashMap<>();
        map.put("ts", ts);
        map.put("dim", dim);
        map.put("hll", StringUtils.encodeBase64String(HllUtils.toBytes(hll)));

        out.write(toJson(map));
        out.newLine();
    }

    public static final ObjectMapper objectMapper = new ObjectMapper();

    public static String toJson(Object data){
        try {
            return objectMapper.writeValueAsString(data);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }

}
