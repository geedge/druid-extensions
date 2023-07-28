package org.apache.druid.query.aggregation.sketch.HdrHistogram;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramSketch;
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
        Path path = FileSystems.getDefault().getPath("hdr_histogram.json");
        try (BufferedWriter out = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            Random rand = ThreadLocalRandom.current();
            for (int i = 0; i < 100; i++) {
                String dim = Integer.toString(rand.nextInt(5) + 1);
                HistogramSketch histogram = new HistogramSketch(1, 100, 2, false);
                for (int j = 0; j < 20; j++) {
                    histogram.recordValue(rand.nextInt(100) + 1);
                }
                writeSketchRecordJson(out, ts, dim, histogram);
            }
        }
    }

    @Test
    public void geneTsvFile() throws Exception{
        long ts = System.currentTimeMillis() / 1000;
        Path path = FileSystems.getDefault().getPath("hdrHistogram.tsv");
        try (BufferedWriter out = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            out.write("ts");
            out.write("\t");
            out.write("dim");
            out.write("\t");
            out.write("his");
            out.newLine();
            Random rand = ThreadLocalRandom.current();
            for (int i = 0; i < 100; i++) {
                String dim = Integer.toString(rand.nextInt(5) + 1);
                HistogramSketch histogram = new HistogramSketch(1, 100, 2, false);
                for (int j = 0; j < 20; j++) {
                    histogram.recordValue(rand.nextInt(100) + 1);
                }
                writeSketchRecord(out, ts+"", dim, histogram);
            }
        }
    }

    private static void writeSketchRecord(BufferedWriter out, String ts, String dim, HistogramSketch histogram) throws Exception{
        out.write(ts);
        out.write("\t");
        out.write(dim);
        out.write("\t");
        out.write(StringUtils.encodeBase64String(histogram.toBytes()));
        out.newLine();
    }

    private static void writeSketchRecordJson(BufferedWriter out, long ts, String dim, HistogramSketch histogram) throws Exception{
        //ByteBuffer byteBuffer = ByteBuffer.allocate(histogram.getNeededByteBufferCapacity());
        //histogram.encodeIntoByteBuffer(byteBuffer);

        Map<String, Object> map = new HashMap<>();
        map.put("ts", ts);
        map.put("dim", dim);
        map.put("his", StringUtils.encodeBase64String(histogram.toBytes()));

        out.write(HdrHistogramModule.toJson(map));
        out.newLine();
    }

}
