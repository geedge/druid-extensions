package org.apache.druid.query.aggregation.sketch.HdrHistogram;

import org.HdrHistogram.DirectArrayHistogram;
import org.HdrHistogram.HistogramSketch;
import org.HdrHistogram.Histogramer;
import org.HdrHistogram.Percentile;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.io.BufferedWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class HistogramSketchTest {

    @Test
    public void describeTest() throws Exception{
        DirectArrayHistogram histogram = new DirectArrayHistogram(1, 1000000, 3,
                ByteBuffer.allocate(HistogramSketch.getUpdatableSerializationBytes(1, 1000000, 3)));
        System.out.println(histogram.describe());
        for (int i = 0; i < 10000; i++) {
            histogram.recordValue(i);
        }
        System.out.println(histogram.describe());
        for (Percentile percentile : histogram.percentileList(100)) {
            System.out.println(percentile);
        }
    }

    @Test
    public void describeTest1() throws Exception{
        HistogramSketch histogram = new HistogramSketch(1);
        System.out.println(histogram.describe());
        for (int i = 0; i < 10000; i++) {
            histogram.recordValue(i);
        }
        System.out.println(histogram.describe());
        for (Percentile percentile : histogram.percentileList(100)) {
            System.out.println(percentile);
        }
        System.out.println(StringUtils.repeat('#', 100));
        histogram = new HistogramSketch(1);
        for (int i = 0; i < 10000; i++) {
            histogram.recordValue(ThreadLocalRandom.current().nextLong(100000));
        }
        System.out.println(histogram.describe());
        for (Percentile percentile : histogram.percentileList(100)) {
            System.out.println(percentile);
        }
    }

    @Test
    public void describeTest3() throws Exception{
        HistogramSketch histogram = new HistogramSketch(3);
        System.out.println(histogram.describe());
        for (int i = 0; i < 10000; i++) {
            histogram.recordValue(i);
        }
        System.out.println(histogram.describe());
        for (Percentile percentile : histogram.percentileList(100)) {
            System.out.println(percentile);
        }
        System.out.println(StringUtils.repeat('#', 100));
        histogram = new HistogramSketch(3);
        for (int i = 0; i < 10000; i++) {
            histogram.recordValue(ThreadLocalRandom.current().nextLong(100000));
        }
        System.out.println(histogram.describe());
        for (Percentile percentile : histogram.percentileList(100)) {
            System.out.println(percentile);
        }
    }

}
