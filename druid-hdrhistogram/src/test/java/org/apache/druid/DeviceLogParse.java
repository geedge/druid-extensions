package org.apache.druid;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import org.HdrHistogram.ArrayHistogram;
import org.HdrHistogram.HistogramSketch;
import org.HdrHistogram.HistogramUnion;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.sketch.HdrHistogram.HistogramUtils;

import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.Charset;
import java.util.List;

public class DeviceLogParse {

    public static void main(String[] args) throws Exception{
        FileInputStream inputStream = new FileInputStream(new File("D:\\doc\\histogram_metric.json"));
        LineIterator lineIterator = IOUtils.lineIterator(inputStream, Charset.forName("utf-8"));
        HistogramUnion union = new HistogramUnion(3);
        while (lineIterator.hasNext()){
            String line = lineIterator.next();
            JSONArray array = JSON.parseArray(line);
            for (int i = 0; i < array.size(); i++) {
                JSONObject data = array.getJSONObject(i);
                String string = data.getJSONObject("fields").getString("in_latency_ms_sketch");
                HistogramSketch hdr = HistogramSketch.fromBytes(StringUtils.decodeBase64(StringUtils.toUtf8(string)));
                ((ArrayHistogram)(hdr.hisImpl)).outputPercentileDistribution(System.out, 1D);
                System.out.println("#####################");
                union.update(hdr);
            }

        }
        ((ArrayHistogram)(union.getResult().hisImpl)).outputPercentileDistribution(System.out, 1D);
        inputStream.close();
    }

}
