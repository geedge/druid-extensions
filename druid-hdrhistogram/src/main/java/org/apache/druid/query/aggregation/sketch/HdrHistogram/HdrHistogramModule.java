package org.apache.druid.query.aggregation.sketch.HdrHistogram;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Binder;
import org.HdrHistogram.HistogramSketch;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.query.aggregation.sketch.HdrHistogram.sql.HdrHistogramObjectSqlAggregator;
import org.apache.druid.query.aggregation.sketch.HdrHistogram.sql.HdrHistogramPercentilesOperatorConversion;
import org.apache.druid.query.aggregation.sketch.HdrHistogram.sql.HdrHistogramQuantileSqlAggregator;
import org.apache.druid.query.aggregation.sketch.HdrHistogram.sql.HdrHistogramQuantilesOperatorConversion;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.sql.guice.SqlBindings;

import java.util.Collections;
import java.util.List;

public class HdrHistogramModule implements DruidModule {
    public static final byte CACHE_TYPE_ID_OFFSET = (byte) 0xFF;
    public static final byte QUANTILES_HDRHISTOGRAM_BUILD_CACHE_TYPE_ID = 0x01;
    public static final byte QUANTILES_HDRHISTOGRAM_MERGE_CACHE_TYPE_ID = 0x02;
    public static final byte QUANTILES_HDRHISTOGRAM_TO_QUANTILE_CACHE_TYPE_ID = 0x03;
    public static final byte QUANTILES_HDRHISTOGRAM_TO_QUANTILES_CACHE_TYPE_ID = 0x04;
    public static final byte QUANTILES_HDRHISTOGRAM_TO_PERCENTILES_CACHE_TYPE_ID = 0x05;

    public static final String HDRHISTOGRAM_TYPE_NAME = "HdrHistogramSketch";
    public static final ColumnType TYPE = ColumnType.ofComplex(HDRHISTOGRAM_TYPE_NAME);

    public static final ObjectMapper objectMapper = new ObjectMapper();

    public static String toJson(Object data){
        try {
            return objectMapper.writeValueAsString(data);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void configure(Binder binder) {
        registerSerde();
        SqlBindings.addAggregator(binder, HdrHistogramQuantileSqlAggregator.class);
        SqlBindings.addAggregator(binder, HdrHistogramObjectSqlAggregator.class);

        SqlBindings.addOperatorConversion(binder, HdrHistogramQuantilesOperatorConversion.class);
        SqlBindings.addOperatorConversion(binder, HdrHistogramPercentilesOperatorConversion.class);
    }

    @Override
    public List<? extends Module> getJacksonModules() {
        return Collections.<Module>singletonList(
                new SimpleModule("HdrHistogramSketchModule")
                        .registerSubtypes(
                                new NamedType(HdrHistogramAggregatorFactory.class, "HdrHistogramSketchBuild"),
                                new NamedType(HdrHistogramMergeAggregatorFactory.class, "HdrHistogramSketchMerge"),
                                new NamedType(HdrHistogramToQuantilePostAggregator.class, "HdrHistogramSketchToQuantile"),
                                new NamedType(HdrHistogramToQuantilesPostAggregator.class, "HdrHistogramSketchToQuantiles"),
                                new NamedType(HdrHistogramToPercentilesPostAggregator.class, "HdrHistogramSketchToPercentiles")
                        ).addSerializer(HistogramSketch.class, new HistogramJsonSerializer())
        );
    }

    @VisibleForTesting
    public static void registerSerde() {
        ComplexMetrics.registerSerde(HDRHISTOGRAM_TYPE_NAME, new HdrHistogramMergeSerde());
    }

}
