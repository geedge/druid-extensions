package org.apache.druid.query.aggregation.sketch.hlld;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Binder;
import com.zdjz.galaxy.sketch.hlld.Hll;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.query.aggregation.sketch.hlld.sql.HllApproxCountDistinctSqlAggregator;
import org.apache.druid.query.aggregation.sketch.hlld.sql.HllEstimateOperatorConversion;
import org.apache.druid.query.aggregation.sketch.hlld.sql.HllObjectSqlAggregator;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.sql.guice.SqlBindings;

import java.util.Collections;
import java.util.List;

public class HllModule implements DruidModule {
    public static final byte CACHE_TYPE_ID_OFFSET = (byte) 0xFF;
    public static final byte HLLD_BUILD_CACHE_TYPE_ID = 21;
    public static final byte HLLD_MERGE_CACHE_TYPE_ID = 22;
    public static final byte HLLD_TO_ESTIMATE_CACHE_TYPE_ID = 23;

    public static final String HLLD_TYPE_NAME = "HLLDSketch";
    public static final String HLLD_BUILD_TYPE_NAME = "HLLDSketchBuild";
    public static final ColumnType TYPE = ColumnType.ofComplex(HLLD_TYPE_NAME);
    public static final ColumnType BUILD_TYPE = ColumnType.ofComplex(HLLD_BUILD_TYPE_NAME);


    @Override
    public void configure(Binder binder) {
        registerSerde();
        SqlBindings.addAggregator(binder, HllApproxCountDistinctSqlAggregator.class);
        SqlBindings.addAggregator(binder, HllObjectSqlAggregator.class);

        SqlBindings.addOperatorConversion(binder, HllEstimateOperatorConversion.class);
    }

    @Override
    public List<? extends Module> getJacksonModules() {
        return Collections.<Module>singletonList(
                new SimpleModule("HLLDSketchModule")
                        .registerSubtypes(
                                new NamedType(HllAggregatorFactory.class, "HLLDSketchBuild"),
                                new NamedType(HllMergeAggregatorFactory.class, "HLLDSketchMerge"),
                                new NamedType(HllToEstimatePostAggregator.class, "HLLDSketchEstimate")
                        ).addSerializer(Hll.class, new HllJsonSerializer())
        );
    }

    @VisibleForTesting
    public static void registerSerde() {
        ComplexMetrics.registerSerde(HLLD_TYPE_NAME, new HllMergeSerde());
        ComplexMetrics.registerSerde(HLLD_BUILD_TYPE_NAME, new HllBuildMergeSerde());
    }
}
