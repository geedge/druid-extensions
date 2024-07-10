package org.apache.druid.query.aggregation.sketch.HdrHistogram.sql;

import com.alibaba.fastjson2.JSON;
import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.inject.Injector;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.sketch.HdrHistogram.HdrHistogramAggregatorFactory;
import org.apache.druid.query.aggregation.sketch.HdrHistogram.HdrHistogramMergeAggregatorFactory;
import org.apache.druid.query.aggregation.sketch.HdrHistogram.HdrHistogramModule;
import org.apache.druid.query.aggregation.sketch.HdrHistogram.HdrHistogramToQuantilePostAggregator;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.QueryTestBuilder;
import org.apache.druid.sql.calcite.QueryTestRunner;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.util.*;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class HdrHistogramQuantileSqlAggregatorTest extends BaseCalciteQueryTest {
    @Override
    public void gatherProperties(Properties properties)
    {
        super.gatherProperties(properties);
    }

    @Override
    public void configureGuice(DruidInjectorBuilder builder)
    {
        super.configureGuice(builder);
        builder.addModule(new HdrHistogramModule());
    }

    public static final List<InputRow> ROWS1 = ImmutableList.of(
            TestDataBuilder.createRow(
                    ImmutableMap.<String, Object>builder()
                            .put("t", "2000-01-01")
                            .put("m1", "1")
                            .put("m2", "1.0")
                            .put("dim1", "")
                            .put("dim2", ImmutableList.of("a"))
                            .put("dim3", ImmutableList.of("a", "b"))
                            .build()
            ),
            TestDataBuilder.createRow(
                    ImmutableMap.<String, Object>builder()
                            .put("t", "2000-01-02")
                            .put("m1", "2.0")
                            .put("m2", "2.0")
                            .put("dim1", "10.1")
                            .put("dim2", ImmutableList.of())
                            .put("dim3", ImmutableList.of("b", "c"))
                            .build()
            ),
            TestDataBuilder.createRow(
                    ImmutableMap.<String, Object>builder()
                            .put("t", "2000-01-03")
                            .put("m1", "3.0")
                            .put("m2", "3.0")
                            .put("dim1", "2")
                            .put("dim2", ImmutableList.of(""))
                            .put("dim3", ImmutableList.of("d"))
                            .build()
            ),
            TestDataBuilder.createRow(
                    ImmutableMap.<String, Object>builder()
                            .put("t", "2001-01-01")
                            .put("m1", "4.0")
                            .put("m2", "4.0")
                            .put("dim1", "1")
                            .put("dim2", ImmutableList.of("a"))
                            .put("dim3", ImmutableList.of(""))
                            .build()
            ),
            TestDataBuilder.createRow(
                    ImmutableMap.<String, Object>builder()
                            .put("t", "2001-01-02")
                            .put("m1", "5.0")
                            .put("m2", "5.0")
                            .put("dim1", "def")
                            .put("dim2", ImmutableList.of("abc"))
                            .put("dim3", ImmutableList.of())
                            .build()
            ),
            TestDataBuilder.createRow(
                    ImmutableMap.<String, Object>builder()
                            .put("t", "2001-01-03")
                            .put("m1", "6.0")
                            .put("m2", "6.0")
                            .put("dim1", "abc")
                            .build()
            )
    );

    @SuppressWarnings("resource")
    @Override
    public SpecificSegmentsQuerySegmentWalker createQuerySegmentWalker(
            final QueryRunnerFactoryConglomerate conglomerate,
            final JoinableFactoryWrapper joinableFactory,
            final Injector injector
    ) throws IOException{
        HdrHistogramModule.registerSerde();
        for (Module mod : new HdrHistogramModule().getJacksonModules()) {
            CalciteTests.getJsonMapper().registerModule(mod);
            TestHelper.JSON_MAPPER.registerModule(mod);
        }
        //final QueryableIndex index = TestHelper.getTestIndexIO().loadIndex(new File("D:/doc/datas/testIndex-6201298"));
        /*final QueryableIndex index = IndexBuilder.create()
                .tmpDir(temporaryFolder.newFolder())
                .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                .schema(
                        new IncrementalIndexSchema.Builder()
                                .withMetrics(
                                        new CountAggregatorFactory("cnt"),
                                        new DoubleSumAggregatorFactory("m1", "m1"),
                                        new HdrHistogramAggregatorFactory(
                                                "hist_m1",
                                                "m1",
                                                1L,
                                                100L,
                                                2,
                                                false
                                        )
                                )
                                .withRollup(false)
                                .build()
                )
                //.rows(CalciteTests.ROWS1)
                .rows(ROWS1)
                .buildMMappedIndex();*/

        String[] files = new String[]{
                "D:\\doc\\datas\\statistics_rule_segments\\2023-10-16T00_00_00.000Z_2023-10-17T00_00_00.000Z\\2023-10-16T07_51_47.981Z\\0\\17a457e4-599d-49c2-86e7-6655851bb99a\\index",
                "D:\\doc\\datas\\statistics_rule_segments\\2023-10-15T00_00_00.000Z_2023-10-16T00_00_00.000Z\\2023-10-15T00_00_04.240Z\\15\\9a766f6c-779d-4f9f-9ff5-6a12c19b8c6c\\index"
        };
        files = new String[]{
                "D:/doc/datas/testIndex-6201298"
        };
        SpecificSegmentsQuerySegmentWalker walker = new SpecificSegmentsQuerySegmentWalker(conglomerate);

        for (int i = 0; i < files.length; i++) {
            QueryableIndex index = TestHelper.getTestIndexIO().loadIndex(new File(files[i]));
            return walker.add(
                    DataSegment.builder()
                            .dataSource(CalciteTests.DATASOURCE1)
                            .interval(index.getDataInterval())
                            .version("1")
                            .shardSpec(new LinearShardSpec(i))
                            .size(0)
                            .build(),
                    index
            );
        }

        return walker;
    }

    @Test
    public void testCount0() throws Exception {
        String sql = "select count(1) cnt, APPROX_QUANTILE_HDR(hist_m1, 0.5, 1, 100, 2) from druid.foo where dim1 = 'aaa'";
        QueryTestBuilder builder = testBuilder().sql(sql).skipVectorize();
        builder.run();
        QueryTestRunner.QueryResults queryResults = builder.results();
        List<Object[]> results = queryResults.results;
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }
    }

    @Test
    public void testSqlQueryError() throws Exception {
        String sql = "select min(__time) min_time,max(__time) max_time, HDR_HISTOGRAM(latency_ms_sketch) hdr from druid.foo";
        QueryTestBuilder builder = testBuilder().sql(sql).skipVectorize();
        builder.run();
        QueryTestRunner.QueryResults queryResults = builder.results();
        List<Object[]> results = queryResults.results;
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }
    }

    @Test
    public void testSqlQuery() throws Exception {
        String[] columns = new String[]{"__time", "dim1", "dim2", "dim3", "cnt", "hist_m1", "m1"};
        String sql = "select " + String.join(",", columns) + " from druid.foo";
        QueryTestBuilder builder = testBuilder().sql(sql);
        builder.run();
        QueryTestRunner.QueryResults queryResults = builder.results();
        List<Object[]> results = queryResults.results;
        for (Object[] result : results) {
            Map row = new LinkedHashMap();
            for (int i = 0; i < result.length; i++) {
                row.put(columns[i], result[i]);
            }
            System.out.println(JSON.toJSONString(row));
            // System.out.println(Arrays.toString(result));
        }

        for (int i = 0; i < columns.length; i++) {
            Object[] values = new Object[results.size()];
            for (int j = 0; j < results.size(); j++) {
                values[j] = results.get(j)[i];
            }
            System.out.println(columns[i] + ":" + Arrays.toString(values));
        }
    }

    @Test
    public void testSqlQuery3() throws Exception {
        //cannotVectorize();
        //String sql = "select HLLD_ESTIMATE(HLLD(hll_dim1)) from druid.foo  where dim1 = ''";
        String sql = "select HDR_HISTOGRAM(hist_m1) hdr from druid.foo ";
        QueryTestBuilder builder = testBuilder().sql(sql).skipVectorize();
        builder.run();
        QueryTestRunner.QueryResults queryResults = builder.results();
        List<Object[]> results = queryResults.results;
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }
    }
    @Test
    public void testSqlQuery4() throws Exception {
        //cannotVectorize();
        //String sql = "select HLLD_ESTIMATE(HLLD(hll_dim1)) from druid.foo  where dim1 = ''";
        String sql = "select APPROX_QUANTILE_HDR (hdr, 0.95) as p95th_tcp_latency_ms from (select HDR_HISTOGRAM(hist_m1) hdr from druid.foo) t ";
        QueryTestBuilder builder = testBuilder().sql(sql).skipVectorize();
        builder.run();
        QueryTestRunner.QueryResults queryResults = builder.results();
        List<Object[]> results = queryResults.results;
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }
    }

    @Test
    public void testSqlQuery5() throws Exception {
        //cannotVectorize();
        //String sql = "select HLLD_ESTIMATE(HLLD(hll_dim1)) from druid.foo  where dim1 = ''";
        String sql = "select dim1, APPROX_QUANTILE_HDR (hdr, 0.95) as p95th_tcp_latency_ms from (select dim1, HDR_HISTOGRAM(hist_m1) hdr from druid.foo group by dim1) t group by dim1";
        QueryTestBuilder builder = testBuilder().sql(sql).skipVectorize();
        builder.run();
        QueryTestRunner.QueryResults queryResults = builder.results();
        List<Object[]> results = queryResults.results;
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }
    }

    @Test
    public void testSqlQuery6() throws Exception {
        //cannotVectorize();
        //String sql = "select HLLD_ESTIMATE(HLLD(hll_dim1)) from druid.foo  where dim1 = ''";
        //String sql = "select dim1, APPROX_QUANTILE_HDR (hdr, 0.95) as p95th_tcp_latency_ms from (select dim1, HDR_HISTOGRAM(hist_m1) hdr from druid.foo group by dim1 limit 10) t group by dim1";
        String sql = "select dim1, HDR_GET_QUANTILES(HDR_HISTOGRAM(hdr), 0.95) as p95th_tcp_latency_ms from (select dim1, HDR_HISTOGRAM(hist_m1) hdr from druid.foo group by dim1 limit 10) t group by dim1";
        QueryTestBuilder builder = testBuilder().sql(sql).skipVectorize();
        builder.run();
        QueryTestRunner.QueryResults queryResults = builder.results();
        List<Object[]> results = queryResults.results;
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }
    }

   @Test
    public void testGroup() throws Exception {
        String sql = "select cnt, APPROX_QUANTILE_HDR(hist_m1, 0.5, 1, 100, 2) from druid.foo group by cnt";
       QueryTestBuilder builder = testBuilder().sql(sql).skipVectorize();
       builder.run();
       QueryTestRunner.QueryResults queryResults = builder.results();
       List<Object[]> results = queryResults.results;
       for (Object[] result : results) {
           System.out.println(Arrays.toString(result));
       }
    }

    @Test
    public void testGroup2() throws Exception {
        String sql = "select HDR_HISTOGRAM(hist_m1) from druid.foo";
        QueryTestBuilder builder = testBuilder().sql(sql).skipVectorize();
        builder.run();
        QueryTestRunner.QueryResults queryResults = builder.results();
        List<Object[]> results = queryResults.results;
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }
    }

    @Test
    public void testGroup3() throws Exception {
        String sql = "select APPROX_QUANTILE_HDR(h, 0.5) from(select HDR_HISTOGRAM(hist_m1) h from druid.foo) t";
        QueryTestBuilder builder = testBuilder().sql(sql).skipVectorize();
        builder.run();
        QueryTestRunner.QueryResults queryResults = builder.results();
        List<Object[]> results = queryResults.results;
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }
    }

    @Test
    public void testGroup4() throws Exception {
        String sql = "select hdr_get_quantiles(h, 0.1, 0.2, 0.3, 0.5, 0.9, 0.99, 1) from(select HDR_HISTOGRAM(hist_m1) h from druid.foo) t";
        QueryTestBuilder builder = testBuilder().sql(sql).skipVectorize();
        builder.run();
        QueryTestRunner.QueryResults queryResults = builder.results();
        List<Object[]> results = queryResults.results;
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }
    }

    @Test
    public void testSqlQueryGeneHdr() throws Exception {
        String sql = "select HDR_HISTOGRAM(hist_m1, 1, 100, 2), HDR_HISTOGRAM(cnt, 1, 100, 2)  from druid.foo";
        QueryTestBuilder builder = testBuilder().sql(sql).skipVectorize();
        builder.run();
        QueryTestRunner.QueryResults queryResults = builder.results();
        List<Object[]> results = queryResults.results;
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }
    }

    @Test
    public void testSqlQueryGeneHdr2() throws Exception {
        // HDR_HISTOGRAM(hist_m1, 1, 100, 2),
        String sql = "select HDR_GET_QUANTILES(HDR_HISTOGRAM(m1, 1, 100, 2), 0.1, 0.2, 0.3, 0.5, 0.9, 1) from druid.foo";
        QueryTestBuilder builder = testBuilder().sql(sql).skipVectorize();
        builder.run();
        QueryTestRunner.QueryResults queryResults = builder.results();
        List<Object[]> results = queryResults.results;
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }
    }

    @Test
    public void testSqlQueryGeneHdrArgs() throws Exception {
        String sql = "select HDR_GET_QUANTILEs(HDR_HISTOGRAM(m1), 0.1, 0.2, 0.3, 0.5, 0.9, 1), "
                + "HDR_GET_QUANTILEs(HDR_HISTOGRAM(m1, 2), 0.1, 0.2, 0.3, 0.5, 0.9, 1) ,\n"
                + "HDR_GET_QUANTILEs(HDR_HISTOGRAM(m1, 1, 110, 2), 0.1, 0.2, 0.3, 0.5, 0.9, 1) ,\n"
                + "HDR_GET_QUANTILEs(HDR_HISTOGRAM(m1, 1, 110, 2, false), 0.1, 0.2, 0.3, 0.5, 0.9, 1) \n"
                + "from druid.foo";
        QueryTestBuilder builder = testBuilder().sql(sql).skipVectorize();
        builder.run();
        QueryTestRunner.QueryResults queryResults = builder.results();
        List<Object[]> results = queryResults.results;
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }
    }

     @Test
    public void testSqlQueryGeneHdrArgs2() throws Exception {
        String sql = "select APPROX_QUANTILE_HDR(m1, 0.1), "
                + "APPROX_QUANTILE_HDR(m1, 0.1, 2) ,\n"
                + "APPROX_QUANTILE_HDR(m1, 0.1, 1, 110, 2) ,\n"
                + "APPROX_QUANTILE_HDR(m1, 0.1, 1, 110, 2, false)\n"
                + "from druid.foo";
         QueryTestBuilder builder = testBuilder().sql(sql).skipVectorize();
         builder.run();
         QueryTestRunner.QueryResults queryResults = builder.results();
         List<Object[]> results = queryResults.results;
         for (Object[] result : results) {
             System.out.println(Arrays.toString(result));
         }
    }

    @Test
    public void testSqlQueryGeneHdr3() throws Exception {
        // 函数不区分大小写
        // HDR_HISTOGRAM(hist_m1, 1, 100, 2),
        //String sql = "select HDR_GET_PERCENTILES(HDR_HISTOGRAM(m1, 1, 100, 2)) from druid.foo";
        //String sql = "select hdr_get_percentiles(hdr_histogram(m1, 1, 100, 2)) from druid.foo";
        String sql = "select hdr_get_percentiles(hdr_histogram(hist_m1, 1, 100, 2)) from druid.foo";
        QueryTestBuilder builder = testBuilder().sql(sql).skipVectorize();
        builder.run();
        QueryTestRunner.QueryResults queryResults = builder.results();
        List<Object[]> results = queryResults.results;
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }
    }

    @Test
    public void testSqlQueryQuantiles() throws Exception {
        String sql = "SELECT\n"
                + "APPROX_QUANTILE_HDR(m1, 0.01, 1, 100, 2),\n"
                + "APPROX_QUANTILE_HDR(m1, 0.5, 1, 100, 2),\n"
                + "APPROX_QUANTILE_HDR(m1, 0.98, 1, 100, 2),\n"
                + "APPROX_QUANTILE_HDR(m1, 0.99, 1, 100, 2),\n"
                + "APPROX_QUANTILE_HDR(m1 * 2, 0.97, 1, 100, 2),\n"
                + "APPROX_QUANTILE_HDR(m1, 0.99, 1, 100, 2) FILTER(WHERE dim1 = 'abc'),\n"
                + "APPROX_QUANTILE_HDR(m1, 0.999, 1, 100, 2) FILTER(WHERE dim1 <> 'abc'),\n"
                + "APPROX_QUANTILE_HDR(m1, 0.999, 1, 100, 2) FILTER(WHERE dim1 = 'abc'),\n"
                + "APPROX_QUANTILE_HDR(cnt, 0.5, 1, 100, 2)\n"
                + "FROM foo";
        QueryTestBuilder builder = testBuilder().sql(sql).skipVectorize();
        builder.run();
        QueryTestRunner.QueryResults queryResults = builder.results();
        List<Object[]> results = queryResults.results;
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }
    }

    @Test
    public void testSqlQueryQuantilesOnComplexColumn() throws Exception {
        String sql = "SELECT\n"
                + "APPROX_QUANTILE_HDR(hist_m1, 0.01, 1, 100, 2),\n"
                + "APPROX_QUANTILE_HDR(hist_m1, 0.5, 1, 100, 2),\n"
                + "APPROX_QUANTILE_HDR(hist_m1, 0.98, 1, 100, 2),\n"
                + "APPROX_QUANTILE_HDR(hist_m1, 0.99, 1, 100, 2),\n"
                + "APPROX_QUANTILE_HDR(hist_m1, 0.99, 1, 100, 2) FILTER(WHERE dim1 = 'abc'),\n"
                + "APPROX_QUANTILE_HDR(hist_m1, 0.999, 1, 100, 2) FILTER(WHERE dim1 <> 'abc'),\n"
                + "APPROX_QUANTILE_HDR(hist_m1, 0.999, 1, 100, 2) FILTER(WHERE dim1 = 'abc')\n"
                + "FROM foo";
        QueryTestBuilder builder = testBuilder().sql(sql).skipVectorize();
        builder.run();
        QueryTestRunner.QueryResults queryResults = builder.results();
        List<Object[]> results = queryResults.results;
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }
    }

    @Test
    public void testSqlQueryQuantilesArray() throws Exception {

    }

    @Test
    public void testQuantileOnFloatAndLongs() throws Exception {
        String sql = "SELECT\n"
                + "APPROX_QUANTILE_HDR(m1, 0.01, 1, 100, 2),\n"
                + "APPROX_QUANTILE_HDR(m1, 0.5, 1, 100, 2),\n"
                + "APPROX_QUANTILE_HDR(m1, 0.98, 1, 100, 2),\n"
                + "APPROX_QUANTILE_HDR(m1, 0.99, 1, 100, 2),\n"
                + "APPROX_QUANTILE_HDR(m1 * 2, 0.97, 1, 100, 2),\n"
                + "APPROX_QUANTILE_HDR(m1, 0.99, 1, 100, 2) FILTER(WHERE dim1 = 'abc'),\n"
                + "APPROX_QUANTILE_HDR(m1, 0.999, 1, 100, 2) FILTER(WHERE dim1 <> 'abc'),\n"
                + "APPROX_QUANTILE_HDR(m1, 0.999, 1, 100, 2) FILTER(WHERE dim1 = 'abc'),\n"
                + "APPROX_QUANTILE_HDR(cnt, 0.5, 1, 100, 2)\n"
                + "FROM foo";
        QueryTestBuilder builder = testBuilder().sql(sql).skipVectorize();
        builder = builder.expectedQueries(Collections.singletonList(Druids.newTimeseriesQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                .granularity(Granularities.ALL)
                .virtualColumns(
                        new ExpressionVirtualColumn(
                                "v0",
                                "(\"m1\" * 2)",
                                ColumnType.LONG,
                                TestExprMacroTable.INSTANCE
                        )
                )
                .aggregators(ImmutableList.of(
                        new HdrHistogramAggregatorFactory("a0:agg", "m1", 1L, 100L, 2, true),
                        new HdrHistogramAggregatorFactory("a4:agg", "v0", 1L, 100L, 2, true),
                        new FilteredAggregatorFactory(
                                new HdrHistogramAggregatorFactory("a5:agg", "m1", 1L, 100L, 2, true),
                                new SelectorDimFilter("dim1", "abc", null)
                        ),
                        new FilteredAggregatorFactory(
                                new HdrHistogramAggregatorFactory("a6:agg", "m1", 1L, 100L, 2, true),
                                new NotDimFilter(new SelectorDimFilter("dim1", "abc", null))
                        ),
                        new HdrHistogramAggregatorFactory("a8:agg", "cnt", 1L, 100L, 2, true)
                ))
                .postAggregators(
                        new HdrHistogramToQuantilePostAggregator("a0", "a0:agg", 0.01f),
                        new HdrHistogramToQuantilePostAggregator("a1", "a0:agg", 0.50f),
                        new HdrHistogramToQuantilePostAggregator("a2", "a0:agg", 0.98f),
                        new HdrHistogramToQuantilePostAggregator("a3", "a0:agg", 0.99f),
                        new HdrHistogramToQuantilePostAggregator("a4", "a4:agg", 0.97f),
                        new HdrHistogramToQuantilePostAggregator("a5", "a5:agg", 0.99f),
                        new HdrHistogramToQuantilePostAggregator("a6", "a6:agg", 0.999f),
                        new HdrHistogramToQuantilePostAggregator("a7", "a5:agg", 0.999f),
                        new HdrHistogramToQuantilePostAggregator("a8", "a8:agg", 0.50f)
                )
                .context(QUERY_CONTEXT_DEFAULT)
                .build()));
        builder.run();
        QueryTestRunner.QueryResults queryResults = builder.results();
        List<Object[]> results = queryResults.results;
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }
    }

    @Test
    public void testQuantileOnComplexColumn() throws Exception{
        String sql = "SELECT\n"
                + "APPROX_QUANTILE_HDR(hist_m1, 0.01, 1, 100, 2),\n"
                + "APPROX_QUANTILE_HDR(hist_m1, 0.5, 1, 100, 2),\n"
                + "APPROX_QUANTILE_HDR(hist_m1, 0.98, 1, 100, 2),\n"
                + "APPROX_QUANTILE_HDR(hist_m1, 0.99, 1, 100, 2),\n"
                + "APPROX_QUANTILE_HDR(hist_m1, 0.99, 1, 100, 2) FILTER(WHERE dim1 = 'abc'),\n"
                + "APPROX_QUANTILE_HDR(hist_m1, 0.999, 1, 100, 2) FILTER(WHERE dim1 <> 'abc'),\n"
                + "APPROX_QUANTILE_HDR(hist_m1, 0.999, 1, 100, 2) FILTER(WHERE dim1 = 'abc')\n"
                + "FROM foo";
        QueryTestBuilder builder = testBuilder().sql(sql).skipVectorize();
        builder = builder.expectedQueries(Collections.singletonList(Druids.newTimeseriesQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                .granularity(Granularities.ALL)
                .aggregators(ImmutableList.of(
                        new HdrHistogramMergeAggregatorFactory("a0:agg", "hist_m1", 1L, 100L, 2, true),
                        new FilteredAggregatorFactory(
                                new HdrHistogramMergeAggregatorFactory("a4:agg", "hist_m1", 1L, 100L, 2, true),
                                new SelectorDimFilter("dim1", "abc", null)
                        ),
                        new FilteredAggregatorFactory(
                                new HdrHistogramMergeAggregatorFactory("a5:agg", "hist_m1", 1L, 100L, 2, true),
                                new NotDimFilter(new SelectorDimFilter("dim1", "abc", null))
                        )
                ))
                .postAggregators(
                        new HdrHistogramToQuantilePostAggregator("a0", "a0:agg", 0.01f),
                        new HdrHistogramToQuantilePostAggregator("a1", "a0:agg", 0.50f),
                        new HdrHistogramToQuantilePostAggregator("a2", "a0:agg", 0.98f),
                        new HdrHistogramToQuantilePostAggregator("a3", "a0:agg", 0.99f),
                        new HdrHistogramToQuantilePostAggregator("a4", "a4:agg", 0.99f),
                        new HdrHistogramToQuantilePostAggregator("a5", "a5:agg", 0.999f),
                        new HdrHistogramToQuantilePostAggregator("a6", "a4:agg", 0.999f)
                )
                .context(QUERY_CONTEXT_DEFAULT)
                .build()));
        builder.run();
        QueryTestRunner.QueryResults queryResults = builder.results();
        List<Object[]> results = queryResults.results;
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }



    }

    private static PostAggregator makeFieldAccessPostAgg(String name) {
        return new FieldAccessPostAggregator(name, name);
    }
}