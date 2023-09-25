package org.apache.druid.query.aggregation.sketch.hlld.sql;


import com.alibaba.fastjson2.JSON;
import com.fasterxml.jackson.databind.Module;
import com.google.inject.Injector;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.aggregation.sketch.hlld.HllModule;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.QueryTestBuilder;
import org.apache.druid.sql.calcite.QueryTestRunner;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.util.*;

// 新版本父类直接变了，实现更简单了
public class HllApproxCountDistinctSqlAggregatorTest extends BaseCalciteQueryTest {
    private static final boolean ROUND = true;

    @Override
    public void gatherProperties(Properties properties)
    {
        super.gatherProperties(properties);
    }

    @Override
    public void configureGuice(DruidInjectorBuilder builder)
    {
        super.configureGuice(builder);
        builder.addModule(new HllModule());
    }



    @SuppressWarnings("resource")
    @Override
    public SpecificSegmentsQuerySegmentWalker createQuerySegmentWalker(
            final QueryRunnerFactoryConglomerate conglomerate,
            final JoinableFactoryWrapper joinableFactory,
            final Injector injector
    ) throws IOException
    {
        HllModule.registerSerde();
        for (Module mod : new HllModule().getJacksonModules()) {
            CalciteTests.getJsonMapper().registerModule(mod);
            TestHelper.JSON_MAPPER.registerModule(mod);
        }

        final QueryableIndex index = TestHelper.getTestIndexIO().loadIndex(new File("D:/doc/datas/testIndex-1369101812"));
        //final QueryableIndex index = TestHelper.getTestIndexIO().loadIndex(new File("D:/doc/datas/9_index"));
       /*final QueryableIndex index = IndexBuilder.create()
                .tmpDir(temporaryFolder.newFolder())
                .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                .schema(
                        new IncrementalIndexSchema.Builder()
                                .withMetrics(
                                        new CountAggregatorFactory("cnt"),
                                        new DoubleSumAggregatorFactory("m1", "m1"),
                                        new HllAggregatorFactory(
                                                "hll_dim1",
                                                "dim1",
                                                null,
                                                ROUND
                                        )
                                )
                                .withRollup(false)
                                .build()
                )
                .rows(TestDataBuilder.ROWS1)
                .buildMMappedIndex();*/

        return new SpecificSegmentsQuerySegmentWalker(conglomerate).add(
                DataSegment.builder()
                        .dataSource(CalciteTests.DATASOURCE1)
                        .interval(index.getDataInterval())
                        .version("1")
                        .shardSpec(new LinearShardSpec(0))
                        .size(0)
                        .build(),
                index
        );
    }

    @Test
    public void testSqlQuery() throws Exception {
        // Can't vectorize due to SUBSTRING expression.
        cannotVectorize();
        String[] columns = new String[]{"__time", "dim1", "dim2", "dim3", "cnt", "hll_dim1", "m1"};

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
    public void testSqlQuery1() throws Exception {
        // Can't vectorize due to SUBSTRING expression.
        cannotVectorize();

        String sql = "select dim1 from druid.foo";
        QueryTestBuilder builder = testBuilder().sql(sql);
        builder.run();
        QueryTestRunner.QueryResults queryResults = builder.results();
        List<Object[]> results = queryResults.results;
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }
    }

    @Test
    public void testSqlQuery2() throws Exception {
        //cannotVectorize();
        //String sql = "select HLLD_ESTIMATE(HLLD(hll_dim1)) from druid.foo  where dim1 = '1'";
        // Caused by: org.apache.calcite.sql.validate.SqlValidatorException: Aggregate expressions cannot be nested
        //String sql = "select HLLD_ESTIMATE(HLLD(hll_dim1)), APPROX_COUNT_DISTINCT_HLLD(HLLD(hll_dim1)), HLLD(hll_dim1) from druid.foo";
        String sql = "select HLLD_ESTIMATE(HLLD(hll_dim1)), APPROX_COUNT_DISTINCT_HLLD(hll_dim1), HLLD(hll_dim1) from (select HLLD(hll_dim1) hll_dim1 from druid.foo) t";
        QueryTestBuilder builder = testBuilder().sql(sql).skipVectorize();
        builder.run();
        QueryTestRunner.QueryResults queryResults = builder.results();
        List<Object[]> results = queryResults.results;
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }
    }

    @Test
    public void testSqlQuery3() throws Exception {
        //cannotVectorize();
        //String sql = "select HLLD_ESTIMATE(HLLD(hll_dim1)) from druid.foo  where dim1 = ''";
        String sql = "select APPROX_COUNT_DISTINCT_HLLD(hll, 12) from (select HLLD(hll_dim1) hll from druid.foo where dim1 = '1') t ";
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
        String sql = "select APPROX_COUNT_DISTINCT_HLLD(hll, 12) from (select HLLD(hll_dim1) hll from druid.foo where dim1 = '1') t ";
        QueryTestBuilder builder = testBuilder().sql(sql).skipVectorize();
        builder.run();
        QueryTestRunner.QueryResults queryResults = builder.results();
        List<Object[]> results = queryResults.results;
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }
    }

   @Test
    public void testAgg() throws Exception {
        final String sql = "SELECT\n"
                + "  SUM(cnt),\n"
                + "  APPROX_COUNT_DISTINCT_HLLD(hll_dim1)\n"
                + "FROM druid.foo";

       QueryTestBuilder builder = testBuilder().sql(sql).skipVectorize();
       builder.run();
       QueryTestRunner.QueryResults queryResults = builder.results();
       List<Object[]> results = queryResults.results;
       for (Object[] result : results) {
           System.out.println(Arrays.toString(result));
       }

    }

     @Test
    public void testDistinct() throws Exception {
        final String sql = "SELECT\n"
                + "  SUM(cnt),\n"
                + "  APPROX_COUNT_DISTINCT_HLLD(dim2),\n" // uppercase
                + "  APPROX_COUNT_DISTINCT_HLLD(dim2) FILTER(WHERE dim2 <> ''),\n" // lowercase; also, filtered
                + "  APPROX_COUNT_DISTINCT_HLLD(SUBSTRING(dim2, 1, 1)),\n" // on extractionFn
                + "  APPROX_COUNT_DISTINCT_HLLD(SUBSTRING(dim2, 1, 1) || 'x'),\n" // on expression
                + "  APPROX_COUNT_DISTINCT_HLLD(hll_dim1, 16),\n" // on native HllSketch column
                + "  APPROX_COUNT_DISTINCT_HLLD(hll_dim1)\n" // on native HllSketch column
                + "FROM druid.foo";

         QueryTestBuilder builder = testBuilder().sql(sql).skipVectorize();
         builder.run();
         QueryTestRunner.QueryResults queryResults = builder.results();
         List<Object[]> results = queryResults.results;
         for (Object[] result : results) {
             System.out.println(Arrays.toString(result));
         }
    }

    @Test
    public void testDistinct2() throws Exception {
        final String sql = "SELECT\n"
                + "  SUM(cnt),\n"
                + "  APPROX_COUNT_DISTINCT_HLLD(dim2),\n"
                + "  HLLD(dim2),\n"
                + "  HLLD(hll_dim1),\n"
                + "  HLLD_ESTIMATE(HLLD(dim2)),\n"
                + "  HLLD_ESTIMATE(HLLD(dim2), true),\n"
                + "  HLLD_ESTIMATE(HLLD(dim1), true),\n"
                + "  HLLD_ESTIMATE(HLLD(hll_dim1)),\n" // on native HllSketch column
                + "  APPROX_COUNT_DISTINCT_HLLD(hll_dim1)\n" // on native HllSketch column
                + "FROM druid.foo";

        QueryTestBuilder builder = testBuilder().sql(sql).skipVectorize();
        builder.run();
        QueryTestRunner.QueryResults queryResults = builder.results();
        List<Object[]> results = queryResults.results;
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }

    }

    @Test
    public void testDistinctDebug2() throws Exception {
        final String sql = "SELECT\n"
                + "  dim1, dim2\n"
                + "FROM druid.foo";

        QueryTestBuilder builder = testBuilder().sql(sql).skipVectorize();
        builder.run();
        QueryTestRunner.QueryResults queryResults = builder.results();
        List<Object[]> results = queryResults.results;
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }

    }

    @Test
    public void testDistinctDebug() throws Exception {
        final String sql = "SELECT\n"
                + "  SUM(cnt),\n"
                + "  APPROX_COUNT_DISTINCT_HLLD(dim2)\n"
                + "FROM druid.foo";

        QueryTestBuilder builder = testBuilder().sql(sql).skipVectorize();
        builder.run();
        QueryTestRunner.QueryResults queryResults = builder.results();
        List<Object[]> results = queryResults.results;
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }

    }

    @Test
    public void testDeser() throws Exception {
        final String sql = "SELECT\n"
                + "  APPROX_COUNT_DISTINCT_HLLD(hll_dim1) cnt\n"
                + "FROM druid.foo";

        QueryTestBuilder builder = testBuilder().sql(sql).skipVectorize();
        builder.run();
        QueryTestRunner.QueryResults queryResults = builder.results();
        List<Object[]> results = queryResults.results;
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }

    }


    @Test
    public void testGroupBy() throws Exception {
        final String sql = "SELECT cnt,\n"
                + "  APPROX_COUNT_DISTINCT_HLLD(hll_dim1, 14) cnt2\n"
                + "FROM druid.foo group by cnt";

        QueryTestBuilder builder = testBuilder().sql(sql).skipVectorize();
        builder.run();
        QueryTestRunner.QueryResults queryResults = builder.results();
        List<Object[]> results = queryResults.results;
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }
    }

    @Test
    public void testGroupBy1() throws Exception {
        final String sql = "SELECT __time,\n"
                + "  APPROX_COUNT_DISTINCT_HLLD(hll_dim1, 14) cnt\n"
                + "FROM druid.foo group by __time";

        QueryTestBuilder builder = testBuilder().sql(sql).skipVectorize();
        builder.run();
        QueryTestRunner.QueryResults queryResults = builder.results();
        List<Object[]> results = queryResults.results;
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }

    }

    @Test
    public void testGroupBy2() throws Exception {
        final String sql = "SELECT __time,\n"
                + "  APPROX_COUNT_DISTINCT_HLLD(hll_dim1, 14) cnt\n"
                + "FROM druid.foo group by __time order by cnt desc";
        QueryTestBuilder builder = testBuilder().sql(sql).skipVectorize();
        builder.run();
        QueryTestRunner.QueryResults queryResults = builder.results();
        List<Object[]> results = queryResults.results;
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }

    }
}
