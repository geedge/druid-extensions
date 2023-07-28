package org.apache.druid.query.aggregation.sketch.HdrHistogram.sql;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.druid.data.input.InputRow;
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
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.sql.SqlLifecycle;
import org.apache.druid.sql.SqlLifecycleFactory;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.QueryLogHook;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class HdrHistogramQuantileSqlAggregatorTest extends CalciteTestBase {
    private static final String DATA_SOURCE = "foo";

    private static QueryRunnerFactoryConglomerate conglomerate;
    private static Closer resourceCloser;
    private static AuthenticationResult authenticationResult = CalciteTests.REGULAR_USER_AUTH_RESULT;
    private static final Map<String, Object> QUERY_CONTEXT_DEFAULT = ImmutableMap.of(
            PlannerContext.CTX_SQL_QUERY_ID, "dummy"
    );

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public QueryLogHook queryLogHook = QueryLogHook.create();

    private SpecificSegmentsQuerySegmentWalker walker;
    private SqlLifecycleFactory sqlLifecycleFactory;

    @BeforeClass
    public static void setUpClass() {
        resourceCloser = Closer.create();
        conglomerate = QueryStackTests.createQueryRunnerFactoryConglomerate(resourceCloser);
    }

    @AfterClass
    public static void tearDownClass() throws IOException {
        resourceCloser.close();
    }

    public static final List<InputRow> ROWS1 = ImmutableList.of(
            CalciteTests.createRow(
                    ImmutableMap.<String, Object>builder()
                            .put("t", "2000-01-01")
                            .put("m1", "1")
                            .put("m2", "1.0")
                            .put("dim1", "")
                            .put("dim2", ImmutableList.of("a"))
                            .put("dim3", ImmutableList.of("a", "b"))
                            .build()
            ),
            CalciteTests.createRow(
                    ImmutableMap.<String, Object>builder()
                            .put("t", "2000-01-02")
                            .put("m1", "2.0")
                            .put("m2", "2.0")
                            .put("dim1", "10.1")
                            .put("dim2", ImmutableList.of())
                            .put("dim3", ImmutableList.of("b", "c"))
                            .build()
            ),
            CalciteTests.createRow(
                    ImmutableMap.<String, Object>builder()
                            .put("t", "2000-01-03")
                            .put("m1", "3.0")
                            .put("m2", "3.0")
                            .put("dim1", "2")
                            .put("dim2", ImmutableList.of(""))
                            .put("dim3", ImmutableList.of("d"))
                            .build()
            ),
            CalciteTests.createRow(
                    ImmutableMap.<String, Object>builder()
                            .put("t", "2001-01-01")
                            .put("m1", "4.0")
                            .put("m2", "4.0")
                            .put("dim1", "1")
                            .put("dim2", ImmutableList.of("a"))
                            .put("dim3", ImmutableList.of(""))
                            .build()
            ),
            CalciteTests.createRow(
                    ImmutableMap.<String, Object>builder()
                            .put("t", "2001-01-02")
                            .put("m1", "5.0")
                            .put("m2", "5.0")
                            .put("dim1", "def")
                            .put("dim2", ImmutableList.of("abc"))
                            .put("dim3", ImmutableList.of())
                            .build()
            ),
            CalciteTests.createRow(
                    ImmutableMap.<String, Object>builder()
                            .put("t", "2001-01-03")
                            .put("m1", "6.0")
                            .put("m2", "6.0")
                            .put("dim1", "abc")
                            .build()
            )
    );

    @Before
    public void setUp() throws Exception {
        HdrHistogramModule.registerSerde();
        for (Module mod : new HdrHistogramModule().getJacksonModules()) {
            CalciteTests.getJsonMapper().registerModule(mod);
            TestHelper.JSON_MAPPER.registerModule(mod);
        }

        final QueryableIndex index = IndexBuilder.create()
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
                .buildMMappedIndex();

        walker = new SpecificSegmentsQuerySegmentWalker(conglomerate).add(
                DataSegment.builder()
                        .dataSource(DATA_SOURCE)
                        .interval(index.getDataInterval())
                        .version("1")
                        .shardSpec(new LinearShardSpec(0))
                        .size(0)
                        .build(),
                index
        );

        final PlannerConfig plannerConfig = new PlannerConfig();
        final DruidOperatorTable operatorTable = new DruidOperatorTable(
                ImmutableSet.of(
                        new HdrHistogramQuantileSqlAggregator(),
                        new HdrHistogramObjectSqlAggregator()
                ),
                ImmutableSet.of(
                        new HdrHistogramQuantilesOperatorConversion(),
                        new HdrHistogramPercentilesOperatorConversion()
                )
        );
        SchemaPlus rootSchema =
                CalciteTests.createMockRootSchema(conglomerate, walker, plannerConfig, AuthTestUtils.TEST_AUTHORIZER_MAPPER);

        sqlLifecycleFactory = CalciteTests.createSqlLifecycleFactory(
                new PlannerFactory(
                        rootSchema,
                        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
                        operatorTable,
                        CalciteTests.createExprMacroTable(),
                        plannerConfig,
                        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
                        CalciteTests.getJsonMapper(),
                        CalciteTests.DRUID_SCHEMA_NAME
                )
        );
    }

    @After
    public void tearDown() throws Exception {
        walker.close();
        walker = null;
    }

    @Test
    public void testSqlQuery() throws Exception {
        SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();
        String sql = "select * from druid.foo";
        final List<Object[]> results =
                sqlLifecycle.runSimple(sql, QUERY_CONTEXT_DEFAULT, DEFAULT_PARAMETERS, authenticationResult).toList();
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }
    }

    @Test
    public void testGroup() throws Exception {
        SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();
        String sql = "select cnt, APPROX_QUANTILE_HDR(hist_m1, 0.5, 1, 100, 2) from druid.foo group by cnt";
        final List<Object[]> results =
                sqlLifecycle.runSimple(sql, QUERY_CONTEXT_DEFAULT, DEFAULT_PARAMETERS, authenticationResult).toList();
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }
    }

    @Test
    public void testGroup2() throws Exception {
        SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();
        String sql = "select HDR_HISTOGRAM(hist_m1) from druid.foo";
        final List<Object[]> results =
                sqlLifecycle.runSimple(sql, QUERY_CONTEXT_DEFAULT, DEFAULT_PARAMETERS, authenticationResult).toList();
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }
    }

    @Test
    public void testSqlQueryGeneHdr() throws Exception {
        SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();
        String sql = "select HDR_HISTOGRAM(hist_m1, 1, 100, 2), HDR_HISTOGRAM(cnt, 1, 100, 2)  from druid.foo";
        final List<Object[]> results =
                sqlLifecycle.runSimple(sql, QUERY_CONTEXT_DEFAULT, DEFAULT_PARAMETERS, authenticationResult).toList();
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }
    }

    @Test
    public void testSqlQueryGeneHdr2() throws Exception {
        SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();
        // HDR_HISTOGRAM(hist_m1, 1, 100, 2),
        String sql = "select HDR_GET_QUANTILES(HDR_HISTOGRAM(m1, 1, 100, 2), 0.1, 0.2, 0.3, 0.5, 0.9, 1) from druid.foo";
        final List<Object[]> results =
                sqlLifecycle.runSimple(sql, QUERY_CONTEXT_DEFAULT, DEFAULT_PARAMETERS, authenticationResult).toList();
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }
    }

    @Test
    public void testSqlQueryGeneHdrArgs() throws Exception {
        SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();
        String sql = "select HDR_GET_QUANTILEs(HDR_HISTOGRAM(m1), 0.1, 0.2, 0.3, 0.5, 0.9, 1), "
                + "HDR_GET_QUANTILEs(HDR_HISTOGRAM(m1, 2), 0.1, 0.2, 0.3, 0.5, 0.9, 1) ,\n"
                + "HDR_GET_QUANTILEs(HDR_HISTOGRAM(m1, 1, 110, 2), 0.1, 0.2, 0.3, 0.5, 0.9, 1) ,\n"
                + "HDR_GET_QUANTILEs(HDR_HISTOGRAM(m1, 1, 110, 2, false), 0.1, 0.2, 0.3, 0.5, 0.9, 1) \n"
                + "from druid.foo";
        final List<Object[]> results =
                sqlLifecycle.runSimple(sql, QUERY_CONTEXT_DEFAULT, DEFAULT_PARAMETERS, authenticationResult).toList();
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }
    }

    @Test
    public void testSqlQueryGeneHdrArgs2() throws Exception {
        SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();
        String sql = "select APPROX_QUANTILE_HDR(m1, 0.1), "
                + "APPROX_QUANTILE_HDR(m1, 0.1, 2) ,\n"
                + "APPROX_QUANTILE_HDR(m1, 0.1, 1, 110, 2) ,\n"
                + "APPROX_QUANTILE_HDR(m1, 0.1, 1, 110, 2, false)\n"
                + "from druid.foo";
        final List<Object[]> results =
                sqlLifecycle.runSimple(sql, QUERY_CONTEXT_DEFAULT, DEFAULT_PARAMETERS, authenticationResult).toList();
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }
    }

    @Test
    public void testSqlQueryGeneHdr3() throws Exception {
        SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();
        // 函数不区分大小写
        // HDR_HISTOGRAM(hist_m1, 1, 100, 2),
        //String sql = "select HDR_GET_PERCENTILES(HDR_HISTOGRAM(m1, 1, 100, 2)) from druid.foo";
        //String sql = "select hdr_get_percentiles(hdr_histogram(m1, 1, 100, 2)) from druid.foo";
        String sql = "select hdr_get_percentiles(hdr_histogram(hist_m1, 1, 100, 2)) from druid.foo";
        final List<Object[]> results =
                sqlLifecycle.runSimple(sql, QUERY_CONTEXT_DEFAULT, DEFAULT_PARAMETERS, authenticationResult).toList();
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }
    }

    @Test
    public void testSqlQueryQuantiles() throws Exception {
        SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();
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
        final List<Object[]> results =
                sqlLifecycle.runSimple(sql, QUERY_CONTEXT_DEFAULT, DEFAULT_PARAMETERS, authenticationResult).toList();
        System.out.println(sql);
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }
    }

    @Test
    public void testSqlQueryQuantilesOnComplexColumn() throws Exception {
        SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();
        String sql = "SELECT\n"
                + "APPROX_QUANTILE_HDR(hist_m1, 0.01, 1, 100, 2),\n"
                + "APPROX_QUANTILE_HDR(hist_m1, 0.5, 1, 100, 2),\n"
                + "APPROX_QUANTILE_HDR(hist_m1, 0.98, 1, 100, 2),\n"
                + "APPROX_QUANTILE_HDR(hist_m1, 0.99, 1, 100, 2),\n"
                + "APPROX_QUANTILE_HDR(hist_m1, 0.99, 1, 100, 2) FILTER(WHERE dim1 = 'abc'),\n"
                + "APPROX_QUANTILE_HDR(hist_m1, 0.999, 1, 100, 2) FILTER(WHERE dim1 <> 'abc'),\n"
                + "APPROX_QUANTILE_HDR(hist_m1, 0.999, 1, 100, 2) FILTER(WHERE dim1 = 'abc')\n"
                + "FROM foo";
        final List<Object[]> results =
                sqlLifecycle.runSimple(sql, QUERY_CONTEXT_DEFAULT, DEFAULT_PARAMETERS, authenticationResult).toList();
        System.out.println(sql);
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }
    }

    @Test
    public void testSqlQueryQuantilesArray() throws Exception {

    }

    @Test
    public void testQuantileOnFloatAndLongs() throws Exception {
        SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();
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
        final List<Object[]> results =
                sqlLifecycle.runSimple(sql, QUERY_CONTEXT_DEFAULT, DEFAULT_PARAMETERS, authenticationResult).toList();
        System.out.println(sql);
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }

        // Verify query
        Assert.assertEquals(
                Druids.newTimeseriesQueryBuilder()
                        .dataSource(CalciteTests.DATASOURCE1)
                        .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                        .granularity(Granularities.ALL)
                        .virtualColumns(
                                new ExpressionVirtualColumn(
                                        "v0",
                                        "(\"m1\" * 2)",
                                        ValueType.LONG,
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
                        .context(ImmutableMap.of("skipEmptyBuckets", true, PlannerContext.CTX_SQL_QUERY_ID, "dummy"))
                        .build(),
                Iterables.getOnlyElement(queryLogHook.getRecordedQueries())
        );
    }

    @Test
    public void testQuantileOnComplexColumn() throws Exception{
        SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();
        String sql = "SELECT\n"
                + "APPROX_QUANTILE_HDR(hist_m1, 0.01, 1, 100, 2),\n"
                + "APPROX_QUANTILE_HDR(hist_m1, 0.5, 1, 100, 2),\n"
                + "APPROX_QUANTILE_HDR(hist_m1, 0.98, 1, 100, 2),\n"
                + "APPROX_QUANTILE_HDR(hist_m1, 0.99, 1, 100, 2),\n"
                + "APPROX_QUANTILE_HDR(hist_m1, 0.99, 1, 100, 2) FILTER(WHERE dim1 = 'abc'),\n"
                + "APPROX_QUANTILE_HDR(hist_m1, 0.999, 1, 100, 2) FILTER(WHERE dim1 <> 'abc'),\n"
                + "APPROX_QUANTILE_HDR(hist_m1, 0.999, 1, 100, 2) FILTER(WHERE dim1 = 'abc')\n"
                + "FROM foo";
        final List<Object[]> results =
                sqlLifecycle.runSimple(sql, QUERY_CONTEXT_DEFAULT, DEFAULT_PARAMETERS, authenticationResult).toList();
        System.out.println(sql);
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }

        // Verify query
        Assert.assertEquals(
                Druids.newTimeseriesQueryBuilder()
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
                        .context(ImmutableMap.of("skipEmptyBuckets", true, PlannerContext.CTX_SQL_QUERY_ID, "dummy"))
                        .build(),
                Iterables.getOnlyElement(queryLogHook.getRecordedQueries())
        );
    }

    private static PostAggregator makeFieldAccessPostAgg(String name) {
        return new FieldAccessPostAggregator(name, name);
    }
}