package org.apache.druid.query.aggregation.sketch.hlld.sql;


import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.sketch.hlld.HllAggregatorFactory;
import org.apache.druid.query.aggregation.sketch.hlld.HllModule;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.sql.SqlLifecycle;
import org.apache.druid.sql.SqlLifecycleFactory;
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

public class HllApproxCountDistinctSqlAggregatorTest extends CalciteTestBase {
    private static final String DATA_SOURCE = "foo";
    private static final boolean ROUND = true;
    private static final Map<String, Object> QUERY_CONTEXT_DEFAULT = ImmutableMap.of(
            PlannerContext.CTX_SQL_QUERY_ID, "dummy"
    );
    private static QueryRunnerFactoryConglomerate conglomerate;
    private static Closer resourceCloser;
    private static AuthenticationResult authenticationResult = CalciteTests.REGULAR_USER_AUTH_RESULT;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public QueryLogHook queryLogHook = QueryLogHook.create(TestHelper.JSON_MAPPER);

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

    @Before
    public void setUp() throws Exception {
        HllModule.registerSerde();
        for (Module mod : new HllModule().getJacksonModules()) {
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
                .rows(CalciteTests.ROWS1)
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
                        new HllApproxCountDistinctSqlAggregator(),
                        new HllObjectSqlAggregator()
                ),
                ImmutableSet.of(
                        new HllEstimateOperatorConversion()
                )
        );

        SchemaPlus rootSchema = CalciteTests.createMockRootSchema(conglomerate, walker, plannerConfig, AuthTestUtils.TEST_AUTHORIZER_MAPPER);
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
    public void testSqlQuery2() throws Exception {
        SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();
        String sql = "select HLLD_ESTIMATE(HLLD(hll_dim1)) from druid.foo  where dim1 = ''";
        final List<Object[]> results =
                sqlLifecycle.runSimple(sql, QUERY_CONTEXT_DEFAULT, DEFAULT_PARAMETERS, authenticationResult).toList();
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }
    }

    @Test
    public void testAgg() throws Exception {
        SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();

        final String sql = "SELECT\n"
                + "  SUM(cnt),\n"
                + "  APPROX_COUNT_DISTINCT_HLLD(hll_dim1)\n"
                + "FROM druid.foo";

        final List<Object[]> results =
                sqlLifecycle.runSimple(sql, QUERY_CONTEXT_DEFAULT, DEFAULT_PARAMETERS, authenticationResult).toList();
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }

    }


    @Test
    public void testDistinct() throws Exception {
        SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();

        final String sql = "SELECT\n"
                + "  SUM(cnt),\n"
                + "  APPROX_COUNT_DISTINCT_HLLD(dim2),\n" // uppercase
                + "  APPROX_COUNT_DISTINCT_HLLD(dim2) FILTER(WHERE dim2 <> ''),\n" // lowercase; also, filtered
                + "  APPROX_COUNT_DISTINCT_HLLD(SUBSTRING(dim2, 1, 1)),\n" // on extractionFn
                + "  APPROX_COUNT_DISTINCT_HLLD(SUBSTRING(dim2, 1, 1) || 'x'),\n" // on expression
                + "  APPROX_COUNT_DISTINCT_HLLD(hll_dim1, 16),\n" // on native HllSketch column
                + "  APPROX_COUNT_DISTINCT_HLLD(hll_dim1)\n" // on native HllSketch column
                + "FROM druid.foo";

        final List<Object[]> results =
                sqlLifecycle.runSimple(sql, QUERY_CONTEXT_DEFAULT, DEFAULT_PARAMETERS, authenticationResult).toList();
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }

    }

    @Test
    public void testDistinct2() throws Exception {
        SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();

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

        final List<Object[]> results =
                sqlLifecycle.runSimple(sql, QUERY_CONTEXT_DEFAULT, DEFAULT_PARAMETERS, authenticationResult).toList();
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }

    }

    @Test
    public void testDistinctDebug() throws Exception {
        SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();

        final String sql = "SELECT\n"
                + "  SUM(cnt),\n"
                + "  APPROX_COUNT_DISTINCT_HLLD(dim2)\n"
                + "FROM druid.foo";

        final List<Object[]> results =
                sqlLifecycle.runSimple(sql, QUERY_CONTEXT_DEFAULT, DEFAULT_PARAMETERS, authenticationResult).toList();
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }

    }

    @Test
    public void testDeser() throws Exception {
        SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();

        final String sql = "SELECT\n"
                + "  APPROX_COUNT_DISTINCT_HLLD(hll_dim1) cnt\n"
                + "FROM druid.foo";

        final List<Object[]> results =
                sqlLifecycle.runSimple(sql, QUERY_CONTEXT_DEFAULT, DEFAULT_PARAMETERS, authenticationResult).toList();
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }

    }


    @Test
    public void testGroupBy() throws Exception {
        SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();

        final String sql = "SELECT cnt,\n"
                + "  APPROX_COUNT_DISTINCT_HLLD(hll_dim1, 14) cnt2\n"
                + "FROM druid.foo group by cnt";

        final List<Object[]> results =
                sqlLifecycle.runSimple(sql, QUERY_CONTEXT_DEFAULT, DEFAULT_PARAMETERS, authenticationResult).toList();
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }

    }

    @Test
    public void testGroupBy1() throws Exception {
        SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();

        final String sql = "SELECT __time,\n"
                + "  APPROX_COUNT_DISTINCT_HLLD(hll_dim1, 14) cnt\n"
                + "FROM druid.foo group by __time";

        final List<Object[]> results =
                sqlLifecycle.runSimple(sql, QUERY_CONTEXT_DEFAULT, DEFAULT_PARAMETERS, authenticationResult).toList();
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }

    }

    @Test
    public void testGroupBy2() throws Exception {
        SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();

        final String sql = "SELECT __time,\n"
                + "  APPROX_COUNT_DISTINCT_HLLD(hll_dim1, 14) cnt\n"
                + "FROM druid.foo group by __time order by cnt desc";

        final List<Object[]> results =
                sqlLifecycle.runSimple(sql, QUERY_CONTEXT_DEFAULT, DEFAULT_PARAMETERS, authenticationResult).toList();
        for (Object[] result : results) {
            System.out.println(Arrays.toString(result));
        }

    }
}
