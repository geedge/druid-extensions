package org.apache.druid.query.aggregation.sketch.hlld.sql;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.*;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.aggregation.post.FinalizingFieldAccessPostAggregator;
import org.apache.druid.query.aggregation.sketch.hlld.HllAggregatorFactory;
import org.apache.druid.query.aggregation.sketch.hlld.HllToEstimatePostAggregator;
import org.apache.druid.sql.calcite.aggregation.Aggregation;

import java.util.Collections;

public class HllApproxCountDistinctSqlAggregator extends HllBaseSqlAggregator {
    private static final SqlAggFunction FUNCTION_INSTANCE = new CPCSketchApproxCountDistinctSqlAggFunction();
    private static final String NAME = "APPROX_COUNT_DISTINCT_HLLD";

    public HllApproxCountDistinctSqlAggregator(){
        super(true);
    }

    @Override
    public SqlAggFunction calciteFunction() {
        return FUNCTION_INSTANCE;
    }

    // 新版本参数少了virtualColumns
    @Override
    protected Aggregation toAggregation(
            String name,
            boolean finalizeAggregations,
            AggregatorFactory aggregatorFactory
    ) {
        return Aggregation.create(
                Collections.singletonList(aggregatorFactory),
                //感觉是否是最外层的函数吧
                finalizeAggregations ? new HllToEstimatePostAggregator(
                        name,
                        new FieldAccessPostAggregator(
                                aggregatorFactory.getName(),
                                aggregatorFactory.getName()
                        ),
                        ((HllAggregatorFactory)aggregatorFactory).isRound()
                ) : null
        );
    }

    private static class CPCSketchApproxCountDistinctSqlAggFunction extends SqlAggFunction {
        private static final String SIGNATURE1 = "'" + NAME + "(column, precision)'\n";
        private static final String SIGNATURE2 = "'" + NAME + "(column, precision, round)'\n";

        CPCSketchApproxCountDistinctSqlAggFunction() {
            super(
                    NAME,
                    null,
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.explicit(SqlTypeName.BIGINT),
                    InferTypes.VARCHAR_1024,
                    OperandTypes.or(
                            OperandTypes.ANY,
                            OperandTypes.and(
                                    OperandTypes.sequence(SIGNATURE1, OperandTypes.ANY, OperandTypes.LITERAL),
                                    OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.NUMERIC)
                            ),
                            OperandTypes.and(
                                    OperandTypes.sequence(SIGNATURE2, OperandTypes.ANY, OperandTypes.LITERAL, OperandTypes.LITERAL),
                                    OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.NUMERIC, SqlTypeFamily.BOOLEAN)
                            )
                    ),
                    SqlFunctionCategory.NUMERIC,
                    false,
                    false
            );
        }
    }
}
