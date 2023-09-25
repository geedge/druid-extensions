package org.apache.druid.query.aggregation.sketch.hlld.sql;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.*;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.sql.calcite.aggregation.Aggregation;

import java.util.Collections;

public class HllObjectSqlAggregator extends HllBaseSqlAggregator {
    private static final SqlAggFunction FUNCTION_INSTANCE = new CpcSketchSqlAggFunction();
    private static final String NAME = "HLLD";

    public HllObjectSqlAggregator(){
        super(false);
    }

    @Override
    public SqlAggFunction calciteFunction() {
        return FUNCTION_INSTANCE;
    }

    @Override
    protected Aggregation toAggregation(
            String name,
            boolean finalizeAggregations,
            AggregatorFactory aggregatorFactory
    ) {
        return Aggregation.create(
                Collections.singletonList(aggregatorFactory),
                null
        );
    }

    private static class CpcSketchSqlAggFunction extends SqlAggFunction {
        private static final String SIGNATURE1 = "'" + NAME + "(column, precision)'\n";
        private static final String SIGNATURE2 = "'" + NAME + "(column, precision, round)'\n";

        CpcSketchSqlAggFunction() {
            super(
                    NAME,
                    null,
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.explicit(SqlTypeName.OTHER),
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
                    SqlFunctionCategory.USER_DEFINED_FUNCTION,
                    false,
                    false
            );
        }
    }
}
