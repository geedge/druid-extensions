package org.apache.druid.query.udf.sql;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import javax.annotation.Nullable;

public class DimensionBucketOperatorConversion implements SqlOperatorConversion {
    private static final SqlFunction SQL_FUNCTION = new SqlFunction(
            "DIMENSION_BUCKET",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(
                    factory -> Calcites.createSqlTypeWithNullability(factory, SqlTypeName.VARCHAR, true)
            ),
            null,
            OperandTypes.variadic(SqlOperandCountRanges.from(2)),
            SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    @Override
    public SqlOperator calciteOperator() {
        return SQL_FUNCTION;
    }

    @Nullable
    @Override
    public DruidExpression toDruidExpression(PlannerContext plannerContext, RowSignature rowSignature, RexNode rexNode) {
        return OperatorConversions.convertDirectCall(plannerContext, rowSignature, rexNode, "dimension_bucket");
    }
}
