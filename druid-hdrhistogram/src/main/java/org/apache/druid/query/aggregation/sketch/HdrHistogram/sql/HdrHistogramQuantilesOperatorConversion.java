package org.apache.druid.query.aggregation.sketch.HdrHistogram.sql;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.query.aggregation.sketch.HdrHistogram.HdrHistogramToQuantilesPostAggregator;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.PostAggregatorVisitor;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import javax.annotation.Nullable;
import java.util.List;

public class HdrHistogramQuantilesOperatorConversion implements SqlOperatorConversion {
    private static final String FUNCTION_NAME = "HDR_GET_QUANTILES";

    private static final SqlFunction SQL_FUNCTION = new SqlFunction(
            FUNCTION_NAME,
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(
                    factory -> Calcites.createSqlType(factory, SqlTypeName.OTHER)
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
        return null;
    }


    @Nullable
    @Override
    public PostAggregator toPostAggregator(
            PlannerContext plannerContext,
            RowSignature rowSignature,
            RexNode rexNode,
            PostAggregatorVisitor postAggregatorVisitor
    )
    {
        final List<RexNode> operands = ((RexCall) rexNode).getOperands();
        final float[] args = new float[operands.size() - 1];

        // 新版本直接就从第一个参数取
        final PostAggregator inputSketchPostAgg = OperatorConversions.toPostAggregator(
                plannerContext,
                rowSignature,
                operands.get(0),
                postAggregatorVisitor,
                true
        );

        if (inputSketchPostAgg == null) {
            return null;
        }

        // 直接解析
        for (int i = 1; i < operands.size(); i++) {
            RexNode operand = operands.get(i);
            float arg = ((Number) RexLiteral.value(operand)).floatValue();
            args[i - 1] = arg;
        }

        return new HdrHistogramToQuantilesPostAggregator(
                postAggregatorVisitor.getOutputNamePrefix() + postAggregatorVisitor.getAndIncrementCounter(),
                ((FieldAccessPostAggregator)inputSketchPostAgg).getFieldName(),
                args
        );
    }


}
