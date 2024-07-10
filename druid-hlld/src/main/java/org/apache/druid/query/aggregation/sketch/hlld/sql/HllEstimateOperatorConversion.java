package org.apache.druid.query.aggregation.sketch.hlld.sql;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.sketch.hlld.HllAggregatorFactory;
import org.apache.druid.query.aggregation.sketch.hlld.HllToEstimatePostAggregator;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.*;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import javax.annotation.Nullable;
import java.util.List;

// postAggregator, toDruidExpression返回null。相当于post udf和普通udf是不一样的。
// 新版本直接修改了父类
public class HllEstimateOperatorConversion implements SqlOperatorConversion {
    private static final String FUNCTION_NAME = "HLLD_ESTIMATE";
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
            .operatorBuilder(StringUtils.toUpperCase(FUNCTION_NAME))
            .operandTypes(SqlTypeFamily.ANY, SqlTypeFamily.BOOLEAN)
            .requiredOperands(1)
            //.returnTypeInference(ReturnTypes.BIGINT)
            .returnTypeInference(ReturnTypes.DOUBLE)
            .build();

    // 新版本少了构造函数

    @Override
    public SqlOperator calciteOperator() {
        return SQL_FUNCTION;
    }

    @Override
    public DruidExpression toDruidExpression(
            PlannerContext plannerContext,
            RowSignature rowSignature,
            RexNode rexNode
    ) {
        return null;
    }

    @Nullable
    @Override
    public PostAggregator toPostAggregator(
            PlannerContext plannerContext,
            RowSignature rowSignature,
            RexNode rexNode,
            PostAggregatorVisitor postAggregatorVisitor
    ) {
        final List<RexNode> operands = ((RexCall) rexNode).getOperands();
        final PostAggregator firstOperand = OperatorConversions.toPostAggregator(
                plannerContext,
                rowSignature,
                operands.get(0),
                postAggregatorVisitor,
                true // 新版本多了个参数
        );

        if (firstOperand == null) {
            return null;
        }

        boolean round = HllAggregatorFactory.DEFAULT_ROUND;
        if (operands.size() == 2) {
            if (!operands.get(1).isA(SqlKind.LITERAL)) {
                return null;
            }

            round = RexLiteral.booleanValue(operands.get(1));
        }

        return new HllToEstimatePostAggregator(
                postAggregatorVisitor.getOutputNamePrefix() + postAggregatorVisitor.getAndIncrementCounter(),
                firstOperand,
                round
        );
    }
}
