package org.apache.druid.query.aggregation.sketch.HdrHistogram.sql;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.aggregation.sketch.HdrHistogram.HdrHistogramToDescribePostAggregator;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.PostAggregatorVisitor;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import javax.annotation.Nullable;
import java.util.List;

public class HdrHistogramDescribeOperatorConversion implements SqlOperatorConversion {
    private static final String FUNCTION_NAME = "HDR_DESCRIBE";
    private static final SqlFunction SQL_FUNCTION = OperatorConversions
            .operatorBuilder(StringUtils.toUpperCase(FUNCTION_NAME))
            .operandTypes(SqlTypeFamily.ANY)
            .requiredOperands(1)
            .returnTypeInference(ReturnTypes.explicit(SqlTypeName.VARCHAR))
            .build();

    @Override
    public SqlOperator calciteOperator()
    {
        return SQL_FUNCTION;
    }

    @Override
    public DruidExpression toDruidExpression(
            PlannerContext plannerContext,
            RowSignature rowSignature,
            RexNode rexNode
    )
    {
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
        final PostAggregator postAgg = OperatorConversions.toPostAggregator(
                plannerContext,
                rowSignature,
                operands.get(0),
                postAggregatorVisitor,
                true
        );

        if (postAgg == null) {
            return null;
        }


        return new HdrHistogramToDescribePostAggregator(
                postAggregatorVisitor.getOutputNamePrefix() + postAggregatorVisitor.getAndIncrementCounter(),
                ((FieldAccessPostAggregator)postAgg).getFieldName()
        );
    }
}
