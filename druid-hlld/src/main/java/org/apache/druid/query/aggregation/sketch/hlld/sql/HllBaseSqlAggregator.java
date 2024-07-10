package org.apache.druid.query.aggregation.sketch.hlld.sql;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.sketch.hlld.HllAggregatorFactory;
import org.apache.druid.query.aggregation.sketch.hlld.HllMergeAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public abstract class HllBaseSqlAggregator implements SqlAggregator {

  private final boolean finalizeSketch;

  protected HllBaseSqlAggregator(boolean finalizeSketch){
    this.finalizeSketch = finalizeSketch;
  }

    @Nullable
    @Override
    public Aggregation toDruidAggregation(
            PlannerContext plannerContext,
            RowSignature rowSignature,
            VirtualColumnRegistry virtualColumnRegistry,
            RexBuilder rexBuilder,
            String name,
            AggregateCall aggregateCall,
            Project project,
            List<Aggregation> existingAggregations,
            boolean finalizeAggregations
    ) {
        // Don't use Aggregations.getArgumentsForSimpleAggregator, since it won't let us use direct column access
        // for string columns.
        final RexNode columnRexNode = Expressions.fromFieldAccess(
                rowSignature,
                project,
                aggregateCall.getArgList().get(0)
        );

        final DruidExpression columnArg = Expressions.toDruidExpression(plannerContext, rowSignature, columnRexNode);
        if (columnArg == null) {
            return null;
        }

        // 获取带二个参数precision，Literal类型的参数为常数
        final int precision;
        if (aggregateCall.getArgList().size() >= 2) {
            final RexNode logKarg = Expressions.fromFieldAccess(
                    rowSignature,
                    project,
                    aggregateCall.getArgList().get(1)
            );

            if (!logKarg.isA(SqlKind.LITERAL)) {
                // logK must be a literal in order to plan.
                return null;
            }

            precision = ((Number) RexLiteral.value(logKarg)).intValue();
        } else {
            precision = HllAggregatorFactory.DEFAULT_PRECISION;
        }

        // 获取带三个参数round，Literal类型的参数为常数
        final boolean round;
        if (aggregateCall.getArgList().size() >= 3) {
            final RexNode tgtHllTypeArg = Expressions.fromFieldAccess(
                    rowSignature,
                    project,
                    aggregateCall.getArgList().get(2)
            );

            if (!tgtHllTypeArg.isA(SqlKind.LITERAL)) {
                // tgtHllType must be a literal in order to plan.
                return null;
            }

            round = RexLiteral.booleanValue(tgtHllTypeArg);
        } else {
          round = HllAggregatorFactory.DEFAULT_ROUND;
        }

        // 新版本删除了final List<VirtualColumn> virtualColumns = new ArrayList<>();
        final AggregatorFactory aggregatorFactory;
        //final String aggregatorName = finalizeAggregations ? Calcites.makePrefixedName(name, "a") : name;
        final String aggregatorName = finalizeSketch ? Calcites.makePrefixedName(name, "a") : name;

        // 输入是Hll，返回HllSketchMergeAggregatorFactory
        if (columnArg.isDirectColumnAccess()
                && rowSignature.getColumnType(columnArg.getDirectColumn()).map(type -> type.is(ValueType.COMPLEX)).orElse(false)) {
            // 这就是具体的聚合函数吧
            aggregatorFactory = new HllMergeAggregatorFactory(
                    aggregatorName,
                    columnArg.getDirectColumn(),
                    precision,
                    round
            );
        } else {
            // 输入是regular column，HllBuildAggregatorFactory
            final RelDataType dataType = columnRexNode.getType();
            final ColumnType inputType = Calcites.getColumnTypeForRelDataType(dataType);
            if (inputType == null) {
                throw new ISE("Cannot translate sqlTypeName[%s] to Druid type for field[%s]", dataType.getSqlTypeName(), aggregatorName);
            }

            final DimensionSpec dimensionSpec;

            if (columnArg.isDirectColumnAccess()) {
                dimensionSpec = columnArg.getSimpleExtraction().toDimensionSpec(null, inputType);
            } else {
                String virtualColumnName = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
                        columnArg,
                        dataType
                );
                dimensionSpec = new DefaultDimensionSpec(virtualColumnName, null, inputType);
            }

            // 新版本的判断，输入是Hll
            if (inputType.is(ValueType.COMPLEX)) {
                aggregatorFactory = new HllMergeAggregatorFactory(
                        aggregatorName,
                        dimensionSpec.getOutputName(),
                        precision,
                        round
                );
            } else {
                aggregatorFactory = new HllAggregatorFactory(
                        aggregatorName,
                        dimensionSpec.getDimension(),
                        precision,
                        round
                );
            }
        }

        return toAggregation(
                name,
                finalizeSketch,
                aggregatorFactory
        );
    }

    protected abstract Aggregation toAggregation(
            String name,
            boolean finalizeAggregations,
            AggregatorFactory aggregatorFactory
    );
}
