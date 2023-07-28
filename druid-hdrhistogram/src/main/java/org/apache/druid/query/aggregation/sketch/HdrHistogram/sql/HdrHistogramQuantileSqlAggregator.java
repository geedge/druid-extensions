package org.apache.druid.query.aggregation.sketch.HdrHistogram.sql;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.*;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.sketch.HdrHistogram.HdrHistogramAggregatorFactory;
import org.apache.druid.query.aggregation.sketch.HdrHistogram.HdrHistogramMergeAggregatorFactory;
import org.apache.druid.query.aggregation.sketch.HdrHistogram.HdrHistogramToQuantilePostAggregator;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

import static org.apache.druid.query.aggregation.sketch.HdrHistogram.HdrHistogramAggregatorFactory.*;
import static org.apache.druid.query.aggregation.sketch.HdrHistogram.HdrHistogramAggregatorFactory.DEFAULT_AUTO_RESIZE;

public class HdrHistogramQuantileSqlAggregator implements SqlAggregator {
    private static final SqlAggFunction FUNCTION_INSTANCE = new HdrHistogramQuantileSqlAggFunction();
    private static final String NAME = "APPROX_QUANTILE_HDR";

    @Override
    public SqlAggFunction calciteFunction() {
        return FUNCTION_INSTANCE;
    }

    @Nullable
    @Override
    public Aggregation toDruidAggregation(PlannerContext plannerContext, RowSignature rowSignature, VirtualColumnRegistry virtualColumnRegistry, RexBuilder rexBuilder, String name, AggregateCall aggregateCall, Project project, List<Aggregation> existingAggregations, boolean finalizeAggregations) {
        final DruidExpression input = Expressions.toDruidExpression(
                plannerContext,
                rowSignature,
                Expressions.fromFieldAccess(
                        rowSignature,
                        project,
                        aggregateCall.getArgList().get(0)
                )
        );
        if (input == null) {
            return null;
        }

        final AggregatorFactory aggregatorFactory;
        final String histogramName = StringUtils.format("%s:agg", name);

        final RexNode probabilityArg = Expressions.fromFieldAccess(
                rowSignature,
                project,
                aggregateCall.getArgList().get(1)
        );
        if (!probabilityArg.isA(SqlKind.LITERAL)) {
            // Probability must be a literal in order to plan.
            return null;
        }
        final float probability = ((Number) RexLiteral.value(probabilityArg)).floatValue();

        long lowestDiscernibleValue = DEFAULT_LOWEST;
        long highestTrackableValue = DEFAULT_HIGHEST;
        int numberOfSignificantValueDigits = DEFAULT_SIGNIFICANT;
        boolean autoResize = DEFAULT_AUTO_RESIZE;

        if(aggregateCall.getArgList().size() == 3) {
            final RexNode numberOfSignificantValueDigitsArg = Expressions.fromFieldAccess(
                    rowSignature,
                    project,
                    aggregateCall.getArgList().get(2)
            );
            if (!numberOfSignificantValueDigitsArg.isA(SqlKind.LITERAL)) {
                return null;
            }
            numberOfSignificantValueDigits = ((Number) RexLiteral.value(numberOfSignificantValueDigitsArg)).intValue();
        }else if (aggregateCall.getArgList().size() > 3){
            final RexNode lowestDiscernibleValueArg = Expressions.fromFieldAccess(
                    rowSignature,
                    project,
                    aggregateCall.getArgList().get(2)
            );
            if (!lowestDiscernibleValueArg.isA(SqlKind.LITERAL)) {
                return null;
            }
            lowestDiscernibleValue = ((Number) RexLiteral.value(lowestDiscernibleValueArg)).longValue();

            final RexNode highestTrackableValueArg = Expressions.fromFieldAccess(
                    rowSignature,
                    project,
                    aggregateCall.getArgList().get(3)
            );
            if (!highestTrackableValueArg.isA(SqlKind.LITERAL)) {
                return null;
            }
            highestTrackableValue = ((Number) RexLiteral.value(highestTrackableValueArg)).longValue();

            final RexNode numberOfSignificantValueDigitsArg = Expressions.fromFieldAccess(
                    rowSignature,
                    project,
                    aggregateCall.getArgList().get(4)
            );
            if (!numberOfSignificantValueDigitsArg.isA(SqlKind.LITERAL)) {
                return null;
            }
            numberOfSignificantValueDigits = ((Number) RexLiteral.value(numberOfSignificantValueDigitsArg)).intValue();

            if (aggregateCall.getArgList().size() >= 6) {
                final RexNode autoResizeArg = Expressions.fromFieldAccess(
                        rowSignature,
                        project,
                        aggregateCall.getArgList().get(5)
                );
                if (!autoResizeArg.isA(SqlKind.LITERAL)) {
                    return null;
                }
                autoResize = RexLiteral.booleanValue(autoResizeArg);
            }else{
                autoResize = DEFAULT_AUTO_RESIZE;
            }
        }

        // Look for existing matching aggregatorFactory.
        for (final Aggregation existing : existingAggregations) {
            for (AggregatorFactory factory : existing.getAggregatorFactories()) {
                if (factory instanceof HdrHistogramAggregatorFactory) {
                    final HdrHistogramAggregatorFactory theFactory = (HdrHistogramAggregatorFactory) factory;

                    // Check input for equivalence.
                    final boolean inputMatches;
                    final VirtualColumn virtualInput = existing.getVirtualColumns()
                            .stream()
                            .filter(
                                    virtualColumn ->
                                            virtualColumn.getOutputName()
                                                    .equals(theFactory.getFieldName())
                            )
                            .findFirst()
                            .orElse(null);

                    if (virtualInput == null) {
                        inputMatches = input.isDirectColumnAccess()
                                && input.getDirectColumn().equals(theFactory.getFieldName());
                    } else {
                        inputMatches = ((ExpressionVirtualColumn) virtualInput).getExpression()
                                .equals(input.getExpression());
                    }

                    final boolean matches = inputMatches
                            && theFactory.getLowestDiscernibleValue() == lowestDiscernibleValue
                            && theFactory.getHighestTrackableValue() == highestTrackableValue
                            && theFactory.getNumberOfSignificantValueDigits() == numberOfSignificantValueDigits
                            && theFactory.isAutoResize() == autoResize;

                    if (matches) {
                        // Found existing one. Use this.
                        return Aggregation.create(
                                ImmutableList.of(),
                                new HdrHistogramToQuantilePostAggregator(name, factory.getName(), probability)
                        );
                    }
                }
            }
        }

        // No existing match found. Create a new one.
        final List<VirtualColumn> virtualColumns = new ArrayList<>();

        if (input.isDirectColumnAccess()) {
            // 参数是Histogram对象
            if (rowSignature.getColumnType(input.getDirectColumn()).orElse(null) == ValueType.COMPLEX) {
                aggregatorFactory = new HdrHistogramMergeAggregatorFactory(
                        histogramName,
                        input.getDirectColumn(),
                        lowestDiscernibleValue,
                        highestTrackableValue,
                        numberOfSignificantValueDigits,
                        autoResize
                );
            } else {
                aggregatorFactory = new HdrHistogramAggregatorFactory(
                        histogramName,
                        input.getDirectColumn(),
                        lowestDiscernibleValue,
                        highestTrackableValue,
                        numberOfSignificantValueDigits,
                        autoResize
                );
            }
        } else {
            final VirtualColumn virtualColumn =
                    virtualColumnRegistry.getOrCreateVirtualColumnForExpression(plannerContext, input, SqlTypeName.BIGINT);
            virtualColumns.add(virtualColumn);
            aggregatorFactory = new HdrHistogramAggregatorFactory(
                    histogramName,
                    virtualColumn.getOutputName(),
                    lowestDiscernibleValue,
                    highestTrackableValue,
                    numberOfSignificantValueDigits,
                    autoResize
            );
            /*if (rowSignature.getColumnType(input.getDirectColumn()).orElse(null) == ValueType.COMPLEX) {
                aggregatorFactory = new HdrHistogramMergeAggregatorFactory(
                        histogramName,
                        virtualColumn.getOutputName(),
                        lowestDiscernibleValue,
                        highestTrackableValue,
                        numberOfSignificantValueDigits,
                        autoResize
                );
            } else {
                aggregatorFactory = new HdrHistogramAggregatorFactory(
                        histogramName,
                        virtualColumn.getOutputName(),
                        lowestDiscernibleValue,
                        highestTrackableValue,
                        numberOfSignificantValueDigits,
                        autoResize
                );
            }*/
        }

        return Aggregation.create(
                virtualColumns,
                ImmutableList.of(aggregatorFactory),
                new HdrHistogramToQuantilePostAggregator(name, histogramName, probability)
        );
    }

    private static class HdrHistogramQuantileSqlAggFunction extends SqlAggFunction {
        private static final String SIGNATURE1 = "'" + NAME + "(column, probability)'\n";
        private static final String SIGNATURE2 = "'" + NAME + "(column, probability, numberOfSignificantValueDigits)'\n";
        private static final String SIGNATURE3 = "'" + NAME + "(column, probability, lowestDiscernibleValue, highestTrackableValue, numberOfSignificantValueDigits)'\n";
        private static final String SIGNATURE4 = "'" + NAME + "(column, probability, lowestDiscernibleValue, highestTrackableValue, numberOfSignificantValueDigits, autoResize)'\n";

        HdrHistogramQuantileSqlAggFunction() {
            super(
                    NAME,
                    null,
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.explicit(SqlTypeName.BIGINT),
                    null,
                    OperandTypes.or(
                            OperandTypes.and(
                                    OperandTypes.sequence(SIGNATURE1, OperandTypes.ANY, OperandTypes.LITERAL),
                                    OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.NUMERIC)
                            ),
                            OperandTypes.and(
                                    OperandTypes.sequence(SIGNATURE2, OperandTypes.ANY, OperandTypes.LITERAL, OperandTypes.LITERAL),
                                    OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC)
                            ),
                            OperandTypes.and(
                                    OperandTypes.sequence(SIGNATURE3, OperandTypes.ANY, OperandTypes.LITERAL, OperandTypes.LITERAL, OperandTypes.LITERAL, OperandTypes.LITERAL),
                                    OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC)
                            ),
                            OperandTypes.and(
                                    OperandTypes.sequence(SIGNATURE4, OperandTypes.ANY, OperandTypes.LITERAL, OperandTypes.LITERAL, OperandTypes.LITERAL, OperandTypes.LITERAL, OperandTypes.LITERAL),
                                    OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.BOOLEAN)
                            )
                    ),
                    SqlFunctionCategory.NUMERIC,
                    false,
                    false
            );
        }
    }
}
