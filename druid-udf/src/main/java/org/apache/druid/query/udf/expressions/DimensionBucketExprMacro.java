package org.apache.druid.query.udf.expressions;

import org.apache.druid.math.expr.*;
import org.apache.druid.math.expr.ExprMacroTable.ExprMacro;

import javax.annotation.Nullable;
import java.util.List;
import java.util.stream.Collectors;

public class DimensionBucketExprMacro implements ExprMacro {
    private static final String NAME = "dimension_bucket";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Expr apply(List<Expr> args) {
        validationHelperCheckMinArgumentCount(args, 2);
        Expr bucketCnt = args.get(0);
        if(!bucketCnt.isLiteral()|| bucketCnt.eval(InputBindings.nilBindings()).asInt() <= 0) {
            throw validationFailed("first bucketCount argument must is int literal and > 0");
        }
        return new DimensionBucketExpr(args);
    }

    static class DimensionBucketExpr extends ExprMacroTable.BaseScalarMacroFunctionExpr {
        private final int bucketCount;

        public DimensionBucketExpr(List<Expr> args) {
            super(NAME, args);
            bucketCount = args.get(0).eval(InputBindings.nilBindings()).asInt();
        }

        @Override
        public ExprEval eval(ObjectBinding bindings) {
            int result = 1;
            for (int i = 1; i < args.size(); i++) {
                ExprEval eval = args.get(i).eval(bindings);
                Object element = eval.value();
                if(element instanceof Object[]){
                    for (Object ele : (Object[]) element) {
                        result = 31 * result + (ele == null ? 0 : ele.hashCode());
                    }
                }else{
                    result = 31 * result + (element == null ? 0 : element.hashCode());
                }

                /*else if (element instanceof Number) {
                    //result = 31 * result + Integer.hashCode(((Number)element).intValue());
                    result = 31 * result + Long.hashCode(((Number)element).longValue());
                }*/
            }

            int bucket = Math.abs(result) % bucketCount;
            return ExprEval.of(IntToHexUtil.uInt16ToHexStringFast(bucket));
        }

        @Override
        public Expr visit(Shuttle shuttle) {
            List<Expr> newArgs = args.stream().map(x -> x.visit(shuttle)).collect(Collectors.toList());
            return shuttle.visit(new DimensionBucketExpr(newArgs));
        }

        @Override
        public BindingAnalysis analyzeInputs() {
            return super.analyzeInputs();
        }

        @Nullable
        @Override
        public ExpressionType getOutputType(InputBindingInspector inspector) {
            return ExpressionType.STRING;
        }

        @Override
        public boolean canVectorize(InputBindingInspector inspector) {
            return false;
        }
    }
}
