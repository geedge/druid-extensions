package org.apache.druid.query.udf.expressions;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.math.expr.*;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Test;

import java.util.Collections;

public class DimensionBucketExprTest extends InitializedNullHandlingTest {
    private final ExprMacroTable exprMacroTable = new ExprMacroTable(Collections.singletonList(new DimensionBucketExprMacro()));
    Expr.ObjectBinding inputBindings = InputBindings.forInputSuppliers(
            new ImmutableMap.Builder<String, InputBindings.InputSupplier>()
                    .put("string", InputBindings.inputSupplier(ExpressionType.STRING, () -> "abcdef"))
                    .put("long", InputBindings.inputSupplier(ExpressionType.LONG, () -> 1234L))
                    .put("double", InputBindings.inputSupplier(ExpressionType.DOUBLE, () -> 1.234))
                    .put("array1", InputBindings.inputSupplier(ExpressionType.STRING_ARRAY, () -> new Object[]{"1", "2", "3"}))
                    .put("array2", InputBindings.inputSupplier(ExpressionType.STRING_ARRAY, () -> new String[]{"1", "2", "3"}))
                    .put("nullString", InputBindings.inputSupplier(ExpressionType.STRING, () -> null))
                    .put("nullLong", InputBindings.inputSupplier(ExpressionType.LONG, () -> null))
                    .put("nullDouble", InputBindings.inputSupplier(ExpressionType.DOUBLE, () -> null))
                    .build()
    );

    Expr.ObjectBinding[] inputBindingArray = new Expr.ObjectBinding[]{
            InputBindings.forInputSuppliers(
                    new ImmutableMap.Builder<String, InputBindings.InputSupplier>()
                            .put("device_id", InputBindings.inputSupplier(ExpressionType.STRING, () -> "1"))
                            .put("rule_id", InputBindings.inputSupplier(ExpressionType.LONG, () -> 81))
                            .put("template_id", InputBindings.inputSupplier(ExpressionType.LONG, () -> 81))
                            .put("chart_id", InputBindings.inputSupplier(ExpressionType.LONG, () -> 81))
                            .put("version", InputBindings.inputSupplier(ExpressionType.LONG, () -> 1))
                            .put("client_ip_object", InputBindings.inputSupplier(ExpressionType.STRING_ARRAY, () -> null))
                            .put("server_ip_object", InputBindings.inputSupplier(ExpressionType.STRING_ARRAY, () -> null))
                            .put("fqdn_category", InputBindings.inputSupplier(ExpressionType.STRING_ARRAY, () -> null))
                            .put("client_ip", InputBindings.inputSupplier(ExpressionType.STRING, () -> null))
                            .put("server_ip", InputBindings.inputSupplier(ExpressionType.STRING, () -> null))
                            .put("server_fqdn", InputBindings.inputSupplier(ExpressionType.STRING, () -> null))
                            .put("server_domain", InputBindings.inputSupplier(ExpressionType.STRING, () -> null))
                            .put("application", InputBindings.inputSupplier(ExpressionType.STRING, () -> null))
                            .build()
            ),
            InputBindings.forInputSuppliers(
                    new ImmutableMap.Builder<String, InputBindings.InputSupplier>()
                            .put("device_id", InputBindings.inputSupplier(ExpressionType.STRING, () -> "1"))
                            .put("rule_id", InputBindings.inputSupplier(ExpressionType.LONG, () -> 101))
                            .put("template_id", InputBindings.inputSupplier(ExpressionType.LONG, () -> 101))
                            .put("chart_id", InputBindings.inputSupplier(ExpressionType.LONG, () -> 101))
                            .put("version", InputBindings.inputSupplier(ExpressionType.LONG, () -> 1))
                            .put("client_ip_object", InputBindings.inputSupplier(ExpressionType.STRING_ARRAY, () -> new Object[]{"5","7","8"}))
                            .put("server_ip_object", InputBindings.inputSupplier(ExpressionType.STRING_ARRAY, () -> null))
                            .put("fqdn_category", InputBindings.inputSupplier(ExpressionType.STRING_ARRAY, () -> null))
                            .put("client_ip", InputBindings.inputSupplier(ExpressionType.STRING, () -> null))
                            .put("server_ip", InputBindings.inputSupplier(ExpressionType.STRING, () -> null))
                            .put("server_fqdn", InputBindings.inputSupplier(ExpressionType.STRING, () -> null))
                            .put("server_domain", InputBindings.inputSupplier(ExpressionType.STRING, () -> null))
                            .put("application", InputBindings.inputSupplier(ExpressionType.STRING, () -> null))
                            .build()
            ),
            InputBindings.forInputSuppliers(
                    new ImmutableMap.Builder<String, InputBindings.InputSupplier>()
                            .put("device_id", InputBindings.inputSupplier(ExpressionType.STRING, () -> "1"))
                            .put("rule_id", InputBindings.inputSupplier(ExpressionType.LONG, () -> 271L))
                            .put("template_id", InputBindings.inputSupplier(ExpressionType.LONG, () -> 271L))
                            .put("chart_id", InputBindings.inputSupplier(ExpressionType.LONG, () -> 271L))
                            .put("version", InputBindings.inputSupplier(ExpressionType.LONG, () -> 1L))
                            .put("client_ip_object", InputBindings.inputSupplier(ExpressionType.STRING_ARRAY, () -> null))
                            .put("server_ip_object", InputBindings.inputSupplier(ExpressionType.STRING_ARRAY, () -> null))
                            .put("fqdn_category", InputBindings.inputSupplier(ExpressionType.STRING_ARRAY, () -> null))
                            .put("client_ip", InputBindings.inputSupplier(ExpressionType.STRING, () -> null))
                            .put("server_ip", InputBindings.inputSupplier(ExpressionType.STRING, () -> "5.245.228.51"))
                            .put("server_fqdn", InputBindings.inputSupplier(ExpressionType.STRING, () -> null))
                            .put("server_domain", InputBindings.inputSupplier(ExpressionType.STRING, () -> null))
                            .put("application", InputBindings.inputSupplier(ExpressionType.STRING, () -> null))
                            .build()
            ),
            // ...
            InputBindings.forInputSuppliers(
                    new ImmutableMap.Builder<String, InputBindings.InputSupplier>()
                            .put("device_id", InputBindings.inputSupplier(ExpressionType.STRING, () -> "1"))
                            .put("rule_id", InputBindings.inputSupplier(ExpressionType.LONG, () -> 81))
                            .put("template_id", InputBindings.inputSupplier(ExpressionType.LONG, () -> 81))
                            .put("chart_id", InputBindings.inputSupplier(ExpressionType.LONG, () -> 81))
                            .put("version", InputBindings.inputSupplier(ExpressionType.LONG, () -> 1))
                            .put("client_ip_object", InputBindings.inputSupplier(ExpressionType.STRING, () -> null))
                            .put("server_ip_object", InputBindings.inputSupplier(ExpressionType.STRING, () -> null))
                            .put("fqdn_category", InputBindings.inputSupplier(ExpressionType.STRING, () -> null))
                            .put("client_ip", InputBindings.inputSupplier(ExpressionType.STRING, () -> null))
                            .put("server_ip", InputBindings.inputSupplier(ExpressionType.STRING, () -> null))
                            .put("server_fqdn", InputBindings.inputSupplier(ExpressionType.STRING, () -> null))
                            .put("server_domain", InputBindings.inputSupplier(ExpressionType.STRING, () -> null))
                            .put("application", InputBindings.inputSupplier(ExpressionType.STRING, () -> null))
                            .build()
            ),
            InputBindings.forInputSuppliers(
                    new ImmutableMap.Builder<String, InputBindings.InputSupplier>()
                            .put("device_id", InputBindings.inputSupplier(ExpressionType.STRING, () -> "1"))
                            .put("rule_id", InputBindings.inputSupplier(ExpressionType.LONG, () -> 101))
                            .put("template_id", InputBindings.inputSupplier(ExpressionType.LONG, () -> 101))
                            .put("chart_id", InputBindings.inputSupplier(ExpressionType.LONG, () -> 101))
                            .put("version", InputBindings.inputSupplier(ExpressionType.LONG, () -> 1))
                            .put("client_ip_object", InputBindings.inputSupplier(ExpressionType.STRING, () -> "5,7,8"))
                            .put("server_ip_object", InputBindings.inputSupplier(ExpressionType.STRING, () -> null))
                            .put("fqdn_category", InputBindings.inputSupplier(ExpressionType.STRING, () -> null))
                            .put("client_ip", InputBindings.inputSupplier(ExpressionType.STRING, () -> null))
                            .put("server_ip", InputBindings.inputSupplier(ExpressionType.STRING, () -> null))
                            .put("server_fqdn", InputBindings.inputSupplier(ExpressionType.STRING, () -> null))
                            .put("server_domain", InputBindings.inputSupplier(ExpressionType.STRING, () -> null))
                            .put("application", InputBindings.inputSupplier(ExpressionType.STRING, () -> null))
                            .build()
            ),
            InputBindings.forInputSuppliers(
                    new ImmutableMap.Builder<String, InputBindings.InputSupplier>()
                            .put("device_id", InputBindings.inputSupplier(ExpressionType.STRING, () -> "1"))
                            .put("rule_id", InputBindings.inputSupplier(ExpressionType.LONG, () -> 271L))
                            .put("template_id", InputBindings.inputSupplier(ExpressionType.LONG, () -> 271L))
                            .put("chart_id", InputBindings.inputSupplier(ExpressionType.LONG, () -> 271L))
                            .put("version", InputBindings.inputSupplier(ExpressionType.LONG, () -> 1L))
                            .put("client_ip_object", InputBindings.inputSupplier(ExpressionType.STRING, () -> null))
                            .put("server_ip_object", InputBindings.inputSupplier(ExpressionType.STRING, () -> null))
                            .put("fqdn_category", InputBindings.inputSupplier(ExpressionType.STRING, () -> null))
                            .put("client_ip", InputBindings.inputSupplier(ExpressionType.STRING, () -> null))
                            .put("server_ip", InputBindings.inputSupplier(ExpressionType.STRING, () -> "5.245.228.51"))
                            .put("server_fqdn", InputBindings.inputSupplier(ExpressionType.STRING, () -> null))
                            .put("server_domain", InputBindings.inputSupplier(ExpressionType.STRING, () -> null))
                            .put("application", InputBindings.inputSupplier(ExpressionType.STRING, () -> null))
                            .build()
            ),
    };

    @Test
    public void test() {
        Expr expr = Parser.parse("dimension_bucket(1024, 100, 'aaa', string,long,double,array1, array2, nullString, nullLong)", exprMacroTable);
        ExprEval eval = expr.eval(inputBindings);
        System.out.println(eval.value());
    }

    @Test
    public void test2() {
        for (Expr.ObjectBinding objectBinding : inputBindingArray) {
            Expr expr = Parser.parse("dimension_bucket(1024, device_id, rule_id, template_id, chart_id, version, client_ip_object, server_ip_object, fqdn_category, client_ip, server_ip, server_fqdn, server_domain, application)", exprMacroTable);
            ExprEval eval = expr.eval(objectBinding);
            System.out.println(objectBinding.get("rule_id") + ", bucket_id:" + eval.value());
        }
    }
}
