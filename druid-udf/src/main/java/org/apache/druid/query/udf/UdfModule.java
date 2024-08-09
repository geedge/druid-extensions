package org.apache.druid.query.udf;

import com.google.inject.Binder;
import org.apache.druid.guice.ExpressionModule;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.query.udf.expressions.DimensionBucketExprMacro;
import org.apache.druid.query.udf.sql.DimensionBucketOperatorConversion;
import org.apache.druid.sql.guice.SqlBindings;

public class UdfModule implements DruidModule {
    @Override
    public void configure(Binder binder) {
        SqlBindings.addOperatorConversion(binder, DimensionBucketOperatorConversion.class);
        ExpressionModule.addExprMacro(binder, DimensionBucketExprMacro.class);
    }

    /*@Override
    public List<? extends Module> getJacksonModules() {
        // Register Jackson module for any classes we need to be able to use in JSON queries or ingestion specs.
        return Collections.<Module>singletonList(new SimpleModule("UdfModule"));
    }*/

}
