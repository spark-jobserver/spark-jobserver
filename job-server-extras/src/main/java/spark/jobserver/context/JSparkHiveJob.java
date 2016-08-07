package spark.jobserver.context;

import org.apache.spark.sql.hive.HiveContext;
import spark.jobserver.ContextLike;
import spark.jobserver.api.JSparkJob;

public abstract class JSparkHiveJob<R> extends JSparkJob<HiveContext, R> {
    @Override
    protected HiveContext findContext(ContextLike c) {
        return (HiveContext) c;
    }
}
