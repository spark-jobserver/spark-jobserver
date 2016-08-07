package spark.jobserver.context;

import org.apache.spark.sql.SQLContext;
import spark.jobserver.ContextLike;
import spark.jobserver.api.JSparkJob;

public abstract class JSparkSqlJob<R> extends JSparkJob<SQLContext, R> {
    @Override
    protected SQLContext findContext(ContextLike c) {
        return (SQLContext) c;
    }
}
