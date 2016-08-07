package spark.jobserver.context;

import org.apache.spark.streaming.StreamingContext;
import spark.jobserver.ContextLike;
import spark.jobserver.api.JSparkJob;

public abstract class JSparkStreamingJob<R> extends JSparkJob<StreamingContext, R> {
    @Override
    protected StreamingContext findContext(ContextLike c) {
        return (StreamingContext) c;
    }
}