package spark.jobserver.api;

import com.typesafe.config.Config;
import org.apache.spark.api.java.JavaSparkContext;
import org.scalactic.Every;
import org.scalactic.Or;
import spark.jobserver.ContextLike;

public interface JSparkJob<R> {
    default JavaSparkContext javaSparkContext(ContextLike c){
        return JavaSparkContext.fromSparkContext(c.sparkContext());
    }
    R runJob(ContextLike context, JobEnvironment cfg, Config data);
    Or<Config, Every<ValidationProblem>> validate(ContextLike context, JobEnvironment jEnv, Config cfg);
}