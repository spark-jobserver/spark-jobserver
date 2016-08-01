package spark.jobserver;

import com.typesafe.config.Config;
import org.apache.spark.SparkContext;
import org.scalactic.Every;
import org.scalactic.Or;
import spark.jobserver.api.JobEnvironment;
import spark.jobserver.api.ValidationProblem;

public interface JavaSparkJob<R, D> {
    R runJob(SparkContext context, JobEnvironment cfg, D data);
    Or<D, Every<ValidationProblem>> validate(SparkContext context, JobEnvironment jEnv, Config cfg);
}
