package spark.jobserver;

import com.typesafe.config.Config;
import org.scalactic.Every;
import org.scalactic.Or;
import spark.jobserver.api.JobEnvironment;
import spark.jobserver.api.ValidationProblem;

public interface JavaSparkJob<C, R, D> {
    R runJob(C context, JobEnvironment cfg, D data);
    Or<D, Every<ValidationProblem>> validate(C context, JobEnvironment jEnv, Config cfg);
}
