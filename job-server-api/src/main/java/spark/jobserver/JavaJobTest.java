package spark.jobserver;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.scalactic.Every;
import org.scalactic.Good;
import org.scalactic.Or;
import spark.jobserver.api.JobEnvironment;
import spark.jobserver.api.ValidationProblem;
import spark.jobserver.api.JSparkJob;

public class JavaJobTest implements JSparkJob<Integer> {

    @Override
    public Integer runJob(ContextLike context, JobEnvironment cfg, Config data) {
        return 0;
    }

    @Override
    public Or<Config, Every<ValidationProblem>> validate(ContextLike context, JobEnvironment jEnv, Config cfg) {
        return new Good<>(ConfigFactory.empty());
    }
}
