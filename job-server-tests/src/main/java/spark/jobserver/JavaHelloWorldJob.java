package spark.jobserver;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.scalactic.Every;
import org.scalactic.Good;
import org.scalactic.Or;
import spark.jobserver.api.JobEnvironment;
import spark.jobserver.api.ValidationProblem;
import spark.jobserver.api.JSparkJob;

public class JavaHelloWorldJob implements JSparkJob<String> {

    @Override
    public String runJob(ContextLike context, JobEnvironment cfg, Config data) {
        return data.getString("data");
    }

    @Override
    public Or<Config, Every<ValidationProblem>> validate(ContextLike context, JobEnvironment jEnv, Config cfg) {
        return new Good<>(ConfigFactory.parseString("data = s"));
    }
}
