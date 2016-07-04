package spark.jobserver;

import com.typesafe.config.Config;
import org.apache.spark.api.java.JavaSparkContext;
import org.scalactic.Every;
import org.scalactic.Good;
import org.scalactic.Or;
import spark.jobserver.api.JobEnvironment;
import spark.jobserver.api.ValidationProblem;

public class JavaJobTest implements JavaSparkJob<JavaSparkContext, Integer, Integer> {

    @Override
    public Integer runJob(JavaSparkContext context, JobEnvironment cfg, Integer data) {
        return 0;
    }

    @Override
    public Or<Integer, Every<ValidationProblem>> validate(JavaSparkContext context, JobEnvironment jEnv, Config cfg) {
        return new Good<>(1);
    }
}
