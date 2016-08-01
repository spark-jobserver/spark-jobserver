package spark.jobserver;

import com.typesafe.config.Config;
import org.apache.spark.SparkContext;
import org.scalactic.Every;
import org.scalactic.Good;
import org.scalactic.Or;
import spark.jobserver.api.JobEnvironment;
import spark.jobserver.api.ValidationProblem;

public class JavaJobTest implements JavaSparkJob<Integer, Integer> {

    @Override
    public Integer runJob(SparkContext context, JobEnvironment cfg, Integer data) {
        return 0;
    }

    @Override
    public Or<Integer, Every<ValidationProblem>> validate(SparkContext context, JobEnvironment jEnv, Config cfg) {
        return new Good<>(1);
    }
}
