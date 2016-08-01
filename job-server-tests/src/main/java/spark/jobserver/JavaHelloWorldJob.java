package spark.jobserver;

import com.typesafe.config.Config;
import org.apache.spark.SparkContext;
import org.scalactic.Every;
import org.scalactic.Good;
import org.scalactic.Or;
import spark.jobserver.api.JobEnvironment;
import spark.jobserver.api.ValidationProblem;

public class JavaHelloWorldJob implements JavaSparkJob<String, String>{

    @Override
    public String runJob(SparkContext context, JobEnvironment cfg, String data) {
        return data;
    }

    @Override
    public Or<String, Every<ValidationProblem>> validate(SparkContext context, JobEnvironment jEnv, Config cfg) {
        return new Good<>("k");
    }
}
