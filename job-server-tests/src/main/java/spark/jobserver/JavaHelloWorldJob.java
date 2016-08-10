package spark.jobserver;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.api.java.JavaSparkContext;
import spark.jobserver.api.JobEnvironment;
import spark.jobserver.api.JobValidation;
import spark.jobserver.api.JSparkContextJob;

public class JavaHelloWorldJob extends JSparkContextJob<String> {

    @Override
    public String runJob(JavaSparkContext context, JobEnvironment jEnv, Config data) {
        return data.getString("data");
    }

    @Override
    public JobValidation validate(JavaSparkContext context, JobEnvironment jEnv, Config cfg) {
        return new JobValidation.JOB_VALID(ConfigFactory.parseString("data = hello"));
    }
}
