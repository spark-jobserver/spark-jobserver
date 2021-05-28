package spark.jobserver;

import com.typesafe.config.Config;
import org.apache.spark.api.java.JavaSparkContext;
import spark.jobserver.api.JobEnvironment;
import spark.jobserver.japi.JSparkJob;
import spark.jobserver.japi.JavaValidationException;

public class FailingJavaJob implements JSparkJob<Integer> {
    @Override
    public Integer run(JavaSparkContext sc, JobEnvironment runtime, Config data) {
        return null;
    }

    @Override
    public Config verify(JavaSparkContext sc, JobEnvironment runtime, Config config) throws JavaValidationException {
        throw new RuntimeException("fail");
    }
}
