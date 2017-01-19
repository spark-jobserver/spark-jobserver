package spark.jobserver.api;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.api.java.JavaSparkContext;
import spark.jobserver.japi.JSparkJob;

public class TestJob implements JSparkJob<Integer> {

    public Integer run(JavaSparkContext sc, JobEnvironment runtime, Config data) {
        return 0;
    }

    public Config verify(JavaSparkContext sc, JobEnvironment runtime, Config config) {
        return ConfigFactory.empty();
    }
}
