package spark.jobserver;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.api.java.JavaSparkContext;
import spark.jobserver.japi.JSparkJob;
import spark.jobserver.api.JobEnvironment;

public class JavaHelloWorldJob implements JSparkJob<String> {

    public String run(JavaSparkContext sc, JobEnvironment runtime, Config data) {
        return "Hi!";
    }

    public Config verify(JavaSparkContext sc, JobEnvironment runtime, Config config) {
        return ConfigFactory.empty();
    }
}
