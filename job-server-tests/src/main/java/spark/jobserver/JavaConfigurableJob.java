package spark.jobserver;

import com.typesafe.config.Config;
import org.apache.spark.api.java.JavaSparkContext;
import spark.jobserver.api.JobEnvironment;
import spark.jobserver.japi.JavaSparkJob;

public class JavaConfigurableJob implements JavaSparkJob<String, String> {

    public String run(JavaSparkContext sc, JobEnvironment runtime, String data) {
        return data;
    }

    public String verify(JavaSparkContext sc, JobEnvironment runtime, Config config) throws RuntimeException {
        String input = config.getString("input");
        if (input.length() == 0) {
            throw new IllegalArgumentException("You passed an empty string");
        }
        return input;
    }
}
