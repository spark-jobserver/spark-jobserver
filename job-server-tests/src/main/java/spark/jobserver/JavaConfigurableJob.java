package spark.jobserver;

import com.typesafe.config.Config;
import org.apache.spark.api.java.JavaSparkContext;
import spark.jobserver.api.JobEnvironment;
import spark.jobserver.japi.JavaSparkJob;
import spark.jobserver.japi.JavaValidationException;
import spark.jobserver.japi.SingleJavaValidationException;

public class JavaConfigurableJob implements JavaSparkJob<String, String> {

    public String run(JavaSparkContext sc, JobEnvironment runtime, String data) {
        return data;
    }

    public String verify(JavaSparkContext sc, JobEnvironment runtime, Config config) throws JavaValidationException {
        String input = config.getString("input");
        if (input.length() == 0) {
            throw new SingleJavaValidationException("You passed an empty string");
        }
        return input;
    }
}
