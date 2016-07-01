package spark.jobserver;

import com.typesafe.config.Config;
import org.apache.spark.api.java.JavaSparkContext;

public class JavaHelloWorldJob implements JavaSparkJob<JavaSparkContext, String>{

    @Override
    public String runJob(JavaSparkContext context, Config cfg) {
        return "Hello world!";
    }

    @Override
    public JavaSparkJobValidation validate(JavaSparkContext context, Config cfg) {
        return JavaSparkJobValidation.JOB_VALID;
    }
}
