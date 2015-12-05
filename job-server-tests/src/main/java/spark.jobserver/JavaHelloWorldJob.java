package spark.jobserver;

import com.typesafe.config.Config;
import org.apache.spark.api.java.JavaSparkContext;
import spark.jobserver.JavaSparkJob;

public class JavaHelloWorldJob extends JavaSparkJob {
  @Override
  public Object runJob(JavaSparkContext jsc, Object message) {
    return(message);
  }

  @Override
  public Object parse(JavaSparkContext jsc, Config config) {
    return("Hello World!");
  }
}
