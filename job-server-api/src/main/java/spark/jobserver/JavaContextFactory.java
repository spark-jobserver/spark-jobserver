package spark.jobserver;

import com.typesafe.config.Config;
import org.apache.spark.SparkConf;

public interface JavaContextFactory<C> {
    C makeContext(SparkConf conf, Config cfg, String contextName);

    C makeContext(Config cfg, Config contextConfig, String contextName);
}
