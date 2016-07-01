package spark.jobserver;

import com.typesafe.config.Config;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import spark.jobserver.ContextLike;
import spark.jobserver.util.SparkJobUtils;

public class JavaSparkContextFactory implements JavaContextFactory<JavaSparkContext>, ContextLike {

    private JavaSparkContext javaSparkContext;

    @Override
    public JavaSparkContext makeContext(SparkConf conf, Config cfg, String contextName) {
        javaSparkContext = new JavaSparkContext(conf);
        return javaSparkContext;
    }

    @Override
    public JavaSparkContext makeContext(Config cfg, Config contextConfig, String contextName) {
        final SparkConf sparkConf = SparkJobUtils.configToSparkConf(cfg, contextConfig, contextName);
        return makeContext(sparkConf, cfg, contextName);
    }

    @Override
    public SparkContext sparkContext() {
        return javaSparkContext.sc();
    }

    @Override
    public void stop() {
        javaSparkContext.stop();
    }
}
