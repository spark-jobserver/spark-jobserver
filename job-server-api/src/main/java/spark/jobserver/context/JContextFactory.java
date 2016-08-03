package spark.jobserver.context;

import com.typesafe.config.Config;
import org.apache.spark.SparkConf;
import org.joda.time.DateTime;
import org.scalactic.Or;
import spark.jobserver.JobCache;
import spark.jobserver.util.SparkJobUtils;

public interface JContextFactory<C> extends ContextFactoryBase {

    C makeContext(SparkConf conf, Config cfg, String contextName);

    Or<JobContainer<?>, LoadingError> loadAndValidateJob(String appName, DateTime uploadTime, String classPath, JobCache jobCache);

    default C makeContext(Config cfg, Config contextConfig, String contextName){
        final SparkConf sparkConf = SparkJobUtils.configToSparkConf(cfg, contextConfig, contextName);
        return makeContext(sparkConf, cfg, contextName);
    }
}
