package spark.jobserver.context;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.joda.time.DateTime;
import org.scalactic.Bad;
import org.scalactic.Good;
import org.scalactic.Or;
import spark.jobserver.ContextLike;
import spark.jobserver.JavaJarInfo;
import spark.jobserver.JobCache;
import spark.jobserver.api.JSparkJob;

import java.util.Map;

public class JavaSparkContextFactory implements JContextFactory<JavaSparkContext>, ContextLike {

    private JavaSparkContext javaSparkContext;

    @Override
    public JavaSparkContext makeContext(SparkConf conf, Config cfg, String contextName) {
        conf.setAppName(contextName);
        javaSparkContext = new JavaContextLike(conf);
        final Config hadoopConfig = cfg.getConfig("hadoop");
        for (Map.Entry<String, ConfigValue> e : hadoopConfig.entrySet()){
            javaSparkContext.hadoopConfiguration().set(e.getKey(), e.getValue().render());
        }
        return javaSparkContext;
    }

    @Override
    public Or<JobContainer<?>, LoadingError> loadAndValidateJob(String appName, DateTime uploadTime, String classPath, JobCache jobCache) {

        final JavaJarInfo jobJarInfo;
        try{
            jobJarInfo = jobCache.getJavaJob(appName, uploadTime, classPath);
        }catch(ClassNotFoundException e){
            return new Bad<>(JobClassNotFound$.MODULE$);
        }catch(Exception e){
            return new Bad<>(new JobLoadError(e));
        }

        final JSparkJob<?> job = jobJarInfo.job();
        if (isValidJob(job)){
            return new Good<>(new JavaJobContainer(job));
        }else{
            return new Bad<>(JobWrongType$.MODULE$);
        }
    }

    private boolean isValidJob(JSparkJob<?> job){
        return job != null;
    }

    @Override
    public SparkContext sparkContext() {
        return this.javaSparkContext.sc();
    }

    @Override
    public void stop() {
        this.javaSparkContext.stop();
    }
}
