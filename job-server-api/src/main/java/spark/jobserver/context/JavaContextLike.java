package spark.jobserver.context;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import spark.jobserver.ContextLike;

public class JavaContextLike extends JavaSparkContext implements ContextLike {
    JavaContextLike(SparkConf cfg){
        super(cfg);
    }
    @Override
    public SparkContext sparkContext() {
        return this.sc();
    }

    @Override
    public void stop() {
        this.stop();
    }
}