package spark.jobserver;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;

public class JSC extends JavaSparkContext implements ContextLike {
    JSC(SparkConf conf){
        super(conf);
    }
    @Override
    public SparkContext sparkContext() {
        return this.sc();
    }
}