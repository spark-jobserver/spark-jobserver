package spark.jobserver;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.scalactic.Every;
import org.scalactic.Good;
import org.scalactic.Or;
import spark.jobserver.api.JobEnvironment;
import spark.jobserver.api.ValidationProblem;
import spark.jobserver.api.JSparkJob;

import java.util.Arrays;
import java.util.List;

public class ShortDoubleJobJava implements JSparkJob<List<Double>> {
    private final Double[] data = new Double[]{1.0, 2.0, 3.0};

    public static void main(String[] args) {
        final SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("ShortDoubleJob");
        final JSC jsc = new JSC(conf);
        final JobEnvironment jEnv = new TestJobEnvironment();
        final ShortDoubleJobJava sdj = new ShortDoubleJobJava();
        final List<Double> result = sdj.runJob(jsc, jEnv, ConfigFactory.empty());

        System.out.println("Result is: " + result);
    }

    @Override
    public List<Double> runJob(ContextLike context, JobEnvironment cfg, Config data) {
        final JavaSparkContext jsc = javaSparkContext(context);
        final JavaRDD<Double> rdd = jsc.parallelize(Arrays.asList(this.data));
        return rdd.map(new Function<Double, Double>() {
            @Override
            public Double call(Double t) throws Exception {
                return t * 2;
            }
        }).collect();
    }

    @Override
    public Or<Config, Every<ValidationProblem>> validate(ContextLike context, JobEnvironment jEnv, Config cfg) {
        return new Good<>(ConfigFactory.parseString("num = 5.0"));
    }
}
