package spark.jobserver;

import com.typesafe.config.Config;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.scalactic.Every;
import org.scalactic.Good;
import org.scalactic.Or;
import spark.jobserver.api.JobEnvironment;
import spark.jobserver.api.ValidationProblem;

import java.util.Arrays;
import java.util.List;

public class ShortDoubleJobJava implements JavaSparkJob<List<Double>, Double> {
    private final Double[] data = new Double[]{1.0, 2.0, 3.0};

    public static void main(String[] args) {
        final SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("ShortDoubleJob");
        final JavaSparkContext jsc = new JavaSparkContext(conf);
        final JobEnvironment jEnv = new TestJobEnvironment();
        final ShortDoubleJobJava sdj = new ShortDoubleJobJava();
        final List<Double> result = sdj.runJob(jsc.sc(), jEnv, 1.0);

        System.out.println("Result is: " + result);
    }

    @Override
    public List<Double> runJob(SparkContext context, JobEnvironment cfg, Double data) {
        final JavaSparkContext jsc = new JavaSparkContext(context);
        final JavaRDD<Double> rdd = jsc.parallelize(Arrays.asList(this.data));
        return rdd.map(new Function<Double, Double>() {
            @Override
            public Double call(Double t) throws Exception {
                return t * 2;
            }
        }).collect();
    }

    @Override
    public Or<Double, Every<ValidationProblem>> validate(SparkContext context, JobEnvironment jEnv, Config cfg) {
        return new Good<>(1.0);
    }
}
