package spark.jobserver;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import spark.jobserver.api.JobEnvironment;
import spark.jobserver.api.JobValidation;
import spark.jobserver.api.JSparkContextJob;

import java.util.Arrays;
import java.util.List;

public class ShortDoubleJobJava extends JSparkContextJob<List<Double>> {
    private final Double[] data = new Double[]{1.0, 2.0, 3.0};

    public static void main(String[] args) {
        final SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("ShortDoubleJob");
        final JavaSparkContext jsc = new JavaSparkContext(conf);
        final JobEnvironment jEnv = new TestJobEnvironment();
        final ShortDoubleJobJava sdj = new ShortDoubleJobJava();
        final List<Double> result = sdj.runJob(jsc, jEnv, ConfigFactory.empty());

        System.out.println("Result is: " + result);
    }

    @Override
    public List<Double> runJob(JavaSparkContext context, JobEnvironment jEnv, Config data) {
        final JavaRDD<Double> rdd = context.parallelize(Arrays.asList(this.data));
        return rdd.map(new Function<Double, Double>() {
            @Override
            public Double call(Double t) throws Exception {
                return t * 2;
            }
        }).collect();
    }

    @Override
    public JobValidation validate(JavaSparkContext context, JobEnvironment jEnv, Config cfg) {
        return new JobValidation.JOB_VALID(ConfigFactory.parseString("num = 5.0"));
    }
}
