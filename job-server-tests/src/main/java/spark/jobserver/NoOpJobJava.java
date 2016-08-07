package spark.jobserver;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import spark.jobserver.api.JobEnvironment;
import spark.jobserver.api.JobValidation;
import spark.jobserver.api.JSparkContextJob;

public class NoOpJobJava extends JSparkContextJob<Integer> {

    public static void main(String[] args) {
        final SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("NoOpJob");
        final JavaSparkContext jsc = new JavaSparkContext(conf);
        final JobEnvironment jEnv = new TestJobEnvironment();
        final NoOpJobJava nojj = new NoOpJobJava();
        final Integer results = nojj.runJob(jsc, jEnv, ConfigFactory.empty());

        System.out.println("Result is: " + results);
    }

    @Override
    public Integer runJob(JavaSparkContext context, JobEnvironment cfg, Config data) {
        return 6;
    }

    @Override
    public JobValidation validate(JavaSparkContext context, JobEnvironment jEnv, Config cfg) {
        return new JobValidation.JOB_VALID(cfg);
    }
}
