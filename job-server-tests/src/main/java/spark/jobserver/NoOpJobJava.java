package spark.jobserver;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.SparkConf;
import org.scalactic.Every;
import org.scalactic.Good;
import org.scalactic.Or;
import spark.jobserver.api.JobEnvironment;
import spark.jobserver.api.ValidationProblem;
import spark.jobserver.api.JSparkJob;

public class NoOpJobJava implements JSparkJob<Integer> {

    public static void main(String[] args) {
        final SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("NoOpJob");
        final JSC jsc = new JSC(conf);
        final JobEnvironment jEnv = new TestJobEnvironment();
        final NoOpJobJava nojj = new NoOpJobJava();
        final Integer results = nojj.runJob(jsc, jEnv, ConfigFactory.empty());

        System.out.println("Result is: " + results);
    }

    @Override
    public Integer runJob(ContextLike context, JobEnvironment cfg, Config data) {
        return 6;
    }

    @Override
    public Or<Config, Every<ValidationProblem>> validate(ContextLike context, JobEnvironment jEnv, Config cfg) {
        return new Good<>(cfg);
    }
}
