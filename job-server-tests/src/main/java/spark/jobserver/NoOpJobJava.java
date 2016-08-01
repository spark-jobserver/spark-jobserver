package spark.jobserver;

import com.typesafe.config.Config;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.scalactic.Every;
import org.scalactic.Good;
import org.scalactic.Or;
import spark.jobserver.api.JobEnvironment;
import spark.jobserver.api.ValidationProblem;

public class NoOpJobJava implements JavaSparkJob<Integer, Integer> {

    public static void main(String[] args) {
        final SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("NoOpJob");
        final JavaSparkContext jsc = new JavaSparkContext(conf);
        final JobEnvironment jEnv = new TestJobEnvironment();
        final NoOpJobJava nojj = new NoOpJobJava();
        final Integer results = nojj.runJob(jsc.sc(), jEnv, 1);

        System.out.println("Result is: " + results);
    }

    @Override
    public Integer runJob(SparkContext context, JobEnvironment cfg, Integer data) {
        return data;
    }

    @Override
    public Or<Integer, Every<ValidationProblem>> validate(SparkContext context, JobEnvironment jEnv, Config cfg) {
        return new Good<>(1);
    }
}
