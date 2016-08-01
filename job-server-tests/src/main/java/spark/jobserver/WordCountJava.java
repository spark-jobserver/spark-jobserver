package spark.jobserver;

import com.typesafe.config.Config;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.scalactic.*;
import spark.jobserver.api.JobEnvironment;
import spark.jobserver.api.SingleProblem;
import spark.jobserver.api.ValidationProblem;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class WordCountJava implements JavaSparkJob<Map<String, Long>, List<String>> {

    private static final String[] data = new String[]{"Dog", "Dog", "Cat", "Bird", "Scala", "Dog", "Java"};

    public static void main(String[] args) {
        final SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("WordCountJob");
        final JavaSparkContext jsc = new JavaSparkContext(conf);
        final JobEnvironment jEnv = new TestJobEnvironment();
        final WordCountJava jcj = new WordCountJava();
        final Map<String, Long> result = jcj.runJob(jsc.sc(), jEnv, Arrays.asList(WordCountJava.data));

        for (Map.Entry<String, Long> e : result.entrySet()) {
            System.out.println("Word: " + e.getKey() + " Count: " + e.getValue());
        }
    }

    @Override
    public Map<String, Long> runJob(SparkContext context, JobEnvironment cfg, List<String> data) {
        final JavaSparkContext ctx = new JavaSparkContext(context);
        return ctx.parallelize(data).countByValue();
    }

    @Override
    public Or<List<String>, Every<ValidationProblem>> validate(SparkContext context, JobEnvironment jEnv, Config cfg) {
        try {
            final List<String> input = Arrays.asList(cfg.getString("input.string").split(" "));
            return new Good<List<String>, Every<ValidationProblem>>(input);
        } catch (Exception e) {
            return new Bad<List<String>, Every<ValidationProblem>>(new One<>(new SingleProblem(e.getMessage())));
        }
    }

}
