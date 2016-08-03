package spark.jobserver;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.scalactic.*;
import spark.jobserver.api.JobEnvironment;
import spark.jobserver.api.SingleProblem;
import spark.jobserver.api.ValidationProblem;
import spark.jobserver.api.JSparkJob;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class WordCountJava implements JSparkJob<Map<String, Long>> {

    private static final String data = "Java Java Scala Scala Java Map Set";

    public static void main(String[] args) {
        final SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("WordCountJob");
        final JSC jsc = new JSC(conf);
        final JobEnvironment jEnv = new TestJobEnvironment();
        final WordCountJava jcj = new WordCountJava();
        final Map<String, Long> result = jcj.runJob(jsc, jEnv, ConfigFactory.parseString("input.string = " + data));

        for (Map.Entry<String, Long> e : result.entrySet()) {
            System.out.println("Word: " + e.getKey() + " Count: " + e.getValue());
        }
    }

    @Override
    public Map<String, Long> runJob(ContextLike context, JobEnvironment cfg, Config data) {
        final JavaSparkContext ctx = javaSparkContext(context);
        return ctx.parallelize(data.getStringList("input")).countByValue();
    }

    @Override
    public Or<Config, Every<ValidationProblem>> validate(ContextLike context, JobEnvironment jEnv, Config cfg) {
        try {
            final List<String> input = Arrays.asList(cfg.getString("input").split(" "));
            return new Good<>(ConfigFactory.parseString("input = " + input.toString()));
        } catch (Exception e) {
            return new Bad<>(new One<>(new SingleProblem(e.getMessage())));
        }
    }

}
