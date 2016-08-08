package spark.jobserver;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import spark.jobserver.api.JobEnvironment;
import spark.jobserver.api.JobValidation;
import spark.jobserver.api.JSparkContextJob;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class WordCountJava extends JSparkContextJob<Map<String, Long>> {

    private static final String data = "Java Java Scala Scala Java Map Set";

    public static void main(String[] args) {
        final SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("WordCountJob");
        final JavaSparkContext jsc = new JavaSparkContext(conf);
        final JobEnvironment jEnv = new TestJobEnvironment();
        final WordCountJava jcj = new WordCountJava();
        final Map<String, Long> result = jcj.runJob(jsc, jEnv, ConfigFactory.parseString("input.string = " + data));

        for (Map.Entry<String, Long> e : result.entrySet()) {
            System.out.println("Word: " + e.getKey() + " Count: " + e.getValue());
        }
    }

    @Override
    public Map<String, Long> runJob(JavaSparkContext context, JobEnvironment jEnv, Config data) {
        return context.parallelize(data.getStringList("input")).countByValue();
    }

    @Override
    public JobValidation validate(JavaSparkContext context, JobEnvironment jEnv, Config cfg) {
        try {
            final List<String> input = Arrays.asList(cfg.getString("input").split(" "));
            return new JobValidation.JOB_VALID(ConfigFactory.parseString("input = " + input.toString()));
        } catch (Exception e) {
            return new JobValidation.JOB_INVALID(e);
        }
    }

}