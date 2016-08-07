package spark.jobserver.examples;

import com.typesafe.config.Config;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import spark.jobserver.api.JobEnvironment;
import spark.jobserver.api.JobValidation;
import spark.jobserver.context.JSparkStreamingJob;

import java.util.Arrays;
import java.util.LinkedList;

public class StreamingTestJob extends JSparkStreamingJob<Void> {
    @Override
    public Void runJob(StreamingContext context, JobEnvironment cfg, Config data) {
        final JavaStreamingContext jsc = new JavaStreamingContext(context);
        final JavaRDD<Integer> d = jsc.sparkContext().parallelize(Arrays.asList(1, 2, 3, 4, 5));

        final LinkedList<JavaRDD<Integer>> q = new LinkedList<>();
        q.add(d);

        final JavaDStream<Integer> dStream = jsc.queueStream(q);
        final JavaPairDStream<Integer, Long> counts = dStream.countByValue();

        counts.print(5);
        jsc.start();
        jsc.awaitTermination();
        return null;
    }

    @Override
    public JobValidation validate(StreamingContext context, JobEnvironment jEnv, Config cfg) {
        return new JobValidation.JOB_VALID();
    }
}
