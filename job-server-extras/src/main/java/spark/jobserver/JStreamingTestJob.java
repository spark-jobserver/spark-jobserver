package spark.jobserver;

import com.typesafe.config.Config;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import spark.jobserver.api.JobEnvironment;
import spark.jobserver.japi.JStreamingJob;

import java.util.Arrays;
import java.util.LinkedList;

public class JStreamingTestJob implements JStreamingJob<Integer> {

    @Override
    public Integer run(StreamingContext context, JobEnvironment jEnv, Config data) {
        final JavaStreamingContext jsc = new JavaStreamingContext(context);
        final JavaRDD<Integer> d = jsc.sparkContext().parallelize(Arrays.asList(1, 2, 3, 4, 5));

        final LinkedList<JavaRDD<Integer>> q = new LinkedList<>();
        q.add(d);

        final JavaDStream<Integer> dStream = jsc.queueStream(q);

        dStream.countByValue().foreachRDD(new VoidFunction<JavaPairRDD<Integer, Long>>() {
            @Override
            public void call(JavaPairRDD<Integer, Long> rdd) throws Exception {
                rdd.collect(); // do something
            }
        });

        jsc.start();
        try {
          jsc.awaitTermination();
        } catch (InterruptedException e) {
          return -1;
        }
        return 1;
    }

    @Override
    public Config verify(StreamingContext context, JobEnvironment jEnv, Config cfg) {
        return cfg;
    }
}
