package spark.jobserver;

import com.typesafe.config.Config;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.scalactic.Every;
import org.scalactic.Good;
import org.scalactic.Or;
import scala.Tuple2;
import spark.jobserver.api.JobEnvironment;
import spark.jobserver.api.ValidationProblem;

import java.util.Arrays;
import java.util.Date;
import java.util.Random;

import static java.lang.Math.pow;

public class LongPiJobJava implements JavaSparkJob<JavaSparkContext, Double, Integer> {

    private final static Random rand = new Random(new Date().getTime());

    public static void main (String[] args){
        final SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("LongPiJob");
        final JavaSparkContext jsc = new JavaSparkContext(conf);
        final JobEnvironment jEnv = new TestJobEnvironment();
        final LongPiJobJava lpj = new LongPiJobJava();
        final double result = lpj.runJob(jsc, jEnv, 5);

        System.out.println("Pi is " + result);
    }

    @Override
    public Double runJob(JavaSparkContext context, JobEnvironment cfg, Integer data) {
        long hit = 0L;
        long total = 0L;
        final long start = new Date().getTime();
        while (stillHaveTime(start, data)) {
            final Tuple2<Integer, Integer> count = estimatePi(context);
            hit += count._1();
            total += count._2();
        }
        return (4.0 * hit) / total;
    }

    @Override
    public Or<Integer, Every<ValidationProblem>> validate(JavaSparkContext context, JobEnvironment jEnv, Config cfg) {
        final Integer duration;
        if (cfg.hasPath("stress.test.longpijob.duration")) {
            duration = cfg.getInt("stress.test.longpijob.duration");
        } else {
            duration = 5;
        }
        return new Good<>(duration);
    }

    private Tuple2<Integer, Integer> estimatePi(JavaSparkContext jsc) {
        final Integer[] data = new Integer[1000];
        for (int i = 0; i < 1000; i++) {
            data[i] = i;
        }
        final JavaRDD<Integer> rdd = jsc.parallelize(Arrays.asList(data));
        return rdd.map(new DartFunction()).reduce(new PiReduceFunction());
    }

    private static class DartFunction implements Function<Integer, Tuple2<Integer, Integer>> {
        @Override
        public Tuple2<Integer, Integer> call(Integer i) throws Exception {
            return throwDart() ? new Tuple2<>(1, 1) : new Tuple2<>(0, 1);
        }
    }

    private static class PiReduceFunction implements Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> {
        @Override
        public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> x, Tuple2<Integer, Integer> y) throws Exception {
            return new Tuple2<>(x._1() + y._1(), x._2() + y._2());
        }
    }

    private static boolean throwDart() {
        final double x = rand.nextDouble() * 2;
        final double y = rand.nextDouble() * 2;
        final double dist = pow(x - 1, 2) + pow(y - 1, 2);
        return (dist <= 1);
    }


    private boolean stillHaveTime(long startTime, int duration) {
        final long now = new Date().getTime();
        return (now - startTime) < duration * 1000;
    }
}
