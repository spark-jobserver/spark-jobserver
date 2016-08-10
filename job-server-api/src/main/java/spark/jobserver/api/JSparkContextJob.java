package spark.jobserver.api;

import org.apache.spark.api.java.JavaSparkContext;
import spark.jobserver.ContextLike;

/**
 * This is a class the user would actually implement. They only have to implement
 * methods that have their proper SparkContext type, so this should look mostly like the
 * Scala API, but feel more Java. (If that makes any sense)
 *
 * @param <R> The job's return type.
 */
public abstract class JSparkContextJob<R> extends JSparkJob<JavaSparkContext, R> {
    /**
     * The method that each Context Type must implement to give a type safe Job
     *
     * @param c The SparkContext being passed in.
     * @return Proper Context Type, in this case JavaSparkContext
     */
    @Override
    protected JavaSparkContext findContext(ContextLike c) {
        return JavaSparkContext.fromSparkContext(c.sparkContext());
    }
}
