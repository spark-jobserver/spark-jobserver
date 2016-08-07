package spark.jobserver.api;

import com.typesafe.config.Config;
import spark.jobserver.ContextLike;

import java.io.Serializable;

/**
 * This is the top level class fo the Java API, users shouldn't have many
 * reasons to extend this class.
 *
 * @param <C> The context type (JavaSparkContext, SQLContext) of the Job
 * @param <R> The return type of the job
 */

public abstract class JSparkJob<C, R> implements Serializable {

    /**
     * These are the internal methods the SJS executes in order to pass it's ContextLike
     * into the job, but allow the user extending the class to have their proper context
     * type rather than having to deal with a nebulous ContextLike
     *
     * @param context The SparkContext being passed in
     * @param cfg     The JobEnvironment the job is running in
     * @param data    The validated job config from the validate() method
     * @return R
     */
    final public R runJobImpl(ContextLike context, JobEnvironment cfg, Config data) {
        final C ctx = this.findContext(context);
        return this.runJob(ctx, cfg, data);
    }

    final public JobValidation validateImpl(ContextLike context, JobEnvironment jEnv, Config cfg) {
        final C ctx = this.findContext(context);
        return this.validate(ctx, jEnv, cfg);
    }

    /**
     * The implementing class must specify how to cast or generate it's proper SparkContext from
     * the ContextLike, normally it's just a matter of casting it.
     *
     * @param c The SparkContext being passed in.
     * @return The proper SparkContext Type
     */
    abstract protected C findContext(ContextLike c);

    /**
     * These are the abstract methods that the user actually implements. The context (C) and return
     * type (R) are pamaterized and will be filled in down the chain.
     *
     * @param context The actual type of the SparkContext
     * @param cfg     The JobEnvironment the job is running in
     * @param data    The validated data passed from the validate method
     * @return The job results of type R
     */

    abstract public R runJob(C context, JobEnvironment cfg, Config data);

    abstract public JobValidation validate(C context, JobEnvironment jEnv, Config cfg);
}
