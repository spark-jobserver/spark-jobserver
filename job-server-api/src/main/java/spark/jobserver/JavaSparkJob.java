package spark.jobserver;

import com.typesafe.config.Config;

public interface JavaSparkJob<C, R> {
    R runJob(C context, Config cfg);

    JavaSparkJobValidation validate(C context, Config cfg);
}
