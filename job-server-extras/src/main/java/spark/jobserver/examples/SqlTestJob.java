package spark.jobserver.examples;

import com.typesafe.config.Config;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import spark.jobserver.api.JobEnvironment;
import spark.jobserver.api.JobValidation;
import spark.jobserver.context.JSparkSqlJob;

public class SqlTestJob extends JSparkSqlJob<Long> {
    @Override
    public Long runJob(SQLContext context, JobEnvironment jEnv, Config data) {
        final DataFrame df = context.range(1, 100).toDF();
        return df.count();
    }

    @Override
    public JobValidation validate(SQLContext context, JobEnvironment jEnv, Config cfg) {
        return new JobValidation.JOB_VALID();
    }
}
