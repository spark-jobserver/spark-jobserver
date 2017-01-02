package spark.jobserver;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import spark.jobserver.api.JobEnvironment;
import spark.jobserver.japi.JSqlJob;

public class JSqlTestJob implements JSqlJob<Integer> {
    @Override
    public Integer run(SQLContext sc, JobEnvironment runtime, Config data) {
        Row row = sc.sql("select 1+1").take(1)[0];
        return row.getInt(0);
    }

    @Override
    public Config verify(SQLContext sc, JobEnvironment runtime, Config config) {
        return ConfigFactory.empty();
    }
}
