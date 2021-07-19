package spark.jobserver;

import java.util.List;
import com.typesafe.config.Config;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import spark.jobserver.api.JobEnvironment;
import spark.jobserver.japi.JSessionJob;
import spark.jobserver.japi.JavaValidationException;

public class JSessionTestJob extends JSessionJob<Row[]> {

    @Override
    public Row[] run(SparkSession spark, JobEnvironment runtime, Config data) {
        List<Row> rowList = spark.sql(data.getString("sql")).collectAsList();
        Row[] rows = new Row[rowList.size()];
        return rowList.toArray(rows);
    }

    @Override
    public Config verify(SparkSession spark, JobEnvironment runtime, Config config) throws JavaValidationException {
        return config;
    }
}
