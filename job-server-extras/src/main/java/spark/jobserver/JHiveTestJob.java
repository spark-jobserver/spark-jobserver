package spark.jobserver;

import java.util.List;
import com.typesafe.config.Config;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.test.TestHiveContext;
import spark.jobserver.api.JobEnvironment;
import spark.jobserver.japi.JTestHiveJob;

public class JHiveTestJob implements JTestHiveJob<Row[]> {

    @Override
    public Row[] run(TestHiveContext sc, JobEnvironment runtime, Config data) {
        List<Row> rowList = sc.sql(data.getString("sql")).collectAsList();
        Row[] rows = new Row[rowList.size()];
        return rowList.toArray(rows);
    }

    @Override
    public Config verify(TestHiveContext sc, JobEnvironment runtime, Config config) {
        return config;
    }
}
