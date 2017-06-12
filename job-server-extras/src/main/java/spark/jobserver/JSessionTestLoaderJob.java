package spark.jobserver;

import com.typesafe.config.Config;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import spark.jobserver.api.JobEnvironment;
//import spark.jobserver.context.SparkSessionContextLikeWrapper;
import spark.jobserver.japi.JSessionJob;

public class JSessionTestLoaderJob extends JSessionJob<Long> {

    private final String tableCreate = "CREATE TABLE `default`.`test_addresses`";
    private final String tableArgs = "(`firstName` String, `lastName` String, `address` String, `city` String)";
    private final String tableRowFormat = "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'";
    private final String tableColFormat = "COLLECTION ITEMS TERMINATED BY '\002'";
    private final String tableMapFormat = "MAP KEYS TERMINATED BY '\003' STORED";
    private final String tableAs = "AS TextFile";
    private final String loadPath = "'src/main/resources/hive_test_job_addresses.txt'";

    @Override
    public Long run(SparkSession spark, JobEnvironment runtime, Config data) {
        spark.sql("DROP TABLE if exists `default`.`test_addresses`");
        spark.sql(String.format("%s %s %s %s %s %s", tableCreate, tableArgs, tableRowFormat, tableColFormat, tableMapFormat, tableAs));
        spark.sql(String.format("LOAD DATA LOCAL INPATH %s OVERWRITE INTO TABLE `default`.`test_addresses`", loadPath));

        final Dataset<Row> addrRdd = spark.sql("SELECT * FROM `default`.`test_addresses`");
        return addrRdd.count();
    }

    @Override
    public Config verify(SparkSession contextWrapper, JobEnvironment runtime, Config config) {
        return config;
    }
}
