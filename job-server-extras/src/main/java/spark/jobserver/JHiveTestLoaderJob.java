package spark.jobserver;

import com.typesafe.config.Config;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;
import spark.jobserver.api.JobEnvironment;
import spark.jobserver.japi.JHiveJob;

public class JHiveTestLoaderJob implements JHiveJob<Long> {

    private final String tableCreate = "CREATE TABLE `default`.`test_addresses`";
    private final String tableArgs = "(`firstName` String, `lastName` String, `address` String, `city` String)";
    private final String tableRowFormat = "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'";
    private final String tableColFormat = "COLLECTION ITEMS TERMINATED BY '\002'";
    private final String tableMapFormat = "MAP KEYS TERMINATED BY '\003' STORED";
    private final String tableAs = "AS TextFile";
    private final String loadPath = "'src/main/resources/hive_test_job_addresses.txt'";

    @Override
    public Long run(HiveContext sc, JobEnvironment runtime, Config data) {
        sc.sql("DROP TABLE if exists `default`.`test_addresses`");
        sc.sql(String.format("%s %s %s %s %s %s", tableCreate, tableArgs, tableRowFormat, tableColFormat, tableMapFormat, tableAs));
        sc.sql(String.format("LOAD DATA LOCAL INPATH %s OVERWRITE INTO TABLE `default`.`test_addresses`", loadPath));

        final DataFrame addrRdd = sc.sql("SELECT * FROM `default`.`test_addresses`");
        return addrRdd.count();
    }

    @Override
    public Config verify(HiveContext sc, JobEnvironment runtime, Config config) {
        return config;
    }
}
