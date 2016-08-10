package spark.jobserver;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import spark.jobserver.api.JobEnvironment;

public class TestJobEnvironment implements JobEnvironment {
    @Override
    public String jobId() {
        return "abcdef";
    }

    @Override
    public NamedObjects namedObjects() {
        return null;
    }

    @Override
    public Config contextConfig() {
        return ConfigFactory.empty();
    }
}
