package spark.jobserver.api;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * This is the class that validates a job before the SJS runs the job.
 * This is meant to emulate the Scalatic API that the Scala API uses, but in
 * a more Java way that doesn't require very obtuse Java syntax.
 */

public class JobValidation {
    /**
     * A valid Job returns it's configuration that will be used in the run
     * job stage.
     */
    public static final class JOB_VALID extends JobValidation {
        private final Config cfg;

        public JOB_VALID(Config cfg) {
            this.cfg = cfg;
        }

        public JOB_VALID() {
            this.cfg = ConfigFactory.empty();
        }

        public Config getConfig() {
            return this.cfg;
        }
    }

    /**
     * An invalid job returns a throwable, which is the users responsibility
     * inside the validate method to craft their own message to send back.
     */
    public static final class JOB_INVALID extends JobValidation {
        private final Throwable error;

        public JOB_INVALID(Throwable t) {
            this.error = t;
        }

        public Throwable getError() {
            return this.error;
        }
    }
}