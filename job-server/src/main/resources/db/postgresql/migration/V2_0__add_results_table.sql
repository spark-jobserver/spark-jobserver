CREATE TABLE "JOB_RESULTS" (
  "JOB_ID"        VARCHAR(255)                NOT NULL PRIMARY KEY,
  "RESULT"        OID                         NOT NULL
);

/* The lo module provides support for managing Large Objects */
CREATE TRIGGER t_result BEFORE UPDATE OR DELETE ON "JOB_RESULTS" FOR EACH ROW EXECUTE PROCEDURE lo_manage("RESULT");
