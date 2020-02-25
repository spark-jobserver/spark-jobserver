CREATE TABLE "BINARIES" (
  "BIN_ID"          SERIAL                        NOT NULL PRIMARY KEY,
  "APP_NAME"        VARCHAR(255)                  NOT NULL,
  "BINARY_TYPE"     VARCHAR(255)                  NOT NULL,
  "UPLOAD_TIME"     TIMESTAMP WITHOUT TIME ZONE   NOT NULL,
  "BIN_HASH"        BYTEA                         NOT NULL
);

CREATE TABLE "JOBS" (
  "JOB_ID"              VARCHAR(255)                  NOT NULL PRIMARY KEY,
  "CONTEXT_ID"          VARCHAR(255)                  NOT NULL,
  "CONTEXT_NAME"        VARCHAR(255)                  NOT NULL,
  "CLASSPATH"           VARCHAR(255)                  NOT NULL,
  "STATE"               VARCHAR(255)                  NOT NULL,
  "START_TIME"          TIMESTAMP WITHOUT TIME ZONE,
  "END_TIME"            TIMESTAMP WITHOUT TIME ZONE,
  "BIN_IDS"             TEXT                          NOT NULL,
  "URIS"                TEXT                          NOT NULL,
  "ERROR"               TEXT,
  "ERROR_CLASS"         VARCHAR(255),
  "ERROR_STACK_TRACE"   TEXT
);

CREATE TABLE "CONFIGS" (
  "JOB_ID"          VARCHAR(255)                  NOT NULL PRIMARY KEY,
  "JOB_CONFIG"      TEXT                          NOT NULL
);

CREATE TABLE "CONTEXTS" (
  "ID"                    VARCHAR(255)    NOT NULL PRIMARY KEY,
  "NAME"                  VARCHAR(255)    NOT NULL,
  "CONFIG"                TEXT            NOT NULL,
  "ACTOR_ADDRESS"         VARCHAR(255),
  "START_TIME"            TIMESTAMP       NOT NULL,
  "END_TIME"              TIMESTAMP,
  "STATE"                 VARCHAR(255)    NOT NULL,
  "ERROR"                 TEXT
);

CREATE TABLE "BLOBS" (
  "BIN_ID"        VARCHAR(255)                NOT NULL PRIMARY KEY,
  "BINARY"        OID                         NOT NULL
);

/* The lo module provides support for managing Large Objects */
CREATE TRIGGER t_binary BEFORE UPDATE OR DELETE ON "BLOBS" FOR EACH ROW EXECUTE PROCEDURE lo_manage("BINARY");
