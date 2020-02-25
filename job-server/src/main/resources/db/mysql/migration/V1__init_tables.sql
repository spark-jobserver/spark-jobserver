CREATE TABLE `BINARIES` (
  `BIN_ID`          SERIAL                        NOT NULL PRIMARY KEY,
  `APP_NAME`        VARCHAR(255)                  NOT NULL,
  `BINARY_TYPE`     VARCHAR(255)                  NOT NULL,
  `UPLOAD_TIME`     TIMESTAMP(3)                  NOT NULL,
  `BIN_HASH`        VARBINARY(32)                 NOT NULL
);

CREATE TABLE `JOBS` (
  `JOB_ID`             VARCHAR(255)                  NOT NULL PRIMARY KEY,
  `CONTEXT_ID`         VARCHAR(255)                  NOT NULL,
  `CONTEXT_NAME`       VARCHAR(255)                  NOT NULL,
  `CLASSPATH`          VARCHAR(255)                  NOT NULL,
  `STATE`              VARCHAR(255)                  NOT NULL,
  `START_TIME`         TIMESTAMP(3)                  NOT NULL,
  `END_TIME`           TIMESTAMP(3)                  NULL,
  `BIN_IDS`            TEXT                          NULL,
  `URIS`               TEXT                          NULL,
  `ERROR`              TEXT                          NULL,
  `ERROR_CLASS`        VARCHAR(255)                  NOT NULL,
  `ERROR_STACK_TRACE`  TEXT                          NULL

);

CREATE TABLE `CONFIGS` (
  `JOB_ID`          VARCHAR(255)                  NOT NULL PRIMARY KEY,
  `JOB_CONFIG`      TEXT                          NOT NULL
);

CREATE TABLE `CONTEXTS` (
  `ID`                    VARCHAR(255)       NOT NULL PRIMARY KEY,
  `NAME`                  VARCHAR(255)       NOT NULL,
  `CONFIG`                TEXT               NOT NULL,
  `ACTOR_ADDRESS`         VARCHAR(255)       NULL,
  `START_TIME`            TIMESTAMP(3)       NOT NULL,
  `END_TIME`              TIMESTAMP(3)       NULL,
  `STATE`                 VARCHAR(255)       NOT NULL,
  `ERROR`                 TEXT               NULL
);

CREATE TABLE `BLOBS` (
  `BIN_ID`       VARCHAR(255)                  NOT NULL PRIMARY KEY,
  `BINARY`       LONGBLOB                      NOT NULL
);