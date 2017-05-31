CREATE TABLE `JARS` (
  `JAR_ID`          SERIAL                        NOT NULL PRIMARY KEY,
  `APP_NAME`        VARCHAR(255)                  NOT NULL,
  `UPLOAD_TIME`     TIMESTAMP                     NOT NULL,
  `JAR`             LONGBLOB                      NOT NULL
);

CREATE TABLE `JOBS` (
  `JOB_ID`          VARCHAR(255)                  NOT NULL PRIMARY KEY,
  `CONTEXT_NAME`    VARCHAR(255)                  NOT NULL,
  `JAR_ID`          INTEGER                       NOT NULL,
  `CLASSPATH`       VARCHAR(255)                  NOT NULL,
  `START_TIME`      TIMESTAMP                     NOT NULL,
  `END_TIME`        TIMESTAMP                     NULL,
  `ERROR`           TEXT                          NULL
);

CREATE TABLE `CONFIGS` (
  `JOB_ID`          VARCHAR(255)                  NOT NULL PRIMARY KEY,
  `JOB_CONFIG`      TEXT                          NOT NULL
);
