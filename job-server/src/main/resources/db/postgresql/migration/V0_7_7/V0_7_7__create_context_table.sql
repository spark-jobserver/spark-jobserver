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