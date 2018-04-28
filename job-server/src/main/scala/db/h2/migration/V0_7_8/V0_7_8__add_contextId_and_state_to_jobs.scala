package db.h2.migration.V0_7_8

import org.slf4j.LoggerFactory
import db.migration.V0_7_8.Migration

import slick.driver.H2Driver.api.actionBasedSQLInterpolation
import slick.profile.SqlAction
import slick.dbio.Effect
import slick.dbio.NoStream

class V0_7_8__add_contextId_and_state_to_jobs extends Migration {
  val logger = LoggerFactory.getLogger(getClass)

  protected def insertState(id: String, status: String): SqlAction[Int, NoStream, Effect] = {
    sqlu"""UPDATE "JOBS" SET "STATE"=${status} WHERE "JOB_ID"=${id}"""
  }

  val addContextId = sqlu"""ALTER TABLE "JOBS" ADD COLUMN "CONTEXT_ID" VARCHAR(255)"""
  val updateContextId = sqlu"""UPDATE "JOBS" SET "CONTEXT_ID" = (SELECT "CONTEXT_NAME")"""
  val setContextIdNotNull = sqlu"""ALTER TABLE "JOBS" ALTER COLUMN "CONTEXT_ID" SET NOT NULL"""
  val addState = sqlu"""ALTER TABLE "JOBS" ADD COLUMN "STATE" VARCHAR(255)"""
  val getJobsEndTimeAndError = sql"""SELECT "JOB_ID", "END_TIME", "ERROR" FROM "JOBS"""".as[JobData]
  val setStateNotNull = sqlu"""ALTER TABLE "JOBS" ALTER COLUMN "STATE" SET NOT NULL"""
}