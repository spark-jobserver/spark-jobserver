package spark.jobserver.util

import java.time.ZonedDateTime


object DateUtils {

  /**
   * Implicit conversions so we can use Scala comparison operators
   * with ZonedDateTime
   */
  implicit def dateTimeToScalaWrapper(dt: ZonedDateTime): DateTimeWrapper = new DateTimeWrapper(dt)

  class DateTimeWrapper(dt: ZonedDateTime) extends Ordered[ZonedDateTime] with Ordering[ZonedDateTime] {
    def compare(that: ZonedDateTime): Int = dt.compareTo(that)
    def compare(a: ZonedDateTime, b: ZonedDateTime): Int = a.compareTo(b)
  }
}
