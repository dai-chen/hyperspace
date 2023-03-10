package org.opensearch.maximus.function

import java.sql.Timestamp
import java.time.Instant

import org.apache.spark.sql.catalyst.util.DateTimeConstants.MICROS_PER_DAY
import org.apache.spark.sql.catalyst.util.IntervalUtils
import org.apache.spark.unsafe.types.UTF8String

object TumbleUDF {

  val name: String = "TUMBLE"

  def tumble(timestamp: Timestamp, interval: String): Timestamp = {
    val eventTime = timestamp.getTime
    val windowSize = getIntervalInMicroSeconds(interval) / 1000
    val windowStart = eventTime - eventTime % windowSize
    Timestamp.from(Instant.ofEpochMilli(windowStart))
  }

  // Copy from TimeWindow
  private def getIntervalInMicroSeconds(interval: String): Long = {
    val cal = IntervalUtils.stringToInterval(UTF8String.fromString(interval))
    if (cal.months != 0) {
      throw new IllegalArgumentException(
        s"Intervals greater than a month is not supported ($interval).")
    }
    cal.days * MICROS_PER_DAY + cal.microseconds
  }
}
