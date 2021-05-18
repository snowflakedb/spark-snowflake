package org.apache.spark.sql.snowflake

import java.sql.Date
import java.time.{LocalDate, ZoneId}

import org.apache.spark.sql.catalyst.util.DateTimeTestUtils
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.util.DateTimeUtils.SQLDate

object SFTestUtils {
  val CEST = getZoneId("+02:00")
  val LA = getZoneId("America/Los_Angeles")

  def getZoneId(timeZoneId: String): ZoneId = ZoneId.of(timeZoneId, ZoneId.SHORT_IDS)

  def currentDate(zoneId: ZoneId): SQLDate = localDateToDays(LocalDate.now(zoneId))

  def localDateToDays(localDate: LocalDate): Int = {
    Math.toIntExact(localDate.toEpochDay)
  }

  def fromJavaDate(date: Date) = DateTimeUtils.fromJavaDate(date)
}
