package modernflink.model

import java.time.{Instant, ZonedDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.Locale
import scala.util.Try

case class HumidityReading(location: String, timestamp: Long, humidity: Double):

  def sinkOutput: String = s"$location, $timestamp, $humidity"

  override def toString: String =
    s"HumidityReading($location, ${formatTime()}, $humidity)"

  def formatTime(format: String = "yyyy-MM-dd"): String =
    DateTimeFormatter
      .ofPattern(format, Locale.ENGLISH)
      .format(
        ZonedDateTime.ofInstant(
          Instant.ofEpochSecond(timestamp),
          ZoneId.systemDefault()
        )
      )

  def toLocalSummary: LocalSummary =
    LocalSummary(location, s"Humidity on ${formatTime("yyyy-MM-dd")} is $humidity")

object HumidityReading:

  def fromString(string: String): HumidityReading =
    Try {
      val Array(location, timestamp, humidity) = string.split(',')
      HumidityReading(
        location.trim,
        timestamp.trim.toLong,
        humidity.trim.toDouble
      )
    }.getOrElse(HumidityReading("error reading", 0L, 0.0))
