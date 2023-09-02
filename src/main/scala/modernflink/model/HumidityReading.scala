package modernflink.model

import org.apache.flink.api.common.typeinfo.TypeInformation
import java.time.{Instant, ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.util.Locale
import org.apache.flink.api.serializers.*
import scala.util.Try
case class HumidityReading(location: String, timestamp: Long, humidity: Double):

  def sinkOutput: String = s"${location}, ${timestamp}, ${humidity}"

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

object HumidityReading:

  def fromString(string: String): HumidityReading = Try {
    val Array(location, timestamp, humidity) = string.split(',')
    HumidityReading(
      location.trim,
      timestamp.trim.toLong,
      humidity.trim.toDouble
    )
  }.toOption.getOrElse(HumidityReading("error reading", 0L, 0.0))
  given humidityReadingTypeInformation: TypeInformation[HumidityReading] =
    deriveTypeInformation
