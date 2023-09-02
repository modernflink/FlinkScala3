package modernflink.model

import org.apache.flink.api.common.typeinfo.TypeInformation
import java.time.{Instant, ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.util.Locale
import org.apache.flink.api.serializers.*
import scala.util.Try 

case class TemperatureReading(location: String, timestamp: Long, max: Double, min: Double):

  override def toString = s"TemperatureReading($location,${formatTime()},$max,$min)"
  def formatTime(format: String = "yyyy-MM-dd"): String =
    DateTimeFormatter.ofPattern(format, Locale.ENGLISH)
      .format(
        ZonedDateTime.ofInstant(
          Instant.ofEpochSecond(timestamp),
          ZoneId.systemDefault()
        )
      )


object TemperatureReading:
  def fromString(string: String): TemperatureReading = Try {
    val Array(location, timestamp, max, min) = string.split(',')
    TemperatureReading(location.trim, timestamp.trim.toLong, max.trim.toDouble, min.trim.toDouble)
  }.toOption.getOrElse(TemperatureReading("error reading", 0L, 0.0, 0.0))

  given humidityReadingTypeInformation: TypeInformation[TemperatureReading] = deriveTypeInformation
