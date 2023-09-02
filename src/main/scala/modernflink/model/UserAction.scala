package modernflink.model

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flinkx.api.serializers.*

import java.time.{Instant, ZonedDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.Locale
import scala.util.Try

case class UserAction(
    timestamp: Long,
    action: String,
    userid: String,
    name: String
):

  override def toString: String =
    s"UserAction(${formatTime()}, $action, $userid, $name)"

  def formatTime(format: String = "yyyy-MM-dd"): String =
    DateTimeFormatter
      .ofPattern(format, Locale.ENGLISH)
      .format(
        ZonedDateTime.ofInstant(
          Instant.ofEpochSecond(timestamp),
          ZoneId.systemDefault()
        )
      )

object UserAction:
  def fromString(string: String): UserAction =
    Try {
      val Array(timestamp, action, userid, name) = string.split(',')
      UserAction(timestamp.trim.toLong, action.trim, userid.trim, name.trim)
    }.toOption
      .getOrElse(UserAction(0L, "error reading", "error reading", "error reading"))

  given humidityReadingTypeInformation: TypeInformation[UserAction] =
    deriveTypeInformation
