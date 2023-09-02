package modernflink.model

import org.apache.flink.api.common.typeinfo.TypeInformation
import java.time.{Instant, ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.util.Locale
import org.apache.flink.api.serializers.*
import scala.util.Try

case class PurchaseHistory(timestamp: Long, userid: String, amount: Int):

  override def toString: String =
    s"PurchaseHistory(${formatTime()}, $userid, $amount)"

  def formatTime(format: String = "yyyy-MM-dd HH:mm:ss"): String =
    DateTimeFormatter
      .ofPattern(format, Locale.ENGLISH)
      .format(
        ZonedDateTime.ofInstant(
          Instant.ofEpochSecond(timestamp),
          ZoneId.systemDefault()
        )
      )

object PurchaseHistory:
  def fromString(string: String): PurchaseHistory = Try {
    val Array(timestamp, userid, amount) = string.split(",")
    PurchaseHistory(timestamp.trim.toLong, userid.trim, amount.trim.toInt)
  }.toOption.getOrElse(PurchaseHistory(0L, "error reading", 0))
