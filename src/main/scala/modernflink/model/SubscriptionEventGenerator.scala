package modernflink.model

import org.apache.flink.streaming.api.functions.source.SourceFunction

import java.time.Instant
import java.util.UUID
import scala.annotation.tailrec
import scala.util.Random

sealed trait SubscriptionEvent:
  def userId: String
  def time: java.time.Instant
  def eventId: String

case class PaymentEvent(userId: String, time: Instant, eventId: String) extends SubscriptionEvent

case class CancelEvent(userId: String, time: Instant, eventId: String) extends SubscriptionEvent

class SubscriptionEventsGenerator(
    sleepSeconds: Int,
    startTime: Instant
) extends SourceFunction[SubscriptionEvent]:
  private var running = true

  // override the run and cancel method that came with the source function
  override def run(ctx: SourceFunction.SourceContext[SubscriptionEvent]): Unit =
    run(0, 10, ctx)

  override def cancel(): Unit =
    running = false

  @tailrec
  private def run(
      start: Long,
      remainingEvents: Int,
      ctx: SourceFunction.SourceContext[SubscriptionEvent]
  ): Unit =
    if running && remainingEvents > 0 then
      ctx.collect(emitEvent(start)) // .collect emits elements from the source
      Thread.sleep(sleepSeconds * 1000) // delay emitting events
      run(start + 1, remainingEvents - 1, ctx)

  // match the events to the type of payment event
  private def emitEvent(offsetSeconds: Long) =
    import SubscriptionEventsGenerator.genUser
    val id = UUID.randomUUID().toString
    val diceRoll = Random.nextInt(10)
    if diceRoll < 4 then PaymentEvent(genUser, startTime.plusSeconds(offsetSeconds), id)
    else CancelEvent(genUser, startTime.plusSeconds(offsetSeconds), id)

object SubscriptionEventsGenerator:

  val users: Seq[String] =
    Seq(
      "Rob",
      "David",
      "Charlie",
      "Frank",
      "Peggy",
      "Max",
      "David",
      "Emma",
      "Faye",
      "Christie",
      "Joan"
    )

  def genUser: String = users(scala.util.Random.nextInt(users.length))
