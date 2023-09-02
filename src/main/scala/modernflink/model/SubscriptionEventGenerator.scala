package modernflink.model

import org.apache.flink.streaming.api.functions.source.SourceFunction

import java.time.Instant
import java.util.UUID
import scala.annotation.tailrec
import scala.util.Random

sealed trait SubscriptionEvent:
  def userId: String
  def time: java.time.Instant
  def eventNum: String

case class PaymentEvent(userId: String, time: Instant, eventNum: String) extends SubscriptionEvent

case class CancelEvent(userId: String, time: Instant, eventNum: String) extends SubscriptionEvent

class SubscriptionEventsGenerator(
                                   sleepSeconds: Int,
                                   startTime: Instant
                                 ) extends SourceFunction[SubscriptionEvent]:
  private var running = true
  private var maxEvents = 10

  // override the run and cancel method that came with the source function
  override def run(ctx: SourceFunction.SourceContext[SubscriptionEvent]): Unit =
    run(0, ctx)

  override def cancel(): Unit =
    running = false

  @tailrec
  private def run(start: Long, ctx: SourceFunction.SourceContext[SubscriptionEvent]): Unit =
    if running && maxEvents > 0 then
      maxEvents -= 1
      ctx.collect(emitEvents(start)) //.collect emits elements from the source
      Thread.sleep(sleepSeconds * 1000) // delay emitting events
      run(start + 1, ctx)

  // match the events to the type of payment event
  private def emitEvents(num: Long) =
    import SubscriptionEventsGenerator.genUser
    val diceRoll = Random.nextInt(10)
    if diceRoll < 4 then
      PaymentEvent(genUser, startTime.plusSeconds(num), UUID.randomUUID().toString)
    else
      CancelEvent(genUser, startTime.plusSeconds(num), UUID.randomUUID().toString)


object SubscriptionEventsGenerator:

  val users: Seq[String] =
    Seq("Rob", "David", "Charlie", "Frank", "Peggy", "Max", "David", "Emma", "Faye", "Christie", "Joan")

  def genUser: String = users(scala.util.Random.nextInt(users.length))

