package modernflink.model

import SubscriptionEventGenerator.SubscriptionEvent
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.SourceFunction
import pprint.Tree.Ctx

import java.time.Instant
import java.util.UUID
import scala.meta.tokens.Token.Interpolation.Start
import scala.util.Random
object SubscriptionEventGenerator :

  sealed trait SubscriptionEvent:
    def userId: String
    def time: java.time.Instant
    def eventNum: String

  case class PaymentEvent(
                         userId: String,
                         time: Instant,
                         eventNum: String
                         ) extends SubscriptionEvent

  case class CancelEvent(
                          userId: String,
                         time: Instant,
                          eventNum: String
                         ) extends SubscriptionEvent

  class SubscriptionEventsGenerator(
                                    sleepSeconds: Int,
                                    startTime: Instant = Instant.now()
                                   ) extends SourceFunction[SubscriptionEvent] {

    import SubscriptionEventsGenerator._

    private var running = true
    private var maxEvents = 10

    private def run(start: Long, ctx: SourceFunction.SourceContext[SubscriptionEvent]): Unit =
      if running && maxEvents > 0 then {
        maxEvents -= 1
        ctx.collect(emitEvents(start)) //.collect emits elements from the source
        Thread.sleep(sleepSeconds*1000) // delay emitting events
        run(start + 1, ctx)
      }

    // match the events to the type of payment event
    private def emitEvents(num: Long) = {
      val diceRoll = Random.nextInt(10)
      if diceRoll < 4 then
        PaymentEvent(genUser, startTime.plusSeconds(num), UUID.randomUUID().toString)
      else
        CancelEvent(genUser, startTime.plusSeconds(num), UUID.randomUUID().toString)
    }

    // override the run and cancel method that came with the source function
    override def run(
                      ctx: SourceFunction.SourceContext[SubscriptionEvent]
                    ): Unit = run(0, ctx)

    override def cancel(): Unit = {
      running = false
    }
  }

    object SubscriptionEventsGenerator{

      val users: Seq[String] = Seq("Rob", "David", "Charlie", "Frank", "Peggy", "Max", "David", "Emma", "Chris", "Doug", "Katie", "Faye", "Christie", "Joan")
      def genUser: String = users(scala.util.Random.nextInt(users.length))
    }

