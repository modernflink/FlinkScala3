package modernflink.model

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.SourceFunction
import pprint.Tree.Ctx
import DepositEventGenerator.{genUser, genCurrency}
import modernflink.model.BankingEventGenerator.DepositEventGenerator

import java.time.Instant
import java.util.UUID
import scala.meta.tokens.Token.Interpolation.Start
import scala.util.Random

object BankingEventGenerator {

//  case class Message(userId: String, time:Long) {
//    def sendMessage(d: FiniteDuration)(implicit startTime: Instant): PlayerRegistered =
//      PlayerRegistered(startTime.plusMillis(d.toMillis), playerId, nickname)
//  }

  case class Deposit(userId: String, time: Instant, amount: Int, currency: String)

  class DepositEventGenerator(sleepSeconds: Long, startTime: Instant = Instant.now()
                             ) extends SourceFunction[Deposit] {

    import DepositEventGenerator._

    private var running = true
    private var maxEvents = 15

    private def run(start: Long, ctx: SourceFunction.SourceContext[Deposit]): Unit =
      if running && maxEvents > 0 then {
        maxEvents -= 1
        ctx.collect(emitEvents(start))
        Thread.sleep(sleepSeconds*1000)
        run(start + 1, ctx)
      }
    private def emitEvents(num: Long) = {
      Deposit(genUser, startTime.plusSeconds(num), scala.util.Random.nextInt(10000000), genCurrency)
    }
    override def run(
                      ctx: SourceFunction.SourceContext[Deposit]
                    ): Unit = run(0, ctx)

    override def cancel(): Unit = {
      running = false
    }
  }

    def main(args: Array[String]): Unit = {
      val env = StreamExecutionEnvironment.getExecutionEnvironment()
      val testStream = env.addSource(new DepositEventGenerator(
        sleepSeconds = 1,
        startTime = Instant.parse("2023-08-13T00:00:00.00Z"))
      )
      testStream.print()
      env.execute()
    }
}
object DepositEventGenerator {

  val users: Seq[String] = Seq("Rob", "David", "Charlie", "Frank", "Peggy", "Max", "David", "Emma", "Chris")
  def genUser: String = users(scala.util.Random.nextInt(users.length))

  val currency: Seq[String] = Seq("USD", "Euro", "Yen")
  def genCurrency: String = currency(scala.util.Random.nextInt(currency.length))
}

