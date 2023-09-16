package scalabackup.modelbackup

import scalabackup.modelbackup.BankingEventGenerator.DepositEventGenerator
import scalabackup.modelbackup.DepositEventGenerator.{genCurrency, genUser}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flinkx.api.{DataStream, StreamExecutionEnvironment}
import org.apache.flinkx.api.serializers.*

import java.time.Instant
import java.util.UUID
import scala.util.Random

given instantTypeInfo: TypeInformation[Instant] =
  TypeInformation.of(classOf[Instant])

object BankingEventGenerator:

  def main(args: Array[String]): Unit =
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val testStream: DataStream[Deposit] =
      env.addSource(DepositEventGenerator(sleepSeconds = 1, startTime = Instant.parse("2023-08-13T00:00:00.00Z")))
    testStream.print()
    env.execute()

  case class Deposit(userId: String, time: Instant, amount: Int, currency: String)

  class DepositEventGenerator(sleepSeconds: Long, startTime: Instant = Instant.now()) extends SourceFunction[Deposit]:

    import DepositEventGenerator.*

    private var running = true
    private var maxEvents = 15

    override def run(ctx: SourceFunction.SourceContext[Deposit]): Unit =
      run(0, ctx)

    override def cancel(): Unit =
      running = false

    private def run(start: Long, ctx: SourceFunction.SourceContext[Deposit]): Unit =
      if running && maxEvents > 0 then
        maxEvents -= 1
        ctx.collect(emitEvents(start))
        Thread.sleep(sleepSeconds * 1000)
        run(start + 1, ctx)

    private def emitEvents(num: Long) =
      Deposit(genUser, startTime.plusSeconds(num), scala.util.Random.nextInt(10000000), genCurrency)

object DepositEventGenerator:

  val users: Seq[String] = Seq(
    "Rob",
    "David",
    "Charlie",
    "Frank",
    "Peggy",
    "Max",
    "David",
    "Emma",
    "Chris"
  )
  val currency: Seq[String] = Seq("USD", "Euro", "Yen")

  def genUser: String = users(scala.util.Random.nextInt(users.length))

  def genCurrency: String = currency(scala.util.Random.nextInt(currency.length))

