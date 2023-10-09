package modernflink.model

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flinkx.api.{DataStream, StreamExecutionEnvironment}
import org.apache.flinkx.api.serializers.*

import java.time.Instant
import java.util.UUID
import scala.annotation.tailrec
import scala.util.Random

given instantTypeInfo: TypeInformation[Instant] =
  TypeInformation.of(classOf[Instant])

@main def depositEventGeneratorDemo(): Unit =
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  val testStream: DataStream[Deposit] =
    env.addSource(DepositEventGenerator(sleepSeconds = 1, startTime = Instant.parse("2023-08-13T00:00:00.00Z")))
  testStream.print()
  env.execute()

case class Deposit(userId: String, time: Instant, amount: Int, currency: String)

class DepositEventGenerator(sleepSeconds: Long, startTime: Instant = Instant.now()) extends SourceFunction[Deposit]:

  private var running = true

  override def run(ctx: SourceFunction.SourceContext[Deposit]): Unit =
    myRun(0, 20, ctx)

  override def cancel(): Unit =
    running = false

  @tailrec
  private def myRun(
      start: Long,
      remainingEvents: Int,
      ctx: SourceFunction.SourceContext[Deposit]
  ): Unit =
    if running && remainingEvents > 0 then
      ctx.collect(emitEvents(start))
      Thread.sleep(sleepSeconds * 1000)
      myRun(start + 1, remainingEvents - 1, ctx)

  private def emitEvents(num: Long) =
    Deposit(
      DepositEventGenerator.genUser,
      startTime.plusSeconds(num),
      scala.util.Random.nextInt(10000000),
      DepositEventGenerator.genCurrency
    )

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
