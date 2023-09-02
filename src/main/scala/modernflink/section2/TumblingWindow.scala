package modernflink.section2

import modernflink.model.BankingEventGenerator
import modernflink.model.BankingEventGenerator.{Deposit, DepositEventGenerator}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.windowing.assigners.{
  GlobalWindows,
  SlidingEventTimeWindows,
  TumblingEventTimeWindows,
  TumblingProcessingTimeWindows
}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{CountTrigger, PurgingTrigger}
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow, Window}
import org.apache.flink.util.Collector
import org.apache.flinkx.api.{DataStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flinkx.api.function.WindowFunction
import org.apache.flinkx.api.serializers.*

import java.time.{Duration, Instant}

given instantTypeInfo: TypeInformation[Instant] =
  TypeInformation.of(classOf[Instant])

class DepositByTumblingWindow extends WindowFunction[Deposit, String, String, TimeWindow]:
  override def apply(key: String, window: TimeWindow, input: Iterable[Deposit], out: Collector[String]): Unit =
    out.collect(s"$key ${window.getStart} to ${window.getEnd}: $input")

object TumblingWindow:

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val depositData =
    env
      .addSource(DepositEventGenerator(sleepSeconds = 1, startTime = Instant.parse("2023-08-13T00:00:00.00Z")))
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness(java.time.Duration.ofMillis(0))
          .withTimestampAssigner(new SerializableTimestampAssigner[Deposit] {
            override def extractTimestamp(element: Deposit, recordTimestamp: Long) =
              element.time.toEpochMilli
          })
      )

  def main(args: Array[String]): Unit =
    createTumblingWindowStream()

  private def createTumblingWindowStream(): Unit =
    val depositByWindowStream: WindowedStream[Deposit, String, TimeWindow] =
      depositData
        .keyBy(_.currency)
        .window(TumblingEventTimeWindows.of(Time.seconds(5)))

    val tumblingWindowStream: DataStream[String] =
      depositByWindowStream
        .apply(new DepositByTumblingWindow)

    tumblingWindowStream.print()
    env.execute()
