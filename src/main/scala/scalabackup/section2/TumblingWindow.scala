package scalabackup.section2

import modernflink.model.{Deposit, DepositEventGenerator}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{TimeWindow, Window}
import org.apache.flink.util.Collector
import org.apache.flinkx.api.{DataStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flinkx.api.function.WindowFunction
import org.apache.flinkx.api.serializers.*

import java.time.{Duration, Instant}

given instantTypeInfo: TypeInformation[Instant] =
  TypeInformation.of(classOf[Instant])

class DepositByTumblingWindow extends WindowFunction[Deposit, String, String, TimeWindow]:
  override def apply(key: String, window: TimeWindow, input: Iterable[Deposit], out: Collector[String]): Unit =
    out.collect(s"$key ${window.getStart} to ${window.getEnd}: ${input.mkString(", ")}")

@main def tumblingWindowDemo() =

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val depositData =
    env
      .addSource(DepositEventGenerator(sleepSeconds = 1, startTime = Instant.parse("2023-08-13T00:00:00.00Z")))
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness(java.time.Duration.ofMillis(3))
          .withTimestampAssigner(new SerializableTimestampAssigner[Deposit] {
            override def extractTimestamp(element: Deposit, recordTimestamp: Long) =
              element.time.toEpochMilli
          })
      )

  val depositByWindowStream: WindowedStream[Deposit, String, TimeWindow] =
    depositData
      .keyBy(_.currency)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))

  val tumblingWindowStream: DataStream[String] =
    depositByWindowStream
      .apply(new DepositByTumblingWindow)

  tumblingWindowStream.print()
  env.execute()
