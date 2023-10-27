package modernflink.section2

import modernflink.model.{Deposit, DepositEventGenerator}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{TimeWindow, Window}
import org.apache.flink.util.Collector
import org.apache.flinkx.api.serializers.*
import org.apache.flinkx.api.StreamExecutionEnvironment
import org.apache.flinkx.api.function.AllWindowFunction

import java.time.{Duration, Instant}


@main def slidingWindowDemo() =

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val depositData = env
    .addSource(
      new DepositEventGenerator(
        sleepSeconds = 1,
        startTime = Instant.parse("2023-08-13T00:00:00.00Z")
      )
    )
    .assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness(java.time.Duration.ofMillis(0))
        .withTimestampAssigner(new SerializableTimestampAssigner[Deposit] {
          override def extractTimestamp(
                                         element: Deposit,
                                         recordTimestamp: Long
                                       ) =
            element.time.toEpochMilli
        })
    )

