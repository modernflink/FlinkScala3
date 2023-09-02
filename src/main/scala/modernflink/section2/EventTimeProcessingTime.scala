package modernflink.section2

import modernflink.model.{SubscriptionEvent, SubscriptionEventsGenerator}
import org.apache.flink.api.StreamExecutionEnvironment
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.serializers.*
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.*
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util
import org.apache.flink.util.Collector
import modernflink.section2.Given.instantTypeInfo

import java.time.{Duration, Instant}
import scala.io.Source
object EventTimeProcessingTime {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val subscriptionEvent = env.addSource(new SubscriptionEventsGenerator(
    sleepSeconds = 1,
    startTime = Instant.parse("2023-08-13T00:00:00.00Z")
  )
  )


  // Processing Time
  def processingTimeDemo(): Unit = {

    val eventStream1 = subscriptionEvent
      .keyBy(_.userId)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .apply(
        (userId, timeWindow, events, collector: Collector[(String, String)]) => {
          collector.collect(
            userId,
            events
              .map(e => s"Processing Time Window ${timeWindow.getStart}-${timeWindow.getEnd}: ${e.getClass.getSimpleName} at ${e.time}")
              .mkString(", ")
          )
        })

    eventStream1.print()
    env.execute()
  }

  // Event Time
  def eventTimeDemo(): Unit = {
    val withWatermarks = subscriptionEvent
      .assignTimestampsAndWatermarks(
        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10))
          .withTimestampAssigner(new SerializableTimestampAssigner[SubscriptionEvent] {
            override def extractTimestamp(element: SubscriptionEvent, recordTimestamp: Long) =
              element.time.toEpochMilli
          })
      )

    val userActionStream2 = withWatermarks
      .keyBy(_.userId)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .apply(
        (userId, timeWindow, events, collector: Collector[(String, String)]) => {
          collector.collect(
            (
              userId,
              events
                .map(e => s"Event Time Window ${timeWindow.getStart}-${timeWindow.getEnd}: ${e.getClass.getSimpleName}: ${e.time}")
                .mkString(", ")
            )
          )
        })

    userActionStream2.print()
    env.execute()
  }

  def main(args: Array[String]): Unit = {
//    processingTimeDemo()
    eventTimeDemo()
  }
}