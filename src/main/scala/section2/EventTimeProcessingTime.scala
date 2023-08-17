package section2

import org.apache.flink.streaming.api.*
import org.apache.flink.api.StreamExecutionEnvironment
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.serializers.*
import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.io.Source
import modernflink.model.SubscriptionEventGenerator.{SubscriptionEvent, SubscriptionEventsGenerator}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util
import org.apache.flink.util.Collector
import java.time.{Duration, Instant}
import Given.given

object Given:
  given instantTypeInfo: TypeInformation[Instant] = TypeInformation.of(classOf[Instant])


object EventTimeProcessingTime {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val subscriptionEvent = env.addSource(new SubscriptionEventsGenerator(
    sleepSeconds = 1,
    startTime = Instant.parse("2023-08-13T00:00:00.00Z"))
  )

  // Processing Time
  def processingTimeDemo(): Unit = {

    val eventStream1 = subscriptionEvent
      .keyBy(_.userId)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(3)))
      .apply(
        (userId, timeWindow, events, collector: Collector[(String, String)]) => {
        collector.collect(userId, events.map(_.time).toString())
        })

    eventStream1.print()
    env.execute()
  }

  // Event Time
//  def evenTimeDemo(): Unit = {
//
//    subscriptionEvent
//      .assignTimestampsAndWatermarks(
//        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3))
//          .withTimestampAssigner(new SerializableTimestampAssigner[SubscriptionEvent] {
//            override def extractTimestamp(element: SubscriptionEvent, recordTimestamp: Long) =
//              element.time
//          })
//      )
//
//    val userActionStream2 = subscriptionEvent
//      .keyBy(_.userId)
//      .window(TumblingProcessingTimeWindows.of(Time.seconds(3)))
//      .apply(
//        (userId, timeWindow, events, collector: Collector[(String, Seq[String])]) => {
//          collector.collect((userId, events.map(_.eventNum).toSeq))
//        })
//
//    userActionStream2.print()
//    env.execute()
//  }

  def main(args: Array[String]): Unit = {
    processingTimeDemo()
//    evenTimeDemo()
  }
}
