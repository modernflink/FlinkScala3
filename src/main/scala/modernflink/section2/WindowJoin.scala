package modernflink.section2

import org.apache.flink.streaming.api.*
import org.apache.flink.api.StreamExecutionEnvironment
import org.apache.flink.api.serializers.*
import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.io.Source
import org.apache.flink.api.common.eventtime.{
  SerializableTimestampAssigner,
  WatermarkStrategy
}
import org.apache.flink.api.function.{AllWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.{
  GlobalWindows,
  SlidingEventTimeWindows,
  TumblingEventTimeWindows,
  TumblingProcessingTimeWindows
}
import org.apache.flink.streaming.api.windowing.triggers.{
  CountTrigger,
  PurgingTrigger
}
import org.apache.flink.streaming.api.windowing.windows.{
  GlobalWindow,
  TimeWindow,
  Window
}
import org.apache.flink.util.Collector

import java.time.{Duration, Instant}
import org.apache.flink.streaming.api.windowing.time.Time
import Given.given
import modernflink.model.PurchaseHistory
import modernflink.model.UserAction

object WindowJoin:

  def main(args: Array[String]): Unit =

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val inputFile = env.readTextFile("src/main/resources/UserAction.txt")
    val inputData = inputFile.map(UserAction.fromString)

    val userAction = inputData
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness(java.time.Duration.ofMillis(1000))
          .withTimestampAssigner(new SerializableTimestampAssigner[UserAction] {
            override def extractTimestamp(
                element: UserAction,
                recordTimestamp: Long
            ) =
              element.timestamp
          })
      )

    val inputFile2 = env.readTextFile("src/main/resources/PurchaseDetail.txt")
    val inputData2 = inputFile2.map(PurchaseHistory.fromString)

    val purchaseHistory = inputData2
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness(java.time.Duration.ofMillis(1000))
          .withTimestampAssigner(
            new SerializableTimestampAssigner[PurchaseHistory] {
              override def extractTimestamp(
                  element: PurchaseHistory,
                  recordTimestamp: Long
              ) =
                element.timestamp
            }
          )
      )

    val windowJoinStreams = userAction
      .join(purchaseHistory)
      .where(userAction => userAction.userid)
      .equalTo(purchaseHistory => purchaseHistory.userid)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .apply((userAction, purchaseHistory) =>
        s"${userAction.name} spent USD${purchaseHistory.amount} at ${purchaseHistory.formatTime()}."
      )

    windowJoinStreams.print()
    env.execute()
