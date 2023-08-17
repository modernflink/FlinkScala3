package section2

import org.apache.flink.streaming.api.*
import org.apache.flink.api.StreamExecutionEnvironment
import org.apache.flink.api.serializers.*
import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.io.Source
import modernflink.model.{AboveAverage, Average, BelowAverage, HumidityLevel, HumidityReading}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{GlobalWindows, SlidingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.triggers.{CountTrigger, PurgingTrigger}
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow, Window}
import org.apache.flink.util.Collector

import java.time.{Duration, Instant}
import modernflink.model.TemperatureReading
import org.apache.flink.streaming.api.windowing.time.Time

class TemperatureReadingByWindow extends WindowFunction[TemperatureReading, String, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[TemperatureReading], out: Collector[String]): Unit = {
    val highTemp = input.map(_.max).max
    val lowTemp = input.map(_.min).min
    out.collect(s"$key - ${input.map(_.formatTime("yyyy-MM-dd"))}")
  }
}
object SlidingWindow {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val inputFile2 = env.readTextFile("src/main/resources/Temperature.txt")
  val temperatureDataStream = inputFile2
    .map(TemperatureReading.fromString)
    .assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness(java.time.Duration.ofSeconds(0))
        .withTimestampAssigner(new SerializableTimestampAssigner[TemperatureReading] {
          override def extractTimestamp(element: TemperatureReading, recordTimestamp: Long) =
            element.timestamp
        })
    )

  def createSlidingWindowStream(): Unit = {
    val temperatureByWindowStream = temperatureDataStream
      .keyBy(_.location)
      .window(SlidingEventTimeWindows.of(Time.days(3), Time.days(1)))

    val slidingWindowStream = temperatureByWindowStream.apply(new TemperatureReadingByWindow)

    slidingWindowStream.print()
    env.execute()
  }

  def main(args: Array[String]): Unit = {
    createSlidingWindowStream()
  }
}
