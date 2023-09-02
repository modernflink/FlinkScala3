package modernflink.section2

import org.apache.flink.streaming.api.*
import org.apache.flink.api.StreamExecutionEnvironment
import org.apache.flink.api.serializers.*
import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.io.Source
import modernflink.model.{AboveAverage, Average, BelowAverage, HumidityLevel, HumidityReading}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{GlobalWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.triggers.{CountTrigger, PurgingTrigger}
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, Window}
import org.apache.flink.util.Collector

import java.time.{Duration, Instant}

class GlobalWindowDemo extends WindowFunction[HumidityReading, String, String, GlobalWindow]{
  override def apply(key: String, window: GlobalWindow, input: Iterable[HumidityReading], out: Collector[String]): Unit = {
    val averageByGlobalWindow = input.map(_.humidity).sum / input.size
    val last = input.last
    val humidityLevel: HumidityLevel = {
      if last.humidity > averageByGlobalWindow then {
        AboveAverage
      } else if last.humidity == averageByGlobalWindow then {
        Average
      } else {
        BelowAverage
      }
    }
    out.collect(s"$key - ${input.map(_.timestamp)} - $averageByGlobalWindow - $last - $humidityLevel")
  }
}

object GlobalWindow {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val inputFile = env.readTextFile("src/main/resources/Humidity.txt")
  val humidityData = inputFile.map(HumidityReading.fromString)

  def averageOutput(): Unit = {
    val outputGlobalWindowStream = humidityData
      .keyBy(_.location)
      .window(GlobalWindows.create())
      .trigger(PurgingTrigger.of(CountTrigger.of[Window](5))) // Every 5 elements, Flink will create a new Global Window, then clearing the window
      .apply(new GlobalWindowDemo)

    outputGlobalWindowStream.print()
    env.execute()
  }

    def main(args: Array[String]): Unit = {
    averageOutput()
    }
}
