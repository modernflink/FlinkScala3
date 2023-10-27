package modernflink.section2

import modernflink.model.{AboveAverage, Average, BelowAverage, HumidityLevel, HumidityReading}
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.triggers.{CountTrigger, PurgingTrigger}
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, Window}
import org.apache.flink.util.Collector
import org.apache.flinkx.api.function.WindowFunction
import org.apache.flinkx.api.serializers.*
import org.apache.flinkx.api.StreamExecutionEnvironment
import java.time.{Duration, Instant}

@main def globalWindowDemo() =
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val inputFile = env.readTextFile("src/main/resources/Humidity.txt")
  val humidityData = inputFile.map(HumidityReading.fromString)

