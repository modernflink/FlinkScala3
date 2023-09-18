package modernflink.section2

import scalabackup.modelbackup.*
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{CountTrigger, PurgingTrigger}
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, Window}
import org.apache.flink.util.Collector
import org.apache.flinkx.api.*
import org.apache.flinkx.api.function.{AllWindowFunction, WindowFunction}
import org.apache.flinkx.api.serializers.*
import java.time.{Duration, Instant}
import scala.jdk.CollectionConverters.*

val env = StreamExecutionEnvironment.getExecutionEnvironment
val inputFile = env.readTextFile("src/main/resources/Humidity.txt")
val humidityData = inputFile.map(HumidityReading.fromString)

case class KeyedHumidityLevel(location: String, )