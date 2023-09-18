package scalabackup.section2

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

case class KeyedHumidityLevel(location: String, humidityLevel: HumidityLevel)

class HumidityByGlobalWindow
    extends WindowFunction[
      HumidityReading,
      KeyedHumidityLevel,
      String,
      GlobalWindow
    ]:
  override def apply(
      key: String,
      window: GlobalWindow,
      unsorted: Iterable[HumidityReading],
      out: Collector[KeyedHumidityLevel]
  ): Unit =
    val input = unsorted.toIndexedSeq.sortBy(_.timestamp)
    val averageByGlobalWindow = input.map(_.humidity).sum / input.size
    val last = input.last
    val humidityLevel: HumidityLevel =
      if last.humidity > averageByGlobalWindow then AboveAverage
      else if last.humidity == averageByGlobalWindow then Average
      else BelowAverage
    out.collect(KeyedHumidityLevel(key, humidityLevel))

val env = StreamExecutionEnvironment.getExecutionEnvironment
val inputFile = env.readTextFile("src/main/resources/Humidity.txt")
val humidityData = inputFile.map(HumidityReading.fromString)

// Count how many days are below/above average humidity
@main def mapStateDemo() =
  
  val humidityLevelStream: DataStream[KeyedHumidityLevel] = humidityData
    .keyBy(_.location)
    .window(GlobalWindows.create())
    .trigger(
      PurgingTrigger.of(CountTrigger.of[Window](7))
    ) // Every 7 elements, Flink will create a new Global Window, then clearing the window
    .apply(new HumidityByGlobalWindow)

  val processedHumidityLevel = humidityLevelStream
    .keyBy(_.location)
    .process(
      new KeyedProcessFunction[String, KeyedHumidityLevel, String] {

        // create the state
        var humidityLevelCount: MapState[HumidityLevel, Long] = _

        // initialize the state
        override def open(parameters: Configuration): Unit = {
          humidityLevelCount = getRuntimeContext.getMapState(
            new MapStateDescriptor[HumidityLevel, Long](
              "humidityLevelCounter",
              classOf[HumidityLevel],
              classOf[Long]
            )
          )
        }

        override def processElement(
            value: KeyedHumidityLevel,
            ctx: KeyedProcessFunction[
              String,
              KeyedHumidityLevel,
              String
            ]#Context,
            out: Collector[String]
        ): Unit = {

          // update the state
          if humidityLevelCount.contains(value.humidityLevel) then {
            val previousCount = humidityLevelCount.get(value.humidityLevel)
            val updatedCount = previousCount + 1
            humidityLevelCount.put(value.humidityLevel, updatedCount)
          } else {
            humidityLevelCount.put(value.humidityLevel, 1)
          }

          // push the output stream
          out.collect(
            s"${ctx.getCurrentKey} - ${humidityLevelCount.entries().asScala.mkString(",")}"
          )
        }
      }
    )
  humidityLevelStream.print()
  env.execute()
