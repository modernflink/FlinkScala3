package modernflink.section2

import modernflink.model.HumidityReading
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{CountTrigger, PurgingTrigger}
import org.apache.flink.util.Collector
import org.apache.flinkx.api.serializers.*
import org.apache.flinkx.api.StreamExecutionEnvironment

import java.time.{Duration, Instant}
import scala.jdk.CollectionConverters.*

object ListState:

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val inputFile = env.readTextFile("src/main/resources/Humidity.txt")
  val humidityData = inputFile.map(HumidityReading.fromString)

  def main(args: Array[String]): Unit =
    listStateDemo()

  private def listStateDemo(): Unit =
    // store all temperature change per location
    val humidityChangeStream = humidityData
      .keyBy(_.location)
      .process(new KeyedProcessFunction[String, HumidityReading, String] {
        // create state
        var humidityOutputStream: ListState[HumidityReading] = _

        // initialize state
        override def open(parameter: Configuration): Unit = {
          humidityOutputStream = getRuntimeContext.getListState(
            new ListStateDescriptor[HumidityReading](
              "humidityChangeOutputStream",
              classOf[HumidityReading]
            )
          )
        }

        override def processElement(
            value: HumidityReading,
            ctx: KeyedProcessFunction[String, HumidityReading, String]#Context,
            out: Collector[String]
        ): Unit = {

          humidityOutputStream.add(value)
          val humidityRecords: Iterable[HumidityReading] =
            humidityOutputStream.get().asScala.toList
          // keep the list bounded and clear the state when the condition is met
          if humidityRecords.size > 10 then humidityOutputStream.clear()

          out.collect(s"${value.location} - ${humidityRecords.mkString(",")}")
        }
      })
    humidityChangeStream.print()
    env.execute()
