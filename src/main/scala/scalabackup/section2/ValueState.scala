package scalabackup.section2

import scalabackup.modelbackup.HumidityReading
import org.apache.flink.api.common.state.{
  ValueState,
  ValueStateDescriptor
}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.apache.flinkx.api.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flinkx.api.serializers.*
import java.time.{Duration, Instant}

class HumidityAlert() extends KeyedProcessFunction[String, HumidityReading, String]:

  var lastReading: ValueState[Double] = _

  // initialize state
  override def open(parameters: Configuration): Unit =
    lastReading = getRuntimeContext.getState(ValueStateDescriptor[Double]("lastReading", classOf[Double]))

  override def processElement(
      value: HumidityReading,
      ctx: KeyedProcessFunction[String, HumidityReading, String]#Context,
      out: Collector[String]
  ): Unit =

    // call .value to get current state
    val previousHumidity = lastReading.value()

    // humidity rises more than 3%
    if value.humidity > previousHumidity + 3 then
      out.collect(
        s"${ctx.getCurrentKey} humidity increased from - ${lastReading.value()} to ${value.humidity} by ${value.humidity - lastReading.value()}"
      )
    // call .update to get newValue and override current stat
    lastReading.update(value.humidity)

@main def valueStateDemo(): Unit =

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val inputFile = env.readTextFile("src/main/resources/Humidity.txt")
  val humidityData = inputFile.map(HumidityReading.fromString)

  val humidityAlertStream = humidityData
    .keyBy(_.location)
    .process(HumidityAlert())

  humidityAlertStream
//    .getSideOutput(OutputTag[String]("humidity increases"))
    .print()
  humidityAlertStream.print()
  env.execute()
