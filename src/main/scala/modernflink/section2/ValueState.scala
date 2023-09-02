package modernflink.section2

import modernflink.model.HumidityReading
import org.apache.flink.api.common.state.{
  ListState,
  ListStateDescriptor,
  MapState,
  MapStateDescriptor,
  ValueState,
  ValueStateDescriptor
}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{CountTrigger, PurgingTrigger}
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, Window}
import org.apache.flink.util.Collector
import org.apache.flinkx.api.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flinkx.api.function.{AllWindowFunction, WindowFunction}
import org.apache.flinkx.api.serializers.*

import java.time.{Duration, Instant}

class HumidityAlert() extends KeyedProcessFunction[String, HumidityReading, String]:

  // call .value to get current state
  // call .update to get newValue and override current stat
  var lastReading: ValueState[Double] = _
  var currentTime: ValueState[Long] = _

  // initialize state
  override def open(parameters: Configuration): Unit =
    lastReading = getRuntimeContext.getState(
      new ValueStateDescriptor[Double]("lastReading", classOf[Double])
    )
    currentTime = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("currentTime", classOf[Long])
    )

  override def processElement(
      value: HumidityReading,
      ctx: KeyedProcessFunction[String, HumidityReading, String]#Context,
      out: Collector[String]
  ): Unit =

    val previousHumidity = lastReading.value()

    val currentTimer = currentTime.value()
    // humidity rises too much over a one minute
    if value.humidity > previousHumidity + 3 then
      val customTimer = ctx.timerService().currentProcessingTime() + 60
      ctx.timerService().registerProcessingTimeTimer(customTimer)
      currentTime.update(customTimer)
      ctx.output(
        OutputTag[String]("humidity increases"),
        s"${ctx.getCurrentKey} humidity increased from - ${lastReading
            .value()} to ${value.humidity} by ${value.humidity - lastReading.value()}"
      )
    else if value.humidity < previousHumidity || (previousHumidity - value.humidity) == value.humidity
    then
      ctx.timerService().deleteProcessingTimeTimer(currentTimer)
      currentTime.clear()
    lastReading.update(value.humidity)

  override def onTimer(
      timestamp: Long,
      ctx: KeyedProcessFunction[String, HumidityReading, String]#OnTimerContext,
      out: Collector[String]
  ): Unit =
    out.collect(s"timer ${ctx.getCurrentKey} - Humidity increases")

object ValueState:
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val inputFile = env.readTextFile("src/main/resources/Humidity.txt")
  val humidityData = inputFile.map(HumidityReading.fromString)

  def main(args: Array[String]): Unit =
    valueStateDemo()

  def valueStateDemo(): Unit =

    val humidityAlertStream = humidityData
      .keyBy(_.location)
      .process(new HumidityAlert())

    humidityAlertStream
      .getSideOutput(OutputTag[String]("humidity increases"))
      .print()
    humidityAlertStream.print()
    env.execute()
