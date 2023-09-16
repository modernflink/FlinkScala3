package scalabackup.section2

import scalabackup.modelbackup.HumidityReading
import org.apache.flinkx.api.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flinkx.api.serializers.*
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util
import org.apache.flink.util.Collector
import scalabackup.section2.Given.instantTypeInfo
import org.apache.flink.api.common.state.{CheckpointListener, ValueState, ValueStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flinkx.api.{OutputTag, StreamExecutionEnvironment}

import java.time.{Duration, Instant}
import scala.io.Source
object Given:
  given instantTypeInfo: TypeInformation[Instant] =
    TypeInformation.of(classOf[Instant])

case class HumidityAlertwithCheckPoint(threshhold: Long)
    extends KeyedProcessFunction[String, HumidityReading, String]
    with CheckpointedFunction
    with CheckpointListener:

  // call .value to get current state
  // call .update to get newValue and override current stat
  var lastReading: ValueState[Double] = _
  var currentTime: ValueState[Long] = _

  // initialize state
  override def initializeState(context: FunctionInitializationContext): Unit =

    lastReading = context.getKeyedStateStore.getState(
      new ValueStateDescriptor[Double]("lastReading", classOf[Double])
    )
    currentTime = context.getKeyedStateStore.getState(
      new ValueStateDescriptor[Long]("currentTime", classOf[Long])
    )

  // snapshotState is invoked when checkpoint is triggered
  override def snapshotState(context: FunctionSnapshotContext): Unit =
    println(s"Checkpoint at ${context.getCheckpointTimestamp}")

  override def notifyCheckpointComplete(checkpointId: Long): Unit = ()
  override def notifyCheckpointAborted(checkpointId: Long): Unit = ()

  override def processElement(
      value: HumidityReading,
      ctx: KeyedProcessFunction[String, HumidityReading, String]#Context,
      out: Collector[String]
  ): Unit =

    val previousHumidity = lastReading.value()
    lazy val lowHumidity = new OutputTag[String]("humidity increases")

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

object Checkpoint:

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  // set checkpoint intervals
  env.getCheckpointConfig.setCheckpointInterval(
    100
  ) // a checkpoint triggered every 100 ms
  // set checkpoint storage
  env.getCheckpointConfig.setCheckpointStorage(
    "file:///home/migs/Documents/FLinkCourse"
  )

  val inputFile = env.readTextFile("src/main/resources/Humidity.txt")
  val humidityData = inputFile
    .map(HumidityReading.fromString)

  def CheckpointDemo(): Unit =

    val humidityAlertStream = humidityData
      .keyBy(_.location)
      .process(HumidityAlertwithCheckPoint(5000))

    humidityAlertStream
      .getSideOutput(OutputTag[String]("humidity increases"))
      .print()
    humidityAlertStream.print()
    env.execute()

  def main(args: Array[String]): Unit =
    CheckpointDemo()
