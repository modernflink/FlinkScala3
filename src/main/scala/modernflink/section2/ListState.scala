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

val env = StreamExecutionEnvironment.getExecutionEnvironment

val inputFile = env.readTextFile("src/main/resources/Humidity.txt")
val humidityData = inputFile.map(HumidityReading.fromString)



