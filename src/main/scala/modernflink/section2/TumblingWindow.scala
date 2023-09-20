package modernflink.section2

import modernflink.model.{Deposit, DepositEventGenerator}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{TimeWindow, Window}
import org.apache.flink.util.Collector
import org.apache.flinkx.api.{DataStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flinkx.api.function.WindowFunction
import org.apache.flinkx.api.serializers.*

import java.time.{Duration, Instant}

given instantTypeInfo: TypeInformation[Instant] =
  TypeInformation.of(classOf[Instant])

@main def tumblingWindowDemo() =

  val env = StreamExecutionEnvironment.getExecutionEnvironment
