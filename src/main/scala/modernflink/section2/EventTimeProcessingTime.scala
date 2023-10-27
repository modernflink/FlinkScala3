package modernflink.section2

import modernflink.model.{SubscriptionEventsGenerator, SubscriptionEvent}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.apache.flinkx.api.serializers.*
import org.apache.flinkx.api.StreamExecutionEnvironment
import java.time.{Duration, Instant}
import modernflink.model.instantTypeInfo

val env = StreamExecutionEnvironment.getExecutionEnvironment

