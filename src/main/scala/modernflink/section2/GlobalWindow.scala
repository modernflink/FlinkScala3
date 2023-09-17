package modernflink.section2

import scalabackup.modelbackup.*
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
  
