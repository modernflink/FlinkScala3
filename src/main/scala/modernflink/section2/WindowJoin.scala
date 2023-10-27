package modernflink.section2

import modernflink.model.UserAction
import modernflink.model.PurchaseHistory
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flinkx.api.StreamExecutionEnvironment
import org.apache.flinkx.api.function.WindowFunction
import org.apache.flinkx.api.serializers.*

import java.time.Duration

@main def windowJoin() =
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val inputFile = env.readTextFile("src/main/resources/UserAction.txt")
  val inputData = inputFile.map(UserAction.fromString)


  val inputFile2 = env.readTextFile("src/main/resources/PurchaseDetail.txt")
  val inputData2 = inputFile2.map(PurchaseHistory.fromString)

