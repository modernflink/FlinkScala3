package modernflink.section1

import modernflink.model.UserAction
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flinkx.api.serializers.*
import org.apache.flinkx.api.*

val env = StreamExecutionEnvironment.getExecutionEnvironment
val inputFile = env.readTextFile("src/main/resources/UserAction.txt")
val userActionData = inputFile.map(UserAction.fromString)
