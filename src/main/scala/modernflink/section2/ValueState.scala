package modernflink.section2

import modernflink.model.{Deposit, DepositEventGenerator}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flinkx.api.{DataStream, StreamExecutionEnvironment}
import org.apache.flinkx.api.function.StatefulFunction
import org.apache.flinkx.api.serializers.*

import java.time.{Duration, Instant}

@main def valueStateDemo(): Unit =

  val env = StreamExecutionEnvironment.getExecutionEnvironment

