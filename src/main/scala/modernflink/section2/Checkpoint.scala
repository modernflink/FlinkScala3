package modernflink.section2

import modernflink.model.{Deposit, DepositEventGenerator}
import org.apache.flink.api.common.state.{CheckpointListener, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flinkx.api.{DataStream, StreamExecutionEnvironment}
import org.apache.flinkx.api.serializers.*

import java.nio.file.Paths
import java.time.{Duration, Instant}

@main def checkpointDemo(): Unit =
  val env = StreamExecutionEnvironment.getExecutionEnvironment



  val depositData = env
    .addSource(
      new DepositEventGenerator(
        sleepSeconds = 1,
        startTime = Instant.parse("2023-08-13T00:00:00.00Z")
      )
    )
