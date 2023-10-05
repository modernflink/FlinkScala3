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

import java.time.{Duration, Instant}

class CountDepositCheckpoint
    extends KeyedProcessFunction[String, Deposit, String]
    with CheckpointedFunction
    with CheckpointListener:

  var depositStateCounter: ValueState[Int] = _

  override def processElement(value: Deposit,
                              ctx: KeyedProcessFunction[String, Deposit, String]#Context,
                              out: Collector[String]): Unit =
    if (depositStateCounter.value() == null.asInstanceOf[Int]) then
      depositStateCounter.update(1) // default value
    val depositCountByCurrency = depositStateCounter.value()
    depositStateCounter.update(depositCountByCurrency + 1)
    out.collect(s"Total count of deposits in ${value.currency}: ${depositCountByCurrency}")

  override def initializeState(context: FunctionInitializationContext): Unit =
    depositStateCounter = context.getKeyedStateStore.getState(new ValueStateDescriptor[Int]("count state", classOf[Int]))

  override def snapshotState(context: FunctionSnapshotContext): Unit =
    println(s"Checkpoint at ${context.getCheckpointTimestamp}")

  override def notifyCheckpointComplete(checkpointId: Long): Unit = ()

  override def notifyCheckpointAborted(checkpointId: Long): Unit = ()

@main def checkpointDemo(): Unit =
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  // Set a checkpoint every 1 second
  env.enableCheckpointing(3000)
  // Set checkpoint storage
  env.getCheckpointConfig.setCheckpointStorage("file:///home/migs/Documents/FlinkCourse/CheckpointStorage")

  val depositData = env
    .addSource(
      new DepositEventGenerator(
        sleepSeconds = 1,
        startTime = Instant.parse("2023-08-13T00:00:00.00Z")
      )
    )

  val countDepositStream = depositData
    .keyBy(_.currency)
    .process(new CountDepositCheckpoint)

  countDepositStream.print()
  env.execute()






