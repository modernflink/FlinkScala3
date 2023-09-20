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

class UserActionProcessFunction() extends KeyedProcessFunction[String, UserAction, (String, String, String)]:
  override def processElement(value: UserAction,
                              ctx: KeyedProcessFunction[String, UserAction, (String, String, String)]#Context,
                              out: Collector[(String, String, String)]
                             ): Unit = out.collect((value.action, value.userid, value.name))

// Lambda Function
def lambdaDemo(): Unit =
  val userActionLambdaFunction: DataStream[(String, String, String)] =
    userActionData
      .keyBy(_.action)
      .map(data => (data.action, data.userid, data.name))

  userActionLambdaFunction.print()
  env.execute()

// Process Function
def processFunctionDemo(): Unit =
  val userActionStreamProcessFunction: DataStream[(String, String, String)] = userActionData
    .keyBy(_.action)
    .process(new UserActionProcessFunction())

  userActionStreamProcessFunction.print()
  env.execute()

// Rich Function
def richFunctionDemo(): Unit =
  val userActionStreamRichFunction: DataStream[(String, String, String)] = userActionData
    .keyBy(_.action)
    .map(new RichMapFunction[UserAction, (String, String, String)]:
        override def map(value: UserAction): (String, String, String) = (value.action, value.userid, value.name)

        override def open(parameters: Configuration): Unit =
          println("MapFunction processing starts")

        override def close(): Unit =
          println("MapFunction processing ends")
      )

  userActionStreamRichFunction.print()
  env.execute()

@main def flinkAPIs(): Unit =
//  lambdaDemo()
//  processFunctionDemo()
  richFunctionDemo()