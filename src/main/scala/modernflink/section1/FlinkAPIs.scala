package modernflink.section1

import org.apache.flink.streaming.api.*
import org.apache.flink.api.*
import org.apache.flink.api.serializers.*
import org.apache.flink.api.common.typeinfo.TypeInformation
import scala.io.Source
import modernflink.model.UserAction
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStream.Collector
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util

class UserActionProcessFunction() extends KeyedProcessFunction[String, UserAction, (String, String, String)] {

  override def processElement(value: UserAction, ctx: KeyedProcessFunction[String, UserAction, (String, String, String)]#Context,
                              out: util.Collector[(String, String, String)]): Unit = out.collect((value.action, value.userid, value.name))
}

object FlinkAPIs {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val inputFile = env.readTextFile("src/main/resources/UserAction.txt")

  val userActionData = inputFile.map(UserAction.fromString)

  def lambdaDemo(): Unit = {

    // Lambda Function

    val userActionLambdaFunction: DataStream[(String, String, String)] = userActionData
      .keyBy(_.action)
      .map(data => (data.action, data.userid, data.name))

    userActionLambdaFunction.print()
    env.execute()
  }

  // Process Function
  def processFunctionDemo(): Unit = {
    val userActionStream1: DataStream[(String, String, String)] = userActionData
      .keyBy(_.action)
      .process(new UserActionProcessFunction())

    userActionStream1.print()
    env.execute()
  }

  // Rich Function
  def richFunctionDemo(): Unit = {
    val userActionStream2: DataStream[(String, String, String)] = userActionData
      .keyBy(_.action)
      .map(new RichMapFunction[UserAction, (String, String, String)] {
        override def map(value: UserAction): (String, String, String) = {
          (value.action, value.userid, value.name)
        }

        override def open(parameters: Configuration): Unit =
          println("MapFunction processing starts")


        override def close(): Unit =
          println("MapFunction Processing ends")
      }
      )
    userActionStream2.print()
    env.execute()
  }

  def main(args: Array[String]): Unit = {
    //    lambdaDemo()
    //    processFunctionDemo()
    richFunctionDemo()

  }
}
