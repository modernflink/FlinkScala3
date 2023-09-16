package modernflink.section1

import modernflink.model.HumidityReading
import org.apache.flinkx.api.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flinkx.api.serializers.*
import modernflink.model.SubscriptionEventsGenerator

@main def readDataSource() =
    val env = StreamExecutionEnvironment.getExecutionEnvironment

  // 1. read from a collection
    val testStreamOne = env.fromCollection(
      Seq(
        HumidityReading("Flagstaff", 1686122515, 58),
        HumidityReading("Flagstaff", 1686208915, 59)
      )
    )
//    testStreamOne.print("OUTPUTONE").setParallelism(2)

  // 2. read from a file
    val testStream2 = env.readTextFile("src/main/resources/Humidity.txt")
      .map(HumidityReading.fromString)

//    testStream2.print()

  // 3. read from a socket
    val testStreamThree = env.socketTextStream("127.0.0.1", 1235)
    testStreamThree.print()
    env.execute()