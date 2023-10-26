package modernflink.section1

import modernflink.model.HumidityReading
import org.apache.flinkx.api.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flinkx.api.serializers.*
import modernflink.model.SubscriptionEvent
import modernflink.model.SubscriptionEventsGenerator

import java.time.Instant

@main def readDataSource() =
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  // 1. read from a collection
  val testStreamOne = env.fromCollection(Seq(
    HumidityReading("Flagstaff", 1686122515, 58),
    HumidityReading("Flagstaff", 1686208915, 59)
  ))

  testStreamOne.print("OutputStream1").setParallelism(2)

  env.execute()

  // 2. stream from a file
  val testStream2 = env
    .readTextFile("src/main/resources/Humidity.txt")
    .map(HumidityReading.fromString)

  testStream2.print()
//  env.execute()

   // 3. stream from a socket
  val testStreamThree = env.socketTextStream("127.0.0.1", 1235)
//  testStreamThree.print()
//  env.execute()

  // 4. read from a data generator
  given instantTypeInfo: TypeInformation[Instant] = BasicTypeInfo.INSTANT_TYPE_INFO
  val genEvents = SubscriptionEventsGenerator(
    sleepSeconds = 1,
    startTime = Instant.parse("2023-08-13T00:00:00.00Z")
  )

  val testStreamFour: DataStream[SubscriptionEvent] = env.addSource(genEvents)
  testStreamFour.print()
//  env.execute()