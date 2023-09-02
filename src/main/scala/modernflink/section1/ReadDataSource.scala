package modernflink.section1

import modernflink.model.HumidityReading
import modernflink.model.SubscriptionEventsGenerator
import org.apache.flink.api.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.serializers.*
import java.time.Instant

@main def readDataSource() =

  // get execution environment
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  // 1. stream from a collection
  val testStreamOne = env.fromCollection(
    Array[HumidityReading](
      HumidityReading("Flagstaff", 1688241216, 0.59),
      HumidityReading("Phoenix", 1688241216, 0.47),
      HumidityReading("Tucson", 1685172115, 0.44)
    )
  )

  testStreamOne.print("OutputStream1").setParallelism(3)

  // 2. stream from a file
  val inputFile = env.readTextFile("src/main/resources/Humidity.txt")
  val testStreamTwo = inputFile.map(HumidityReading.fromString)
  testStreamTwo.print("OutputStream2").setParallelism(3)

  // 3. stream from socket
//  val testStreamThree = env.socketTextStream("127.0.0.1", 1235)
//  testStreamThree.print("OutputStream3")

  // 4. read from a data generator
  given instantTypeInfo: TypeInformation[Instant] =
    TypeInformation.of(classOf[Instant])

  val testStreamFour = env.addSource(
    SubscriptionEventsGenerator(
      sleepSeconds = 1,
      startTime = Instant.parse("2023-08-13T00:00:00.00Z")
    )
  )
  testStreamFour.print()

  env.execute()
