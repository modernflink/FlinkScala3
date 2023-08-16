package modernflink.section1

import org.apache.flink.streaming.api.*
import org.apache.flink.api.{java as j, *}
import modernflink.model.HumidityReading
import org.apache.flink.streaming.api.environment
import org.apache.flink.api.serializers.*

import _root_.java.time.ZonedDateTime
import _root_.java.util.Collection
import _root_.java.time.{Instant, ZoneId, ZonedDateTime}
import _root_.java.time.format.DateTimeFormatter
import _root_.java.util.Locale
import modernflink.model.DataGenerator.{SubscriptionEvent, SubscriptionEventsGenerator}
import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.io.Source

import Given.given

object Given:
  given instantTypeInfo: TypeInformation[Instant] = TypeInformation.of(classOf[Instant])

object ReadDataSource {
  // create an API entry point
  def main(args: Array[String]): Unit = {
    // get execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 1. stream from a collection
    val testStreamOne = env.fromCollection(Array[HumidityReading](
      HumidityReading("Flagstaff", 1688241216, 0.59),
      HumidityReading("Phoenix", 1688241216, 0.47),
      HumidityReading("Tucson", 1685172115, 0.44)
    ))

    testStreamOne.print("OutputStream1").setParallelism(3)

    // 2. stream from a file
    val testStreamTwo = env.readTextFile("src/main/resources/Humidity.txt")
//    testStreamTwo.map(HumidityReading.fromString).print("OutputStream2").setParallelism(3)

    // 3. stream from socket
    val testStreamThree = env.socketTextStream("127.0.0.1", 1235)
//    testStreamThree.print("OutputStream3")

    // 4. read from a data generator
    val testStreamFour: DataStream[SubscriptionEvent] = env.addSource(new SubscriptionEventsGenerator(
      sleepSeconds = 1,
      startTime = Instant.parse("2023-08-13T00:00:00.00Z"))
    )
    testStreamFour.print()

    env.execute()
  }
}
