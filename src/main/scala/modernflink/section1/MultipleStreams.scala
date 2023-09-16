package modernflink.section1

import org.apache.flinkx.api.serializers.*
import org.apache.flinkx.api.{ConnectedStreams, DataStream, KeyedStream,StreamExecutionEnvironment}
import modernflink.model.HumidityReading
import modernflink.model.TemperatureReading

@main def multipleStreamsDemo() =
  val env = StreamExecutionEnvironment.getExecutionEnvironment

val humidityDataStream: KeyedStream[HumidityReading, String] = env
  .readTextFile("src/main/resources/Humidity.txt")
  .map(HumidityReading.fromString)
  .keyBy(_.location)

val anotherDataStream: KeyedStream[HumidityReading, String] = env
  .readTextFile("src/main/resources/MoreHumidityData.txt")
  .map(HumidityReading.fromString)
  .keyBy(_.location)

val temperatureDataStream: KeyedStream[TemperatureReading, String] = env
  .readTextFile("src/main/resources/Temperature.txt")
  .map(TemperatureReading.fromString)
  .keyBy(_.location)

// Union
def unionExample(): Unit =

  val unionedHumidityData: DataStream[HumidityReading] = humidityDataStream.union(anotherDataStream)

//  unionedHumidityData.print()
  env.execute()

// Connect
def connectExample(): Unit =

  val humidityAndTemperatureData: ConnectedStreams[HumidityReading, TemperatureReading] =
    humidityDataStream.connect(temperatureDataStream)

  val outputConnectedStream: DataStream[(String, String)] =
    humidityAndTemperatureData
      .map(
        (value: HumidityReading) =>
          value.location ->
            s"Humidity on ${value.formatTime("yyyy-MM-dd")} is ${value.humidity}",
        (value: TemperatureReading) =>
          value.location ->
            s"Highest and lowest temperature on ${value.formatTime("yyyy-MM-dd")} are ${value.max} and ${value.min}"
      )
      .keyBy(_._1)

  outputConnectedStream.print()
  env.execute()



