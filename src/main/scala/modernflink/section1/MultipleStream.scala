package modernflink.section1

import modernflink.model.{HumidityReading, TemperatureReading}
import org.apache.flinkx.api.serializers.*
import org.apache.flinkx.api.{ConnectedStreams, DataStream, KeyedStream, StreamExecutionEnvironment}

object MultipleStream:

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  val humidityDataStream: KeyedStream[HumidityReading, String] =
    env
      .readTextFile("src/main/resources/Humidity.txt")
      .map(HumidityReading.fromString)
      .keyBy(_.location)

  val temperatureDataStream: KeyedStream[TemperatureReading, String] =
    env
      .readTextFile("src/main/resources/Temperature.txt")
      .map(TemperatureReading.fromString)
      .keyBy(_.location)

  val anotherDataStream: KeyedStream[HumidityReading, String] =
    env
      .readTextFile("src/main/resources/MoreHumidityData.txt")
      .map(HumidityReading.fromString)
      .keyBy(_.location)

  def main(args: Array[String]): Unit =
    unionExample()
    connectExample()

  // Union: combining two data streams with the same data structure
  def unionExample(): Unit =

    val unionedHumidityData: DataStream[HumidityReading] = humidityDataStream
      .union(anotherDataStream)

    unionedHumidityData.print()
    env.execute()

  // Connect: combining two or more data streams with the same or different data structure
  def connectExample(): Unit =

    val humidityAndTemperatureData: ConnectedStreams[HumidityReading, TemperatureReading] =
      humidityDataStream
        .connect(temperatureDataStream)

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

    outputConnectedStream.print().setParallelism(4)
    env.execute()
