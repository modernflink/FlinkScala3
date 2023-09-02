package modernflink.section1

import org.apache.flink.streaming.api.*
import org.apache.flink.api.*
import org.apache.flink.api.serializers.*
import org.apache.flink.api.common.typeinfo.TypeInformation
import scala.io.Source
import modernflink.model.{HumidityReading, TemperatureReading}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStream.Collector
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util

object MultipleStream:

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val inputFile1 = env.readTextFile("src/main/resources/Humidity.txt")
  val humidityDataStream = inputFile1
    .map(HumidityReading.fromString)
    .keyBy(_.location)

  val inputFile2 = env.readTextFile("src/main/resources/Temperature.txt")
  val temperatureDataStream = inputFile2
    .map(TemperatureReading.fromString)
    .keyBy(_.location)

  val inputFile3 = env.readTextFile("src/main/resources/MoreHumidityData.txt")
  val anotherDataStream = inputFile3
    .map(HumidityReading.fromString)
    .keyBy(_.location)

  // Union: combining two data streams with the same data structure
  def unionExample(): Unit =

    val unionedHumidityData: DataStream[HumidityReading] = humidityDataStream
      .union(anotherDataStream)

//    unionedHumidityData.print()
    env.execute()

  // Connect: combining two or more data streams with the same or different data structure
  def connectExample(): Unit =

    val humidityAndTemperatureData: ConnectedStreams[HumidityReading, TemperatureReading] =
      humidityDataStream
        .connect(temperatureDataStream)

    val outputConnectedStream: DataStream[(String, String)] = humidityAndTemperatureData
      .map(
        (value: HumidityReading) => value.location -> s"Humidity on ${value.formatTime("yyyy-MM-dd")} is ${value.humidity}",
        (value: TemperatureReading) => value.location -> s"Highest and lowest temperature on ${value.formatTime("yyyy-MM-dd")} are ${value.max} and ${value.min}"
      )
      .keyBy(_._1)


    outputConnectedStream.print().setParallelism(4)
    env.execute()

  def main(args: Array[String]): Unit =
    unionExample()
    connectExample()
