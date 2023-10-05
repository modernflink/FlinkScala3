//package modernflink.section1
//
//import org.apache.flinkx.api.serializers.*
//import org.apache.flinkx.api.{ConnectedStreams, DataStream, KeyedStream, StreamExecutionEnvironment}
//import modernflink.model.{HumidityReading, LocalSummary, TemperatureReading}
//
//val env = StreamExecutionEnvironment.getExecutionEnvironment
//
//val humidityDataStream: KeyedStream[HumidityReading, String] = env
//  .readTextFile("src/main/resources/Humidity.txt")
//  .map(HumidityReading.fromString)
//  .keyBy(_.location)
//
//val anotherDataStream: KeyedStream[HumidityReading, String] = env
//  .readTextFile("src/main/resources/MoreHumidityData.txt")
//  .map(HumidityReading.fromString)
//  .keyBy(_.location)
//
//val temperatureDataStream: KeyedStream[TemperatureReading, String] = env
//  .readTextFile("src/main/resources/Temperature.txt")
//  .map(TemperatureReading.fromString)
//  .keyBy(_.location)
//
//// Union
//def unionExample(): Unit =
//
//  val env = StreamExecutionEnvironment.getExecutionEnvironment
//
//  val unionedHumidityData: DataStream[HumidityReading] = humidityDataStream.union(anotherDataStream)
//
//  unionedHumidityData.print()
//  env.execute()
//
//// Connect
//def connectExample(): Unit =
//  val humidityAndTempereatureData: ConnectedStreams[HumidityReading, TemperatureReading] =
//    humidityDataStream.connect(temperatureDataStream)
//
//  val outputConnectedStream: DataStream[LocalSummary] =
//    humidityAndTempereatureData
//      .map(_.toLocalSummary, _.toLocalSummary)
//      .keyBy(_.location)
//
//  outputConnectedStream.print()
//  env.execute()
//
//@main def multipleStreamDemo() =
////  unionExample()
//  connectExample()