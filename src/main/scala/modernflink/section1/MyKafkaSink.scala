package modernflink.section1

import modernflink.model.HumidityReading
import org.apache.flink.api.common.serialization.{SerializationSchema, SimpleStringSchema}
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink, KafkaSinkBuilder}
import org.apache.flinkx.api.{DataStream, StreamExecutionEnvironment}
import org.apache.flinkx.api.serializers.*

@main def writeCustomDataToKafka() =

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  val inputFile: DataStream[String] = env.readTextFile("src/main/resources/Humidity.txt")
  val humidityData: DataStream[HumidityReading] = inputFile.map(HumidityReading.fromString)
