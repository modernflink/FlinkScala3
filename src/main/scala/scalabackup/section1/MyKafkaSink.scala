package scalabackup.section1

import scalabackup.modelbackup.HumidityReading
import org.apache.flink.api.common.serialization.{SerializationSchema, SimpleStringSchema}
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink, KafkaSinkBuilder}
import org.apache.flinkx.api.{DataStream, StreamExecutionEnvironment}
import org.apache.flinkx.api.serializers.*

object MyKafkaSink:

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  val inputFile: DataStream[String] = env.readTextFile("src/main/resources/Humidity.txt")
  val humidityData: DataStream[HumidityReading] = inputFile.map(HumidityReading.fromString)

  def main(args: Array[String]): Unit =
    writeCustomDataToKafka()

  private def writeCustomDataToKafka(): Unit =
    val myKafkaSink = KafkaSink
      .builder[String]()
      .setBootstrapServers("localhost:9092")
      .setRecordSerializer(
        KafkaRecordSerializationSchema
          .builder()
          .setTopic("humidity-reading")
          .setValueSerializationSchema(new SimpleStringSchema())
          .build()
      )
      .setDeliveryGuarantee(DeliveryGuarantee.NONE)
      .build()

    humidityData.map(_.sinkOutput).sinkTo(myKafkaSink)
    humidityData.print("Write to Kafka")
    env.execute()
