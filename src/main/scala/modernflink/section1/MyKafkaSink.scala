package modernflink.section1

import modernflink.model.HumidityReading
import org.apache.flink.api.common.serialization.{SerializationSchema, SimpleStringSchema}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.serializers.*
import org.apache.flink.api.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink, KafkaSinkBuilder}
import org.apache.flink.streaming.api.datastream.{CustomSinkOperatorUidHashes, DataStreamSink}
import org.apache.flink.streaming.api.functions.sink.SinkFunction

import scala.io.Source


object MyKafkaSink {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  val inputFile: DataStream[String] = env.readTextFile("src/main/resources/Humidity.txt")
  val humidityData: DataStream[HumidityReading] = inputFile.map(HumidityReading.fromString)
  private def writeCustomDataToKafka(): Unit = {
    val myKafkaSink = KafkaSink
      .builder[String]()
      .setBootstrapServers("localhost:9092")
      .setRecordSerializer(KafkaRecordSerializationSchema.builder()
        .setTopic("humidity-reading")
        .setValueSerializationSchema(new SimpleStringSchema())
        .build()
      )
      .setDeliveryGuarantee(DeliveryGuarantee.NONE)
      .build()

    humidityData
      .map(_.sinkOutput)
      .sinkTo(myKafkaSink)
    humidityData.print("Write to Kafka")
    env.execute()
  }

    def main(args: Array[String]): Unit = {
      writeCustomDataToKafka()
    }
  }
