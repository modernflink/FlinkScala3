package modernflink.section1
import org.apache.flink.streaming.api.*
import org.apache.flink.api.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.api.serializers.*
import org.apache.flink.api.common.typeinfo.TypeInformation
import modernflink.model.HumidityReading

import scala.io.Source
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.{
  DeserializationSchema,
  SerializationSchema,
  SimpleStringSchema
}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer

object MyKafkaSource:

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  // read string from Kafka
  def readFromKafka(): Unit =
    val kafkaSource = KafkaSource
      .builder[String]()
      .setBootstrapServers("localhost:9092")
      .setTopics("humidity-reading")
      .setGroupId("humidity-group")
      .setProperty("receive.message.max.bytes", "200M")
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    val kafkaInput: DataStream[String] =
      env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka")

    kafkaInput.print("Read from Kafka")
    env.execute()

    class CustomDeserializer extends DeserializationSchema[HumidityReading]:
      override def deserialize(message: Array[Byte]): HumidityReading =
        // format
        val inputData = new String(message)
        val token = inputData.split(",")
        val location = token(0)
        val timestamp = token(1)
        val humidity = token(2)
        HumidityReading(location, timestamp.trim.toLong, humidity.trim.toDouble)

      override def isEndOfStream(nextElement: HumidityReading): Boolean = false

      override def getProducedType: TypeInformation[HumidityReading] =
        implicitly[TypeInformation[HumidityReading]]

    def readCustomDataFromKafka(): Unit =
      val kafkaSource = KafkaSource
        .builder[HumidityReading]()
        .setBootstrapServers("localhost:9092")
        .setTopics("humidity-reading")
        .setGroupId("humidity-group")
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setValueOnlyDeserializer(new CustomDeserializer())
        .build()

      val kafkaInput: DataStream[HumidityReading] =
        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka")

      kafkaInput.print("Read Custom Data From Kafka")
      env.execute()

    def main(args: Array[String]): Unit =
      readCustomDataFromKafka()
      readFromKafka()
