package scalabackup.section1
import modernflink.model.HumidityReading
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema, SimpleStringSchema}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flinkx.api.{DataStream, StreamExecutionEnvironment}
import org.apache.flinkx.api.serializers.*

class CustomDeserializer extends DeserializationSchema[HumidityReading]:
  override def deserialize(message: Array[Byte]): HumidityReading =
    HumidityReading.fromString(String(message))

  override def isEndOfStream(nextElement: HumidityReading): Boolean = nextElement == HumidityReading.error

  override def getProducedType: TypeInformation[HumidityReading] =
    implicitly[TypeInformation[HumidityReading]]

@main def myKafkaSource() =
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  readCustomDataFromKafka()
//  readFromKafka()

// read string from Kafka
def readFromKafka(): Unit =
  val kafkaSource = KafkaSource
    .builder[String]()
    .setBootstrapServers("localhost:9092")
    .setTopics("humidity-reading")
    .setGroupId("humidity-group")
    .setProperty("receive.message.max.bytes", "200M")
    .setStartingOffsets(OffsetsInitializer.earliest())
    .setValueOnlyDeserializer(SimpleStringSchema())
    .build()

  val kafkaInput: DataStream[String] =
    env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka")

  kafkaInput.print("Read from Kafka")
  env.execute()

def readCustomDataFromKafka(): Unit =
  val kafkaSource = KafkaSource
    .builder[HumidityReading]()
    .setBootstrapServers("localhost:9092")
    .setTopics("humidity-reading")
    .setGroupId("humidity-group")
    .setStartingOffsets(OffsetsInitializer.earliest())
    .setValueOnlyDeserializer(CustomDeserializer())
    .build()

  val kafkaInput: DataStream[HumidityReading] =
    env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka")

  kafkaInput.print("Read Custom Data From Kafka")
  env.execute()
