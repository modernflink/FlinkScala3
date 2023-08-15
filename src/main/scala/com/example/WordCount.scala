package com.example

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.util.typeutils.FieldAccessorFactory
import org.apache.flink.api.*
import org.apache.flink.api.serializers.*
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.serializers.*
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.util.typeutils.ScalaProductFieldAccessorFactory
import org.slf4j.LoggerFactory
import org.apache.flink.api.serializers._
import scala.util.Try
//import org.apache.flink.connector.kafka.source.KafkaSource
//import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
object Givens:
  given tupleTypeInfo: TypeInformation[Tuple2[String, Int]] =
    deriveTypeInformation
case class Pair(string: String, int: Int)
@main def wordCount =
  import Givens.tupleTypeInfo
  import scala.collection.Iterable
  val log = LoggerFactory.getLogger(classOf[FieldAccessorFactory])
  import org.apache.flink.streaming.util.typeutils.*
  val scalaProductFieldAccessorFactory = ScalaProductFieldAccessorFactory.load(log)
  require(scalaProductFieldAccessorFactory != null)
  val conf = Configuration()
  conf.setString("state.savepoints.dir", "file:///tmp/savepoints")
  conf.setString(
    "execution.checkpointing.externalized-checkpoint-retention",
    "RETAIN_ON_CANCELLATION"
  )
  conf.setString("execution.checkpointing.interval", "10s")
  conf.setString("execution.checkpointing.min-pause", "10s")
  conf.setString("state.backend", "filesystem")

    val env = Try(
      StreamExecutionEnvironment.getExecutionEnvironment
    ).getOrElse(StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf))
  val text = env.fromElements(
    "To be, or not to be,--that is the question:--",
    "Whether 'tis nobler in the mind to suffer",
    "The slings and arrows of outrageous fortune",
    "Or to take arms against a sea of troubles,"
  )


  text
    .flatMap(_.toLowerCase.split("\\W+"))
    .map(Pair(_, 1))
    .keyBy(_.string)
    .sum(1)
    .print()

  val nums = env
    .fromElements(1, 2, 3)
    .map(x => x + 1)
    .print()
//  val brokers = "localhost:9092"
//  val source = KafkaSource
//    .builder[String]
//    .setBootstrapServers(brokers)
//    .setTopics("action")
//    .setGroupId("my-group")
//    .setStartingOffsets(OffsetsInitializer.earliest)
//    .setValueOnlyDeserializer(new SimpleStringSchema)
//    .build
//
//  val stream = env.fromSource(source, WatermarkStrategy.noWatermarks, "Kafka Source")
//  stream.print()
  env.execute("wordCount")
