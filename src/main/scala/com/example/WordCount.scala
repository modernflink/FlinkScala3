package com.example

import org.apache.flink.api.*
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.serializers.*
import org.apache.flink.configuration.Configuration
import scala.util.Try
//import org.apache.flink.connector.kafka.source.KafkaSource
//import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer

@main def wordCount =

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
    .map((_, 1))
    .keyBy(_._1)
    .sum(1)
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
