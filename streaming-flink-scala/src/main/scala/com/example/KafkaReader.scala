package com.example

import org.apache.flink.streaming.api.scala._
import com.example.model.WikipediaUpdate
import com.example.utils.JsonDeserializer
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.eventtime.WatermarkStrategy

object KafkaReader {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    
    val kafkaSource = KafkaSource
      .builder()
      .setBootstrapServers("kafka:9093")
      .setTopics("wiki_data")
      .setGroupId("flink-consumer-group")
      .setProperty("receive.message.max.bytes", "200M")
      .setStartingOffsets(OffsetsInitializer.earliest())
      //.setValueOnlyDeserializer(new SimpleStringSchema())
      .setValueOnlyDeserializer(new JsonDeserializer[WikipediaUpdate](classOf[WikipediaUpdate]))
      .build()

    val lines = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")

    
    lines.print("OutputStream1").setParallelism(2)


    env.execute("Read from Kafka")
  }
}