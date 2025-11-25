package com.example

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction

import com.example.model.WikipediaUpdate
import com.example.utils.JsonDeserializer
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner
import com.fasterxml.jackson.module.scala.deser.overrides

import com.example.alert.AlerterPushover

// Type for the gathered results
case class WindowResult(windowStart: Long, windowEnd: Long, wiki: String, totalUpdates: Int, distinctUsers: Int)

// Method to gather results by wiki
class GatherRecords extends ProcessWindowFunction[WikipediaUpdate, WindowResult, String, TimeWindow] {
  override def process(
    key: String,
    context: Context,
    elements: Iterable[WikipediaUpdate],
    out: Collector[WindowResult]
  ): Unit = {
    val totalUpdates = elements.map(_ => 1).sum
    // val users = elements.map(_.user).mkString(", ") // Concat user names
    val distinctUsers = elements.toStream.distinct.length

    out.collect(WindowResult(
      context.window.getStart,
      context.window.getEnd,
      key,
      totalUpdates,
      distinctUsers
    ))
  }
}

object KafkaReader {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val kafka_addr: String = sys.env.get("KAFKA_ADDR").getOrElse("MISSING KAFKA_ADDR")
    println("Kafka cluster: >> "+kafka_addr)

    val kafkaSource = KafkaSource
      .builder()
      .setBootstrapServers("kafka:9093")
      .setBootstrapServers(kafka_addr)
      .setTopics("wiki_data")
      .setGroupId("flink-consumer-group")
      .setProperty("receive.message.max.bytes", "200M")
      .setStartingOffsets(OffsetsInitializer.latest())
      //.setValueOnlyDeserializer(new SimpleStringSchema())
      .setValueOnlyDeserializer(new JsonDeserializer[WikipediaUpdate](classOf[WikipediaUpdate]))
      .build()

    //val lines = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")

    val lines: DataStream[WikipediaUpdate] = env.fromSource(kafkaSource, 
      WatermarkStrategy
      .forBoundedOutOfOrderness(java.time.Duration.ofSeconds(5))
      .withTimestampAssigner( new SerializableTimestampAssigner[WikipediaUpdate] {
        override def extractTimestamp(element: WikipediaUpdate, recordTimestamp: Long): Long = element.timestamp * 1000
      })
      .withIdleness(java.time.Duration.ofSeconds(1)), // IMPORTANT: Handle idle sources
      "Kafka source 2"
    )

    // Group the updates by wiki and compute some stats
    val updates_by_wiki: DataStream[WindowResult] = lines
      .keyBy(_.wiki)
      .window(TumblingEventTimeWindows.of(Time.seconds(2)))
      .process(new GatherRecords)
      .name("Compute stats").uid("Compute stats")
    
    // Filter FR only
    val updates_wiki_fr = updates_by_wiki
      .filter(_.wiki == "frwiki")
      .name("Filter FR wiki").uid("Filter FR wiki")

    // Sink print
    updates_wiki_fr
      .map(t => s"${t.windowStart} (${t.wiki}) : ${t.totalUpdates} updates by ${t.distinctUsers} distinct users.")
      .name("Format stdout msg").uid("Format stdout msg")
      //.setParallelism(2)
      .print("Wiki updates (grouped)")

    // Sink alert "Multiple edits"
    updates_wiki_fr
      .filter(rec => rec.totalUpdates > (rec.distinctUsers + 1)) 
      .name("Filter condition anomaly 1").uid("Filter condition anomaly 1")
      .disableChaining()
      .map(t => (new AlerterPushover).alert("Multiple edits",t))
      .name("Pushing alert 1").uid("Pushing alert 1")

    // SAMPLE Sink alert "Multiple edits 2"
    updates_wiki_fr
      .filter(rec => rec.totalUpdates > (rec.distinctUsers + 0)) 
      .name("Filter condition anomaly 2").uid("Filter condition anomaly 2")
      .disableChaining()
      .map(t => (new AlerterPushover).alert("Multiple edits 2",t))
      .name("Pushing alert 2").uid("Pushing alert 2")

    env.execute("Read from Kafka")
  }
}