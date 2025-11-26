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
import com.example.detect.DetectEditWar

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


case class PageActivity(url: String, users: Set[String], edits: Int) extends Serializable

object KafkaReader {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val kafka_addr: String = sys.env.get("KAFKA_ADDR").getOrElse("MISSING KAFKA_ADDR")
    println("Kafka cluster: >> "+kafka_addr)

    val kafkaSource = KafkaSource
      .builder()
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
    )   // Ignore Category pages
      .filter(page =>
        !page.meta.uri.contains("Category:") && 
        !page.meta.uri.contains("Cat%C3%A9gorie:")
      )



    val wiki_by_page_5min = lines
      .filter(t => Set("enwiki", "frwiki").contains(t.wiki))
      .map(e => PageActivity(e.meta.uri, Set(e.user), 1))
      .keyBy(_.url)
      .window(TumblingEventTimeWindows.of(Time.minutes(5)))
      .reduce((p1, p2) => PageActivity(p1.url, p1.users++p2.users, p1.edits+p2.edits))
      .name("Pages grouped by 5 min")


    // Group the updates by wiki and compute some stats
    val updates_by_wiki: DataStream[WindowResult] = lines
      .keyBy(_.wiki)
      .window(TumblingEventTimeWindows.of(Time.seconds(60)))
      .process(new GatherRecords)
      .name("Compute stats")
    
    // Filter FR-EN only
    val updates_wiki_fr_en = updates_by_wiki
      .filter(t => Set("enwiki", "frwiki").contains(t.wiki))
      .name("Filter FR wiki")

    // Sink print
    updates_wiki_fr_en
      .map(t => s"${t.windowStart} (${t.wiki}) : ${t.totalUpdates} updates by ${t.distinctUsers} distinct users.")
      .name("Format stdout msg")
      //.setParallelism(2)
      .print("Wiki updates (grouped)")

    // Sink alert "Multiple edits"
    updates_wiki_fr_en
      .filter(rec => rec.totalUpdates > (rec.distinctUsers + 1)) 
      .name("Filter condition anomaly 1")
      .disableChaining()
      .map(t => (new AlerterPushover).alert("Multiple edits by user: ",s"${t.distinctUsers} utilisateurs ont modifié ${t.totalUpdates} articles."))
      .name("Pushing alert 1")
      .print("ALERT1 Multiple edits by user:")

    // Sink alert "Multiple edits 2"
    // Page updated by > 5 users
    wiki_by_page_5min
      .filter(p => p.users.size>2) 
      .name("Filter condition anomaly 2")
      .disableChaining()
      .map(t => (new AlerterPushover).alert("Multiple edits on page: ",s"Page ${t.url} edited by ${t.users.size} users."))
      .name("Pushing alert 2")
      .print("ALERT2 Multiple edits on page:")

    
    // Sink alert "Edit war"
    // 2+ users edit 3+ times each a single page in 15 minutes
    lines
      .filter(t => Set("enwiki", "frwiki").contains(t.wiki))
      .keyBy(_.meta.uri)
      .window(TumblingEventTimeWindows.of(Time.minutes(15)))
      .aggregate(DetectEditWar.aggregate, DetectEditWar.process)
      .name("Pages with 2+ users, 5+ edits each")
      .disableChaining()
      .map(t => (new AlerterPushover).alert("Edit war on page: ",s"Page ${t.url} edited by ${t.qualified_users.size} users: ${t.qualified_users}"))
      .name("Pushing alert 3")
      .print("ALERT3 Edit war:")


    env.execute("Read from Kafka")
  }
}