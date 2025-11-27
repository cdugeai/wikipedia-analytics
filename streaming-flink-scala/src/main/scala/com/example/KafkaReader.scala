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
    )   

    // Filter FR-EN only + skip Category
    val updates_wiki_fr_en = lines
      .filter(t => Set("enwiki", "frwiki").contains(t.wiki))
      .name("Filter FR wiki")
      .filter(page =>
        !page.meta.uri.contains("Category:") && 
        !page.meta.uri.contains("Cat%C3%A9gorie:")
      ).name("Skip Category pages")


    // [ALERT 1]
    // Group the updates by wiki and compute some stats
    val windows_s_edits_by_user = 10; // Window in s
    val threshold_edits_by_user = 5; // Max edits by user

    val alert1_multiple_edits_user: DataStream[(String, Int)] = 
      updates_wiki_fr_en
      .map(u => (u.user, 1)) // (user, n_edits)
      .keyBy(_._1) // Keyed by user
      .window(TumblingEventTimeWindows.of(Time.seconds(windows_s_edits_by_user)))
      .reduce((a, b) => (a._1, a._2 + b._2)) // Sum edits for user
      .name("Edits grouped by user")
      
    
    // Send alert
    alert1_multiple_edits_user
      .filter(_._2 > threshold_edits_by_user)
      .name("Filter condition anomaly 1")
      .disableChaining()
      .map(t => (new AlerterPushover).alert("Multiple edits by user: ",s"User ${t._1} edited ${t._2} pages."))
      .name("Pushing alert 1")
      .print("ALERT1 Multiple edits by user:")

    // [ALERT 2]
    val updates_by_page_5min = updates_wiki_fr_en
      .map(e => PageActivity(e.meta.uri, Set(e.user), 1))
      .keyBy(_.url)
      .window(TumblingEventTimeWindows.of(Time.minutes(5)))
      .reduce((p1, p2) => PageActivity(p1.url, p1.users++p2.users, p1.edits+p2.edits))
      .name("Pages grouped by 5 min")
    
    // Sink alert "Multiple edits 2"
    // Page updated by > 5 users
    updates_by_page_5min
      .filter(p => p.users.size>2) 
      .name("Filter condition anomaly 2")
      .disableChaining()
      .map(t => (new AlerterPushover).alert("Multiple edits on page: ",s"Page ${t.url} edited by ${t.users.size} users."))
      .name("Pushing alert 2")
      .print("ALERT2 Multiple edits on page:")

    
    // [ALERT 3]

    // Sink alert "Edit war"
    // 2+ users edit 3+ times each a single page in 15 minutes
    updates_wiki_fr_en
      .keyBy(_.meta.uri)
      .window(TumblingEventTimeWindows.of(Time.minutes(15)))
      .aggregate(DetectEditWar.aggregate, DetectEditWar.process)
      .name("Edit War detection")
      .disableChaining()
      .map(t => (new AlerterPushover).alert("Edit war on page: ",s"Page ${t.url} edited by ${t.qualified_users.size} users multiple times: ${t.qualified_users}"))
      .name("Pushing alert 3")
      .print("ALERT3 Edit war:")


    env.execute("Read from Kafka")
  }
}