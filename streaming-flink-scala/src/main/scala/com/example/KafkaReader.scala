package com.example

import org.apache.flink.streaming.api.scala._
import com.example.model.WikipediaUpdate

object KafkaReader {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    
    val testStreamOne: DataStream[WikipediaUpdate] = env.fromCollection(Seq(
      WikipediaUpdate("wiki_fr", 1686122515, "user_1"),
      WikipediaUpdate("wiki_en", 1686208915, "user_2")
    ))
    
    testStreamOne.print("OutputStream1").setParallelism(2)

    testStreamOne.print()

    env.execute("Read sample data")
  }
}