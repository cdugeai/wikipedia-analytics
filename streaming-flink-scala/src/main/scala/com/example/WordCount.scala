package com.example

import org.apache.flink.streaming.api.scala._

object WordCountJob {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    
    // simple source for demonstration
    val text: DataStream[String] = env.fromElements(
      "Hello World",
      "The quick brown fox jumps over the lazy dog",
      "Hello Flink",
      "Flink and Scala"
    )

    // Clear, idiomatic Scala-style pipeline
    val counts = text
      .flatMap(_.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map(word => (word, 1))
      .keyBy(_._1)
      .sum(1)

    counts.print()

    env.execute("Scala 2 WordCount")
  }
}