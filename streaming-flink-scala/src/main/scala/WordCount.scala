import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.functions.{FlatMapFunction, FilterFunction, MapFunction}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.util.Collector

object WordCountJob {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val text: DataStream[String] = env.fromElements(
      "To be, or not to be: that is the question",
      "Whether 'tis nobler in the mind to suffer",
      "To die, to sleep"
    )

    val counts = text
      .flatMap(new FlatMapFunction[String, String] {
        override def flatMap(value: String, out: Collector[String]): Unit = {
          val words = value.toLowerCase.split("\\W+")
          var i = 0
          while (i < words.length) {
            if (words(i).nonEmpty) {
              out.collect(words(i))
            }
            i += 1
          }
        }
      })
      .map(new MapFunction[String, (String, Int)] {
        override def map(value: String): (String, Int) = (value, 1)
      })
      .keyBy(new KeySelector[(String, Int), String] {
        override def getKey(value: (String, Int)): String = value._1
      })
      .sum(1)

    counts.print()
    env.execute("Scala 2 WordCount")
  }
}