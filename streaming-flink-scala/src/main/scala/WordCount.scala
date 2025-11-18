import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.java.functions.KeySelector

object WordCountJob {
  def main(args: Array[String]): Unit = {
    // get the environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // sample input (replace with env.readTextFile(...) or socket stream as needed)
    val text = env.fromElements(
      "To be, or not to be: that is the question",
      "Whether 'tis nobler in the mind to suffer",
      "To die, to sleep"
    )

    // FlatMap: split lines into (word, 1) tuples
    val words = text.flatMap(new FlatMapFunction[String, Tuple2[String, Integer]] {
      override def flatMap(value: String, out: Collector[Tuple2[String, Integer]]): Unit = {
        val tokens = value.toLowerCase.split("\\W+")
        var i = 0
        while (i < tokens.length) {
          val t = tokens(i)
          if (t.nonEmpty) {
            // note: use java.lang.Integer (boxed) for Flink Tuple2<int> semantics
            out.collect(new Tuple2(t, Integer.valueOf(1)))
          }
          i += 1
        }
      }
    })

    // Key by the word (f0) and sum the counts (f1)
    val counts = words
      .keyBy(new KeySelector[Tuple2[String, Integer], String] {
        override def getKey(value: Tuple2[String, Integer]): String = value.f0
      })
      .sum(1)

    // print to stdout
    counts.print()

    env.execute("Scala 3 WordCount (using Java DataStream API)")
  }
}