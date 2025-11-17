import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.java.tuple.Tuple2

//import org.apache.flink.api.createTypeInformation

object WordCountJob:
  def main(args: Array[String]): Unit = 
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    
    val text = env.fromElements(
      "Hello World",
      "Hello Flink",
      "Flink Scala API"
    )
    // Use explicit TypeInformation
    val words = text.flatMap(
      new FlatMapFunction[String, String] {
        override def flatMap(value: String, out: Collector[String]): Unit = {
          value.toLowerCase.split("\\W+").filter(_.nonEmpty).foreach(out.collect)
        }
      },
      Types.STRING  // Explicit type information
    )
    

    val counts = words
      .map(word => new Tuple2(word, Int.box(1)))
      .returns(Types.TUPLE(Types.STRING, Types.INT))
      .keyBy(t => t.f0)
      .sum(1)

    counts.print()
    
    env.execute("WordCount Example")
