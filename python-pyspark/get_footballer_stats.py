from pyspark.sql import SparkSession, DataFrame
import Timing
import pyspark.sql.functions as pys


spark = SparkSession.builder.appName("Footballer stats").getOrCreate()

t = Timing.Timing()


FILE = "out/footballers.csv"


t.start("get_footballers_stats")

df = (
    spark.read.csv(FILE, header=True)
    # .limit(10)
    .withColumn("country", pys.split(pys.col("country"), " ")[0])
)


footballers_stats: DataFrame = (
    df.select("country", "height", "foot", "is_alive")
    .groupBy(["country", "foot"])
    .agg(
        pys.avg(pys.col("height")).alias("avg_height"),
        pys.count("*").alias("n_players"),
    )
    .orderBy("country", "foot")
)

footballers_stats.show()
# footballers_stats.explain()

# print(footballers_stats.collect()[0])
footballers_stats.write.mode("overwrite").csv("out/footballers_stats.csv", header=True)
t.stop()
