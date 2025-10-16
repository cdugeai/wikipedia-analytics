from pyspark.sql import SparkSession, DataFrame
import Timing
import pyspark.sql.functions as pys
from pyspark.sql.window import Window


spark = SparkSession.builder.appName("Footballer stats").getOrCreate()

t = Timing.Timing()


FILE = "out/footballers.csv"


t.start("get_footballers_stats")

df = (
    spark.read.csv(FILE, header=True)
    # .limit(10)
    .withColumn("country", pys.split(pys.col("country"), " ")[0])
)

# Allowed country names, fr-en
countries_ref = spark.read.csv(
    "../data/countries_ref.csv", header=True
).withColumnRenamed("french", "country_ref")

footballers_stats: DataFrame = (
    df.select("country", "height", "foot", "is_alive")
    .groupBy(["country", "foot"])
    .agg(
        pys.avg(pys.col("height")).alias("avg_height"),
        pys.count("*").alias("n_players"),
    )
    .crossJoin(countries_ref)
    # Filter when 3 first letters match
    .filter(pys.substring("country", 0, 4) == pys.substring("country_ref", 0, 4))
    .withColumn(
        "distance",
        pys.levenshtein(
            pys.lower(pys.col("country")), pys.lower(pys.col("country_ref"))
        ),
    )
    .withColumn(
        "rank",
        pys.row_number().over(
            Window.partitionBy("country", "foot").orderBy("distance")
        ),
    )
    # Keep closest country name
    .filter(pys.col("rank") == 1)
    # Use column "english" as ref
    .withColumn("country_ref", pys.col("english"))
    .groupBy(["country_ref", "foot"])
    .agg(
        pys.sum("n_players").alias("n_players"),
        (
            pys.sum((pys.col("n_players") * pys.col("avg_height")))
            / pys.sum("n_players")
        ).alias("avg_height"),
    )
    .filter(pys.col("foot").isin(["left", "right", "both"]) | pys.col("foot").isNull())
    # .select("country", "country_ref", "distance")
    .orderBy("country_ref", "foot")
)

# footballers_stats.explain()

footballers_stats.write.mode("overwrite").csv("out/footballers_stats.csv", header=True)
t.stop()
