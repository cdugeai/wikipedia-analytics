# Bucket the creation dates of each article

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
import pyspark.sql.functions as pys
import Timing


spark = SparkSession.builder.appName("Bucket creation date").getOrCreate()

t = Timing.Timing()


FILE_SAMPLE = "../data/frwiki_namespace_0/sample.jsonl"
FILE_DATA1 = "../data/frwiki_namespace_0/frwiki_namespace_0_0.jsonl"
FILE_DATA_ALL = "../data/frwiki_namespace_0/frwiki_namespace_0_*.jsonl"


# Define schema (only fields you need)
schema = StructType(
    [
        StructField("name", StringType(), True),
        StructField("identifier", IntegerType(), True),
        StructField("date_created", DateType(), True),
        StructField("date_modified", DateType(), True),
    ]
)

t.start("get_general_stats")

df: DataFrame = spark.read.schema(schema).json(FILE_DATA_ALL)  # .limit(100000)

creation_dates = (
    df
    # .filter(pys.col("infoboxes")[0]["name"] == "Infobox Commune de France")
    .withColumn(
        "date_", pys.coalesce(pys.col("date_created"), pys.col("date_modified"))
    )
    .select(
        pys.col("identifier").alias("id"),
        pys.col("name"),
        pys.concat_ws(
            "-Q",
            pys.year(pys.col("date_created")),
            pys.quarter(pys.col("date_created")),
        ).alias("q_created"),
        # pys.col("version").getField("number_of_characters").alias("n_char"),
    )
    .groupBy("q_created")
    .agg(
        pys.count("*").alias("n_articles"),
    )
    .orderBy("q_created")
)

# creation_dates.show()

creation_dates.explain()
creation_dates.write.mode("overwrite").csv("out/creation_dates.csv", header=True)
t.stop()
