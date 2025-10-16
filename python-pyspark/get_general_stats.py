from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)
import pyspark.sql.functions as pys
import Timing


spark = SparkSession.builder.appName("Get general stats").getOrCreate()

t = Timing.Timing()


FILE_SAMPLE = "../data/frwiki_namespace_0/sample.jsonl"
FILE_DATA1 = "../data/frwiki_namespace_0/frwiki_namespace_0_0.jsonl"
FILE_DATA_ALL = "../data/frwiki_namespace_0/frwiki_namespace_0_*.jsonl"


# Define schema (only fields you need)
schema = StructType(
    [
        StructField("name", StringType(), True),
        StructField("identifier", IntegerType(), True),
        StructField(
            "version",
            StructType(
                [
                    StructField("number_of_characters", IntegerType(), True),
                ],
            ),
        ),
    ]
)

t.start("get_general_stats")

df: DataFrame = spark.read.schema(schema).json(FILE_DATA_ALL)

general_stats = (
    df
    # .filter(pys.col("infoboxes")[0]["name"] == "Infobox Commune de France")
    .select(
        pys.col("identifier").alias("id"),
        pys.col("name"),
        pys.col("version").getField("number_of_characters").alias("n_char"),
    )
    .groupBy()
    .agg(
        pys.sum("n_char").alias("total_char"),
        pys.avg("n_char").alias("avg_char_per_article"),
        pys.count("*").alias("n_articles"),
    )
)

# general_stats.show()

general_stats.explain()
general_stats.write.mode("overwrite").csv("out/general_stats.csv", header=True)
t.stop()
