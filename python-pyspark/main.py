from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType,
)
import pyspark.sql.functions as pys
import Timing


spark = SparkSession.builder.appName("Infobox counter").getOrCreate()

t = Timing.Timing()


FILE_SAMPLE = "../data/frwiki_namespace_0/sample.jsonl"
FILE_DATA1 = "../data/frwiki_namespace_0/frwiki_namespace_0_0.jsonl"
FILE_DATA_ALL = "../data/frwiki_namespace_0/frwiki_namespace_0_*.jsonl"


# Define schema (only fields you need)
schema = StructType(
    [
        StructField(
            "infoboxes",
            ArrayType(
                StructType(
                    [
                        StructField("name", StringType(), True),
                        StructField("type", StringType(), True),
                    ]
                )
            ),
            True,
        )
    ]
)

t.start("count_infobox")

df = spark.read.schema(schema).json(FILE_DATA_ALL)

count_infobox = (
    df.select(pys.col("infoboxes")[0]["name"].alias("infobox1_name"))
    .groupBy(pys.col("infobox1_name"))
    .agg(pys.count("*").alias("n_articles"))
    .orderBy(pys.col("n_articles").desc())
)

t.stop()
count_infobox.write.mode("overwrite").csv("out/count_infobox.csv", header=True)
print(count_infobox)
