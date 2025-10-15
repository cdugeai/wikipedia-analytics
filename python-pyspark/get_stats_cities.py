from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ArrayType,
)
import pyspark.sql.functions as pys
import Timing


spark = SparkSession.builder.appName("Get cities").getOrCreate()

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
            "infoboxes",
            ArrayType(
                StructType(
                    [
                        StructField("name", StringType(), True),
                        StructField("type", StringType(), True),
                        StructField("has_parts", StructType(), True),
                    ]
                )
            ),
            True,
        ),
    ]
)

t.start("get_cities")

df = spark.read.schema(schema).json(FILE_DATA_ALL)

cities = (
    df.filter(pys.col("infoboxes")[0]["name"] == "Infobox Commune de France").select(
        pys.col("identifier").alias("id"), pys.col("name")
    )
    .orderBy(pys.col("name")) # adds +100% processing time
)

cities.explain()
cities.write.mode("overwrite").csv("out/cities.csv", header=True)
t.stop()
