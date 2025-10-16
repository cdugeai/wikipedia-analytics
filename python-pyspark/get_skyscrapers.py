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


spark = SparkSession.builder.appName("Get skycrapers").getOrCreate()

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
                        StructField("has_parts", StringType(), True),
                    ]
                )
            ),
            True,
        ),
    ]
)

infobox_schema = ArrayType(
    StructType(
        [
            StructField("type", StringType()),
            StructField("name", StringType()),
            StructField("value", StringType()),
        ]
    )
)


def generate_filter_clause(col_name):
    return f"get(filter(parsed, x -> x.name = '{col_name}'),0).value"


t.start("get_skycrapers")

countries_ref = spark.read.csv(
    "../data/countries_ref.csv", header=True
).withColumnRenamed("french", "country")


df = spark.read.schema(schema).json(FILE_DATA_ALL)

skycrapers = (
    df.filter(pys.col("infoboxes")[0]["name"] == "Infobox Gratte-ciel")
    .select(
        pys.col("identifier").alias("id"),
        pys.col("name"),
        pys.get_json_object(
            pys.col("infoboxes").getItem(0).getField("has_parts"), "$[0].has_parts"
        ).alias("infobox_skyscraper"),
    )
    .withColumn(
        colName="parsed",
        col=pys.from_json(pys.col("infobox_skyscraper"), infobox_schema),
    )
    .withColumns(
        {
            "toit": pys.expr(generate_filter_clause("Hauteur")),
            "country": pys.expr(generate_filter_clause("Pays")),
        }
    )
    .withColumn(
        "height",
        pys.regexp_replace(
            pys.regexp_extract(pys.col("toit"), r"(\d+[\.,\d]*)\sm", 1), ",", "."
        ).try_cast("double")
    )
    .drop("parsed", "infobox_skyscraper", "toit")
    .join(countries_ref, how="left", on="country")
    .withColumnRenamed("english", "country_en")
    .filter(pys.col("country_en").isNotNull())
    .groupBy(["country_en"])
    .agg(
        pys.count("*").alias("n_skyscrapers"),
        pys.max("height").alias("max_height"),
        pys.min("height").alias("min_height"),
        pys.avg("height").alias("avg_height"),
    )
    .withColumnRenamed("country_en", "country")
    .orderBy("country")
)

# skycrapers.show()


skycrapers.write.mode("overwrite").csv("out/skycrapers.csv", header=True)

t.stop()
