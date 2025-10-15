from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ArrayType,
)
import pyspark.sql.functions as pys
import Timing


spark = SparkSession.builder.appName("Get cities stats").getOrCreate()

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


def generate_filter_clause(col_name):
    return f"get(filter(parsed, x -> x.name = '{col_name}'),0).value"


infobox_schema = ArrayType(
    StructType(
        [
            StructField("type", StringType()),
            StructField("name", StringType()),
            StructField("value", StringType()),
        ]
    )
)

t.start("get_stats_cities")

cities_id = (
    spark.read.csv("out/cities.csv", header=True)
    # dedupe file
    .groupBy(pys.col("id"), pys.col("name"))
    .agg(pys.count("*").alias("n_articles"))
    .select(pys.col("id"), pys.col("name"))
    # filter debug
    # .filter(pys.col("id") == "663351")
)


df = spark.read.schema(schema).json(FILE_DATA_ALL)  # .limit(16000)  # limit debug


stats_cities: DataFrame = (
    cities_id.alias("city")
    .join(
        df.alias("article"),
        pys.col("city.id") == pys.col("article.identifier"),
        how="left",
    )
    .select(
        pys.col("id"),
        pys.col("article.name").alias("name"),
        pys.get_json_object(
            pys.col("infoboxes").getItem(0).getField("has_parts"), "$[0].has_parts"
        ).alias("infobox_city"),
    )
    .withColumn(
        colName="parsed", col=pys.from_json(pys.col("infobox_city"), infobox_schema)
    )
    .withColumns(
        {
            "pays": pys.expr(generate_filter_clause("Pays")),
            "code_postal": pys.expr(generate_filter_clause("Code postal")),
            "population_str": pys.expr(generate_filter_clause("Population municipale")),
            "density_str": pys.expr(generate_filter_clause("Densité")),
            "coords_str": pys.expr(generate_filter_clause("Coordonnées")),
            "superficy_str": pys.expr(generate_filter_clause("Superficie")),
            "maire_mandat": pys.expr(generate_filter_clause("Maire Mandat")),
        }
    )
    .drop("parsed", "infobox_city")
    .withColumns(
        {
            "mayor": pys.regexp_extract(pys.col("maire_mandat"), r"^(.+?)\s+\d{4}", 1),
            "postal_code": pys.regexp_extract(pys.col("code_postal"), r"\d+\.?\d*", 0),
            "population": pys.regexp_extract(
                pys.col("population_str"), r"([\d\s]+)\s*hab\.", 1
            ),
            "density_km2": pys.regexp_replace(pys.col("density_str"), " ", ""),
            "area_km2": pys.regexp_replace(pys.col("superficy_str"), " ", ""),
        }
    )
    .withColumns(
        {
            "population": pys.regexp_replace(pys.col("population"), " ", "").try_cast(
                "int"
            ),
            "density_km2": pys.regexp_extract(
                pys.col("density_km2"), r"(\d+(?:[,.]\d+)?)hab\./km2", 1
            ),
            "area_km2": pys.regexp_extract(
                pys.col("area_km2"), r"(\d+(?:[,.]\d+)?)km2", 1
            ),
        }
    )
    .withColumns(
        {
            "density_km2": pys.regexp_replace(
                pys.col("density_km2"), ",", "."
            ).try_cast("float"),
            "area_km2": pys.regexp_replace(pys.col("area_km2"), ",", ".").try_cast(
                "float"
            ),
        }
    )
    .select(
        "id", "name", "mayor", "postal_code", "population", "density_km2", "area_km2"
    )
)

stats_cities.explain()
stats_cities.write.mode("overwrite").csv("out/cities_stats.csv", header=True)
t.stop()
