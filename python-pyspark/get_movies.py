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


spark = SparkSession.builder.appName("Get movies").getOrCreate()

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


t.start("get_movies")

df = spark.read.schema(schema).json(FILE_DATA_ALL)

movies_data = (
    df.filter(pys.col("infoboxes")[0]["name"] == "Infobox Cinéma (film)")
    .select(
        pys.col("identifier").alias("id"),
        pys.col("name"),
        pys.get_json_object(
            pys.col("infoboxes").getItem(0).getField("has_parts"), "$[0].has_parts"
        ).alias("infobox_movie"),
    )
    .withColumn(
        colName="parsed", col=pys.from_json(pys.col("infobox_movie"), infobox_schema)
    )
    .withColumns(
        {
            "country": pys.expr(generate_filter_clause("Pays de production")),
            "genre": pys.expr(generate_filter_clause("Genre")),
            "duration_m": pys.regexp_extract(
                pys.expr(generate_filter_clause("Durée")), r"(\d+)\s+minutes", 1
            ).try_cast(IntegerType()),
        }
    )
    # remove row with no duration values
    .filter(pys.col("duration_m").isNotNull())
    .withColumnRenamed("name", "title")
    # keep first country
    .withColumn(
        "country",
        pys.split(pys.col("country"), " ")[0],
    )
    # keep first genre
    .withColumn(
        "genre",
        pys.regexp_replace(pys.lower(pys.split(pys.col("genre"), " ")[0]), ",", ""),
    )
    .withColumn("genre", pys.coalesce(pys.col("genre"), pys.lit("unknown")))
    .drop("parsed", "infobox_movie")
)

# movies_data.show()

movies_stats_by_genre = movies_data.groupBy(["genre"]).agg(
    pys.count("*").alias("n_movies"),
    pys.percentile_approx(pys.col("duration_m"), 0.25).alias("dur_q1"),
    pys.percentile_approx(pys.col("duration_m"), 0.75).alias("dur_q3"),
    pys.max("duration_m").alias("max_duration"),
    pys.min("duration_m").alias("min_duration"),
    pys.avg("duration_m").alias("avg_duration"),
    pys.median("duration_m").alias("median_duration"),
)

# movies_stats_by_genre.show()

movies_stats_by_country = movies_data.groupBy(["country"]).agg(
    pys.count("*").alias("n_movies"),
    pys.percentile_approx(pys.col("duration_m"), 0.25).alias("dur_q1"),
    pys.percentile_approx(pys.col("duration_m"), 0.75).alias("dur_q3"),
    pys.max("duration_m").alias("max_duration"),
    pys.min("duration_m").alias("min_duration"),
)

movies_stats_by_genre_country = (
    movies_data.groupBy(["genre", "country"])
    .agg(
        pys.count("*").alias("n_movies"),
        pys.percentile_approx(pys.col("duration_m"), 0.25).alias("dur_q1"),
        pys.percentile_approx(pys.col("duration_m"), 0.75).alias("dur_q3"),
        pys.max("duration_m").alias("max_duration"),
        pys.min("duration_m").alias("min_duration"),
        pys.avg("duration_m").alias("avg_duration"),
    )
    .filter(pys.col("country").isNotNull())
    # Afrique is not a country
    .filter(pys.col("country") != "Afrique")
    .orderBy("country", "genre")
)
# movies_stats_by_genre_country.show()

# movies.explain()

movies_stats_by_genre.write.mode("overwrite").csv(
    "out/movies_stats_by_genre.csv", header=True
)
movies_stats_by_genre_country.write.mode("overwrite").csv(
    "out/movies_stats_by_genre_country.csv", header=True
)
t.stop()
