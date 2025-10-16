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


spark = SparkSession.builder.appName("Footballer data").getOrCreate()

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


t.start("get_footballers")

df = spark.read.schema(schema).json(FILE_DATA_ALL)  # .limit(1000)

footballers: DataFrame = (
    df.filter(pys.col("infoboxes")[0]["name"] == "Infobox Footballeur")
    .select(
        pys.col("identifier").alias("id"),
        pys.col("name"),
        pys.get_json_object(
            pys.col("infoboxes").getItem(0).getField("has_parts"), "$[0].has_parts"
        ).alias("infobox_footballer"),
    )
    .withColumn(
        colName="parsed",
        col=pys.from_json(pys.col("infobox_footballer"), infobox_schema),
    )
    .withColumns(
        {
            "team": pys.expr(generate_filter_clause("Équipe")),
            "position": pys.expr(generate_filter_clause("Poste")),
            "country": pys.expr(generate_filter_clause("Nationalité")),
            "height": pys.expr(generate_filter_clause("Taille")),
            "foot": pys.expr(generate_filter_clause("Pied fort")),
            "death": pys.expr(generate_filter_clause("Décès")),
        }
    )
    .withColumns(
        {
            "height": pys.regexp_replace(
                pys.regexp_extract(pys.col("height"), r"^(\d,\d{2})\s+", 1), ",", "."
            ).try_cast("double"),
            # translate left-right
            "foot": pys.regexp_replace(
                pys.regexp_replace(
                    pys.regexp_replace(pys.lower(pys.col("foot")), "gauche", "left"),
                    "droit",
                    "right",
                ),
                "ambidextre",
                "both",
            ),
            "is_alive": pys.isnull(pys.col("death")),
        }
    )
    .drop("parsed", "infobox_footballer")
)

# footballers.show()
# footballers.explain()

# print(footballers.collect()[0])
footballers.write.mode("overwrite").csv("out/footballers.csv", header=True)
t.stop()
