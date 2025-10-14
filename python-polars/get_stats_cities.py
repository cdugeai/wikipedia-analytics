# Script to get all cities articles

import polars as pl
import Timing

t = Timing.Timing()

FILE_SAMPLE = "../data/frwiki_namespace_0/sample.jsonl"
FILE_DATA1 = "../data/frwiki_namespace_0/frwiki_namespace_0_0.jsonl"
FILE_DATA_ALL = "../data/frwiki_namespace_0/frwiki_namespace_0_*.jsonl"

# Json properties to keep (speed-up parsing)
schema_ = {
    "name": pl.Utf8,
    "identifier": pl.Int32,
    "infoboxes": pl.List(
        pl.Struct(
            [
                pl.Field("name", pl.Utf8),
                pl.Field("type", pl.Utf8),
                pl.Field("has_parts", pl.Utf8),
            ]
        )
    ),
}
cities_id = (
    pl.scan_csv("out/cities.csv")
    ## Deduplicate file
    .group_by(pl.col("id"), pl.col("name"))
    .len()
    .select(pl.col("id"), pl.col("name"))
)

df = pl.scan_ndjson(FILE_DATA_ALL, schema=schema_)

t.start("get_stats_cities")

stats_cities = (
    cities_id.join(df, how="left", left_on="id", right_on=pl.col("identifier"))
    .filter(pl.col("infoboxes").is_not_null())
    .select(
        pl.col("id"),
        pl.col("name"),
        infobox_city=pl.col("infoboxes").list.first().struct.field("has_parts"),
    )
    .with_columns(
        code_postal=pl.col("infobox_city").str.json_path_match(
            '$[0].has_parts[?(@.name == "Code postal")].value'
        ),
        population_str=pl.col("infobox_city").str.json_path_match(
            '$[0].has_parts[?(@.name == "Population municipale")].value'
        ),
        density_str=pl.col("infobox_city").str.json_path_match(
            '$[0].has_parts[?(@.name == "Densité")].value'
        ),
        coords_str=pl.col("infobox_city").str.json_path_match(
            '$[0].has_parts[?(@.name == "Coordonnées")].value'
        ),
        superficy_str=pl.col("infobox_city").str.json_path_match(
            '$[0].has_parts[?(@.name == "Superficie")].value'
        ),
        maire_mandat=pl.col("infobox_city").str.json_path_match(
            '$[0].has_parts[?(@.name == "Maire Mandat")].value'
        ),
    )
    .select(pl.exclude("infobox_city"))
).collect(engine="streaming")

t.stop()

stats_cities.write_csv("out/cities_stats.csv")
stats_cities.head(20).write_csv("out/cities_stats_sample20.csv")
