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
        pl.Struct([pl.Field("name", pl.Utf8), pl.Field("type", pl.Utf8)])
    ),
}
df = pl.scan_ndjson(FILE_DATA_ALL, schema=schema_)

t.start("get_cities")
count_infobox = (
    df.with_columns(
        infobox_len=pl.col("infoboxes").len(),
        # Name of the first infobox of the article
        infobox1_name=pl.col("infoboxes").list.first().struct.field("name"),
    )
    .filter(pl.col("infobox1_name").eq("Infobox Commune de France"))
    .select(pl.col("identifier").alias("id"), pl.col("name"))
    .sort(pl.col("name"), descending=False)
).collect(engine="streaming")

t.stop()
print(count_infobox)
count_infobox.write_csv("out/cities.csv")
