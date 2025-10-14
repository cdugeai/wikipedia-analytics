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
    "infoboxes": pl.List(
        pl.Struct([pl.Field("name", pl.Utf8), pl.Field("type", pl.Utf8)])
    ),
}
df = pl.scan_ndjson(FILE_SAMPLE, schema=schema_)

t.start("count_infobox")
count_infobox = (
    df.select(pl.col("infoboxes"))
    .with_columns(
        infobox_len=pl.col("infoboxes").len(),
        # Name of the first infobox of the article
        infobox1_name=pl.col("infoboxes").list.first().struct.field("name"),
    )
    .select(pl.col("infobox1_name"))
    .group_by(pl.col("infobox1_name"))
    .len(name="n_articles")
    .sort(pl.col("n_articles"), descending=True)
).collect(engine="streaming")

t.stop()
print(count_infobox)
count_infobox.write_csv("out/cities.csv")
