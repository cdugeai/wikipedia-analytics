import polars as pl


FILE_SAMPLE = "../data/frwiki_namespace_0/sample.jsonl"
FILE_DATA1 = "../data/frwiki_namespace_0/frwiki_namespace_0_0.jsonl"


df = pl.scan_ndjson(FILE_SAMPLE)


count_infobox = (
    df.select(pl.col("infoboxes"))
    .with_columns(
        infobox_len=pl.col("infoboxes").len(),
        name_=pl.col("infoboxes").list.first().struct.field("name"),
    )
    .group_by(pl.col("name_"))
    .len(name="n")
    .sort(pl.col("n"), descending=True)
)

print(count_infobox.collect())
