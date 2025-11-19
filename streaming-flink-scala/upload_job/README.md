# Description

Uploads and runs a job via REST API of the Flink cluster.

## Usage

First, set `FLINK_HOST` and `FLINK_PORT` variables in `streaming-flink-scala/upload_job/.env` file.

Then, either run:

```sh
# (recommended) From streaming-flink-scala/
make upload
# From streaming-flink-scala/upload_job
uv run python3 main.py
```
