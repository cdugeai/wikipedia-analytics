# Streaming data

Send real-time data to a Kafka cluster.

## Usage

```sh
# Start Kafka broker & Flink
make kafka-flink
# Start streaming data from Wikipedia to Kafka
make streaming
# Run 1st analysis (uses TableAPI)
make analysis_table_api
```
