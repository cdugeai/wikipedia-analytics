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

## Installation

```sh
cd jars
wget https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.1-2.0/flink-sql-connector-kafka-4.0.1-2.0.jar
```
