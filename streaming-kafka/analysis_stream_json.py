## Print Kafka messages as json object

import os
import json
from pyflink.common import WatermarkStrategy, Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "wiki_data"

# not used (not working properly)
json_schema = Types.ROW_NAMED(
        ['id', 'title', 'bot', 'server_name', 'wiki', 'user'],
        [Types.BIG_INT(), Types.STRING(), Types.BOOLEAN(), Types.STRING(), Types.STRING(), Types.STRING()]
    )

def parse_row(json_row: str):
    parsed = json.loads(json_row)
    return {
        "id": int(parsed.get("id")) if parsed.get("id") is not None else None ,
        "title": parsed.get("title"),
        "timestamp": int(parsed.get("timestamp")),
        "user": parsed.get("user"),
        "is_bot": bool(parsed.get("bot")),
        "server_name": parsed.get("server_name"),
        "wiki": parsed.get("wiki"),
    }

def create_flink_job():
    """Create a Flink job that reads from Kafka and prints to terminal"""

    # Create a StreamExecutionEnvironment
    env = StreamExecutionEnvironment.get_execution_environment()
    jar_path = os.path.abspath("./jars/flink-sql-connector-kafka-4.0.1-2.0.jar")
    env.add_jars(f"file:///{jar_path}")
    env.set_parallelism(1)

    # Define the Kafka source
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_topics(KAFKA_TOPIC)
        .set_group_id("flink_group")
        # .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        #.set_value_only_deserializer(
        #    JsonRowDeserializationSchema.builder().type_info(json_schema).build()
        #)
        .build()
    )

    # Add Kafka source to environment
    ds = env.from_source(
        kafka_source, WatermarkStrategy.no_watermarks(), "Kafka Source"
    )

    # Print messages to terminal
    ds.map(parse_row).print()

    # Execute the job
    env.execute("JSON Parsing with Schema")


if __name__ == "__main__":
    create_flink_job()
