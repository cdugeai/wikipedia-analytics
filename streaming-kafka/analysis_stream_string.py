## Print Kafka messages as text

import os
from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "wiki_data"

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
        .build()
    )

    # Add Kafka source to environment
    ds = env.from_source(
        kafka_source, WatermarkStrategy.no_watermarks(), "Kafka Source"
    )

    # Print messages to terminal
    ds.print()

    # Execute the job
    env.execute("Kafka to Console")


if __name__ == "__main__":
    create_flink_job()
