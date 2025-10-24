## Print Kafka messages as json object

import os
import time
import json
from pyflink.common import WatermarkStrategy, Types, Duration, Time
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.common.watermark_strategy import TimestampAssigner

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "wiki_data"

# not used (not working properly)
json_schema = Types.ROW_NAMED(
    ["id", "title", "bot", "server_name", "wiki", "user"],
    [
        Types.BIG_INT(),
        Types.STRING(),
        Types.BOOLEAN(),
        Types.STRING(),
        Types.STRING(),
        Types.STRING(),
    ],
)


def parse_row(json_row: str):
    parsed = json.loads(json_row)
    return {
        "id": int(parsed.get("id")) if parsed.get("id") is not None else None,
        "title": parsed.get("title"),
        "timestamp": int(parsed.get("timestamp")),
        "user": parsed.get("user"),
        "is_bot": bool(parsed.get("bot")),
        "server_name": parsed.get("server_name"),
        "wiki": parsed.get("wiki"),
    }


def create_flink_job():
    """Create a Flink job that reads from Kafka and prints to terminal"""

    WINDOW_DURATION = 3
    WINDOW_BUFFER = 1
    FILTER_WIKIS = True
    FILTERED_WIKIS = ["enwiki", "frwiki"]

    
    # Create a StreamExecutionEnvironment
    env = StreamExecutionEnvironment.get_execution_environment()
    jar_path = os.path.abspath("./jars/flink-sql-connector-kafka-4.0.1-2.0.jar")
    env.add_jars(f"file:///{jar_path}")
    env.set_parallelism(1)

    # Define watermark strategy with 5 seconds out-of-order tolerance
    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_seconds(WINDOW_BUFFER)
    ).with_timestamp_assigner(ExtractTimestampFromMessage())

    # Define the Kafka source
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_topics(KAFKA_TOPIC)
        .set_group_id("flink_group")
        # .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        # .set_value_only_deserializer(
        #    JsonRowDeserializationSchema.builder().type_info(json_schema).build()
        # )
        .build()
    )

    # Add Kafka source to environment
    ds = env.from_source(kafka_source, watermark_strategy, "Kafka Source")

    # Print messages to terminal
    result = (
        ds.map(parse_row)
        .filter(lambda msg: FILTER_WIKIS & (msg.get("wiki") in FILTERED_WIKIS))
        .map(
            lambda msg: (msg.get("wiki"), 1),
            output_type=Types.TUPLE([Types.STRING(), Types.INT()]),
        )
        .key_by(lambda x: x[0])
        .window(TumblingEventTimeWindows.of(Time.seconds(WINDOW_DURATION)))
        .reduce(lambda a, b: (a[0], a[1] + b[1]))
        .map(lambda x: f"Articles in {x[0]}: {x[1]}")
    )
    result.print()

    # Execute the job
    env.execute("JSON Parsing with Schema")


class ExtractTimestampFromMessage(TimestampAssigner):
    def extract_timestamp(self, message_json, record_timestamp):
        """
        Extract timestamp from the message.
        Assumes message is JSON with a 'timestamp' field in milliseconds.
        Adjust this function based on your message format.
        """
        try:
            message = json.loads(message_json)
            # Assuming timestamp is in milliseconds
            return message.get("timestamp", 0)
        except:
            # Return current time if parsing fails
            return int(time.time() * 1000)


if __name__ == "__main__":
    create_flink_job()
