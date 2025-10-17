import os


from pyflink.common import WatermarkStrategy, Encoder
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FileSink, RollingPolicy, OutputFileConfig
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'wiki_data'


def kafka_sink_example():
    """A Flink task which sinks a kafka topic to a file on disk

    We will read from a kafka topic and then perform a count() on
    the message to count the number of characters in the message.
    We will then save that count to the file on disk.
    """
    # Create a StreamExecutionEnvironment
    env = StreamExecutionEnvironment.get_execution_environment()

    # the kafka/sql jar is used here as it's a fat jar and could avoid dependency issues
    #env.add_jars("file:///jars/flink-sql-connector-kafka-3.1.0-1.18.jar")
    jar_path = os.path.abspath("./jars/flink-sql-connector-kafka-3.1.0-1.18.jar")
    print(jar_path)
    env.add_jars(f"file:///{jar_path}")

    # Define the new kafka source with our docker brokers/topics
    # This creates a source which will listen to our kafka broker
    # on the topic we created. It will read from the earliest offset
    # and just use a simple string schema for serialization (no JSON/Proto/etc)
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_topics(KAFKA_TOPIC)
        .set_group_id("flink_group")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Adding our kafka source to our environment
    ds = env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "Kafka Source")

    # Just count the length of the string. You could get way more complex
    # here
    ds = ds.map(lambda a: len(a), output_type=Types.ROW([Types.INT()]))

    output_path = os.path.join(os.environ.get("SINK_DIR", "/sink"), "sink.log")

    # This is the sink that we will write to
    sink = (
        FileSink.for_row_format(
            base_path=output_path, encoder=Encoder.simple_string_encoder()
        )
        .with_output_file_config(OutputFileConfig.builder().build())
        .with_rolling_policy(RollingPolicy.default_rolling_policy())
        .build()
    )

    # Writing the processed stream to the file
    ds.sink_to(sink=sink)

    # Execute the job and submit the job
    env.execute("kafka_sink_example")


if __name__ == "__main__":
    kafka_sink_example()
