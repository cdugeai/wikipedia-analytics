from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import MapFunction
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
import json
import os


class PrintFunction(MapFunction):
    def map(self, value):
        data = json.loads(value)
        print(f"Title: {data.get('title')}, User: {data.get('user')}")
        return value


env = StreamExecutionEnvironment.get_execution_environment()
# env.add_jars("/tmp/flink-connector-kafka-3.0.0-1.17.jar")
jar_path = os.path.abspath("./jars/flink-connector-kafka-3.1.0-1.18.jar")
print(jar_path)
env.add_jars(f"file:///{jar_path}")


# Create Kafka consumer
kafka_consumer = FlinkKafkaConsumer(
    topics="wiki_data",
    deserialization_schema=SimpleStringSchema(),
    properties={"bootstrap.servers": "localhost:9092", "group.id": "flink-group"},
)

# Add source and apply the print function
ds = env.add_source(kafka_consumer)
ds.map(PrintFunction()).print()

# Execute the job
env.execute("Flink Kafka Consumer")
