from kafka import KafkaProducer
import json


class KafkaTopic:
    def __init__(self, topic_name: str, url="localhost:9092"):
        self.msg_sent = 0
        # Create a Kafka producer
        self.producer: KafkaProducer = KafkaProducer(
            bootstrap_servers=[url],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        self.topic_name: str = topic_name

    def send(self, msg: object):
        self.producer.send(self.topic_name, value=msg)
        self.msg_sent += 1
        print("Sent:", self.msg_sent)

    def exit(self):
        self.producer.close()
        print("Producer closed")
