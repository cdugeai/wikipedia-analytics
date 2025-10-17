from kafka import KafkaProducer
import json

# Create a Kafka producer
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

topic_name = "manual_input"

print(
    f"Connected to Kafka. Type messages to send to '{topic_name}' (type 'exit' to quit):"
)

try:
    while True:
        message = input("> ")

        if message.lower() == "exit":
            print("Exiting...")
            break

        # Send the message to Kafka
        producer.send(topic_name, value={"message": message})
        print(f"Message sent: {message}")

except KeyboardInterrupt:
    print("\nExiting...")

finally:
    producer.close()
