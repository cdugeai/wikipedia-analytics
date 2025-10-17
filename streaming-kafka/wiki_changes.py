import requests
import signal
from KafkaTopic import KafkaTopic
from wiki_helpers import format_change, stream_changes


# Generate a signal handle with custom function
def generate_signal_handler(run_on_exit):
    def signal_handler(sig, frame):
        print("\nShutting down gracefully...")
        run_on_exit()
        exit(0)

    return signal_handler


if __name__ == "__main__":
    # Connect to Kafka
    k = KafkaTopic("wiki_data", "localhost:9092")
    # Function to close producer on error
    close_producer = lambda: k.close_producer()
    # Register the signal handler
    signal.signal(signal.SIGINT, generate_signal_handler(close_producer))

    # Send all changes to Kafka
    try:
        for change_json in stream_changes():
            k.send(change_json)
    except Exception as e:
        print(f"Error: {e}")
        close_producer()
