import requests
import signal
from requests_sse import EventSource
from KafkaTopic import KafkaTopic
from wiki_helpers import format_change


def send_data(k: KafkaTopic):
    headers = {
        "User-Agent": "MyWikipediaBot/1.0 (https://example.com; contact@example.com)"
    }

    url = "https://stream.wikimedia.org/v2/stream/recentchange"
    with EventSource(url, headers=headers) as stream:
        for event in stream:
            if event.type == "message":
                try:
                    k.send(format_change(event.data))
                except ValueError:
                    pass


k = KafkaTopic("wiki_data", "localhost:9092")

# Function to run when terminating the process
run_on_close = lambda: k.exit()

def signal_handler(sig, frame):
    print("\nShutting down gracefully...")
    run_on_close()
    exit(0)

# Register the signal handler
signal.signal(signal.SIGINT, signal_handler)

try:
    send_data(k)
except Exception as e:
    print(f"Error: {e}")
    run_on_close()
