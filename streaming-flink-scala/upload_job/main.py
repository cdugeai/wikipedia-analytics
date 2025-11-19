import requests

def upload():

    files = {
        'jarfile': open('../target/scala-3.6.2/streaming-flink-scala-assembly-0.1.0.jar', 'rb'),
    }

    response = requests.post('http://localhost:8081//jars/upload', files=files)


if __name__ == "__main__":
    upload()
