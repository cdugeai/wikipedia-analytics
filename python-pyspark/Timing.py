import time


class Timing:
    def __init__(self):
        self.current = ""
        self.start_time = -1

    def start(self, name):
        self.current = name
        self.start_time = time.time()

    def stop(self):
        now = time.time()
        duration_s = round(now - self.start_time, 1)
        message = f">> Duration {self.current}: {duration_s}s"

        print(message)
