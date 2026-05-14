import time
import json
from collections import defaultdict


class Metrics:
    def __init__(self):
        self.message_count = defaultdict(int)
        self.election_count = 0
        self.election_times = []
        self.current_election_start = None

    # ----------------------------
    # MESSAGE TRACKING
    # ----------------------------
    def log_message(self, msg_type):
        self.message_count[msg_type] += 1

    # ----------------------------
    # ELECTION TRACKING
    # ----------------------------
    def start_election(self):
        self.election_count += 1
        self.current_election_start = time.time()

    def end_election(self):
        if self.current_election_start:
            duration = time.time() - self.current_election_start
            self.election_times.append(duration)
            self.current_election_start = None

    # ----------------------------
    # EXPORT
    # ----------------------------
    def export(self, filename="metrics.json"):
        data = {
            "message_count": dict(self.message_count),
            "election_count": self.election_count,
            "avg_election_time": (
                sum(self.election_times) / len(self.election_times)
                if self.election_times else 0
            ),
            "all_election_times": self.election_times
        }

        with open(filename, "w") as f:
            json.dump(data, f, indent=4)

        print("📊 Metrics sauvegardées dans", filename)