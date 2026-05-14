import json
import time


class Logger:
    def __init__(self, filename="logs.json"):
        self.filename = filename
        # Initialiser le fichier de log en vide
        with open(self.filename, "w") as f:
            f.write("")

    def log(self, event_type, node_id, details=""):
        entry = {
            "timestamp": time.time(),
            "event_type": event_type,
            "node_id": node_id,
            "details": details
        }

        with open(self.filename, "a") as f:
            f.write(json.dumps(entry) + "\n")