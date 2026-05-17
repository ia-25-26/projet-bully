import json
import threading
import time

import config


class Logger:
    """Thread-safe JSON-lines logger controlled by config.VERBOSE_LOGGING."""

    def __init__(self, filename="logs.json"):
        """Create or truncate the log file used by the simulation."""
        self.filename = filename
        self.lock = threading.Lock()
        with open(self.filename, "w") as f:
            f.write("")

    def log(self, event_type, node_id, details=""):
        """Append one JSON event unless verbose logging is disabled."""
        if not config.VERBOSE_LOGGING:
            return

        entry = {
            "timestamp": time.time(),
            "event_type": event_type,
            "node_id": node_id,
            "details": details
        }

        with self.lock:
            with open(self.filename, "a") as f:
                f.write(json.dumps(entry) + "\n")

    def set_verbose(self, enabled):
        """Toggle logging at runtime for all Logger instances."""
        config.VERBOSE_LOGGING = bool(enabled)
